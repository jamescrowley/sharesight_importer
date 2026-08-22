import unittest
import json
import io
from contextlib import redirect_stderr
from unittest.mock import MagicMock, call, patch

import requests

from sharesight_api_client import SharesightApiClient
from test_sharesight_integration import FakeResponse


class SharesightApiClientContractTests(unittest.TestCase):
    def make_client(self, side_effect=None):
        responses = side_effect or [FakeResponse(200, {"access_token": "test-token"})]
        request_patch = patch("sharesight_api_client.requests.request", side_effect=responses)
        request = request_patch.start()
        self.addCleanup(request_patch.stop)
        return SharesightApiClient("client-id", "client-secret", False), request

    def test_oauth_and_trade_request_contract(self):
        client, request = self.make_client([
            FakeResponse(200, {"access_token": "test-token"}),
            FakeResponse(200, {"trade": {"id": 9}}, "https://api.sharesight.com/api/v2/trades.json"),
        ])

        response = client.try_create_trade({"unique_identifier": "synthetic-1"})

        self.assertEqual(response.data, {"trade": {"id": 9}})
        self.assertTrue(response.successful)
        self.assertEqual(response.endpoint, "/api/v2/trades.json")
        self.assertEqual(request.call_args_list[0], call(
            "post",
            "https://api.sharesight.com/oauth2/token",
            json={
                "grant_type": "client_credentials",
                "redirect_uri": "urn:ietf:wg:oauth:2.0:oob",
                "client_id": "client-id",
                "client_secret": "client-secret",
            },
            headers={},
            timeout=(10, 30),
        ))
        self.assertEqual(request.call_args_list[1], call(
            "post",
            "https://api.sharesight.com/api/v2/trades.json",
            json={"trade": {"unique_identifier": "synthetic-1"}},
            headers={"Authorization": "Bearer test-token", "Content-Type": "application/json"},
            timeout=(10, 30),
        ))

    def test_cash_and_custom_investment_endpoint_contracts(self):
        client, request = self.make_client([
            FakeResponse(200, {"access_token": "test-token"}),
            FakeResponse(200, {"cash_account_transaction": {}}),
            FakeResponse(200, {"id": 22}),
            FakeResponse(200, {"prices": []}),
        ])

        client.try_create_cash_transaction(7, {"foreign_identifier": "cash-1"})
        client.create_custom_investment({"portfolio_id": 3, "code": "PRIVATE-3"})
        client.get_custom_investment_prices(22, "2024-01-01", "2024-01-31")

        self.assertEqual(request.call_args_list[1].args[:2], (
            "post", "https://api.sharesight.com/api/v2/cash_accounts/7/cash_account_transactions.json"
        ))
        self.assertEqual(request.call_args_list[1].kwargs["json"], {
            "cash_account_transaction": {"foreign_identifier": "cash-1"}
        })
        self.assertEqual(request.call_args_list[2].args[:2], (
            "post", "https://api.sharesight.com/api/v3/custom_investments"
        ))
        self.assertEqual(
            request.call_args_list[3].args[1],
            "https://api.sharesight.com/api/v3/custom_investment/22/prices.json?start_date=2024-01-01&end_date=2024-01-31",
        )

    def test_try_create_returns_validation_response_without_raising(self):
        client, _ = self.make_client([
            FakeResponse(200, {"access_token": "test-token"}),
            FakeResponse(422, {"errors": {"unique_identifier": ["duplicate"]}}),
        ])
        response = client.try_create_trade({"unique_identifier": "duplicate"})
        self.assertEqual(response.status_code, 422)
        self.assertFalse(response.successful)
        self.assertEqual(response.errors, ("duplicate",))

    def test_try_create_classifies_known_duplicate(self):
        client, _ = self.make_client([
            FakeResponse(200, {"access_token": "test-token"}),
            FakeResponse(422, {"errors": {
                "unique_identifier": [
                    "A trade with this unique_identifier already exists in the portfolio."
                ]
            }}),
        ])

        result = client.try_create_trade({"unique_identifier": "duplicate"})

        self.assertTrue(result.duplicate)
        self.assertFalse(result.successful)

    def test_try_create_normalizes_unstructured_error(self):
        client, _ = self.make_client([
            FakeResponse(200, {"access_token": "test-token"}),
            FakeResponse(500, {"error": "synthetic failure"}),
        ])

        result = client.try_create_payout({})

        self.assertEqual(result.errors, ("synthetic failure",))
        self.assertFalse(result.duplicate)

    def test_malformed_json_is_an_error_even_for_success_status(self):
        response = MagicMock(
            status_code=200,
            text="not-json",
            url="https://api.sharesight.com/api/v2/trades.json",
        )
        response.json.side_effect = json.JSONDecodeError("invalid", "not-json", 0)

        result = SharesightApiClient._result_from_response(response)

        self.assertFalse(result.successful)
        self.assertIn("Error decoding JSON response", result.errors[0])

    def test_strict_methods_raise_for_http_errors(self):
        client, _ = self.make_client([
            FakeResponse(200, {"access_token": "test-token"}),
            FakeResponse(500, {"error": "synthetic failure"}),
        ])
        with patch("builtins.print"):
            with self.assertRaises(requests.HTTPError):
                client.get_portfolios()

    def test_gateway_errors_retry_with_exponential_backoff(self):
        client, request = self.make_client([
            FakeResponse(200, {"access_token": "test-token"}),
            FakeResponse(502, {"error": "gateway"}),
            FakeResponse(504, {"error": "gateway"}),
            FakeResponse(200, {"portfolios": []}),
        ])
        with patch("sharesight_api_client.time.sleep") as sleep:
            self.assertEqual(client.get_portfolios(), {"portfolios": []})
        self.assertEqual(sleep.call_args_list, [call(5), call(10)])
        self.assertEqual(request.call_count, 4)

    def test_gateway_retry_exhaustion_raises_for_strict_method(self):
        client, request = self.make_client([
            FakeResponse(200, {"access_token": "test-token"}),
            FakeResponse(502, {"error": "gateway"}),
            FakeResponse(504, {"error": "gateway"}),
            FakeResponse(502, {"error": "gateway"}),
        ])
        with patch("sharesight_api_client.time.sleep") as sleep, patch("builtins.print"):
            with self.assertRaises(requests.HTTPError):
                client.get_portfolios()
        self.assertEqual(sleep.call_args_list, [call(5), call(10)])
        self.assertEqual(request.call_count, 4)

    def test_mutation_gateway_failure_is_not_retried(self):
        client, request = self.make_client([
            FakeResponse(200, {"access_token": "test-token"}),
            FakeResponse(502, {"error": "gateway"}),
        ])
        result = client.try_create_trade({"unique_identifier": "one-attempt"})
        self.assertEqual(result.status_code, 502)
        self.assertEqual(request.call_count, 2)

    def test_token_acquisition_retries_gateway_failure(self):
        client, request = self.make_client([
            FakeResponse(502, {"error": "gateway"}),
            FakeResponse(200, {"access_token": "test-token"}),
        ])
        self.assertIsNotNone(client)
        self.assertEqual(request.call_count, 2)

    def test_verbose_diagnostics_never_expose_secrets_or_tokens(self):
        stderr = io.StringIO()
        with redirect_stderr(stderr):
            client, _ = self.make_client([
                FakeResponse(200, {"access_token": "very-secret-token"}),
                FakeResponse(200, {"portfolios": []}),
            ])
            client._output_curl = True
            client.get_portfolios()
        output = stderr.getvalue()
        self.assertIn("headers/body redacted", output)
        self.assertNotIn("very-secret-token", output)
        self.assertNotIn("client-secret", output)


if __name__ == "__main__":
    unittest.main()
