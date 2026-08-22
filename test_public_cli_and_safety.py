import io
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import MagicMock, patch

from sharesight_api_client import ApiResult
from sharesight_csv_importer import SharesightCsvImporter
from sharesight_import_options import ImportOptions
from sharesight_importer.cli import main
from sharesight_payloads import build_payout_payload, build_trade_payload
from sharesight_schema import validate_import_files, validate_price_file


ROOT = Path(__file__).parent


class PublicCliAndSchemaTests(unittest.TestCase):
    def test_documented_transaction_examples_validate_offline(self):
        for currency, name in (
            ("AUD", "aud-trades-opening.csv"),
            ("GBP", "gbp-income-cash.csv"),
            ("CAD", "cad-merger-custom.csv"),
            ("EUR", "eur-trade.csv"),
        ):
            with self.subTest(name=name):
                validate_import_files(ROOT / "examples" / name, currency)
        validate_price_file(ROOT / "examples" / "custom-prices.csv")
        validate_import_files(
            ROOT / "examples" / "aud-residency-history.csv",
            "AUD",
            residency_reset_file_path=ROOT / "examples" / "aud-residency-reset.csv",
        )

    def test_validate_cli_needs_no_credentials_or_http(self):
        stdout = io.StringIO()
        with (
            patch.dict("os.environ", {}, clear=True),
            patch("sharesight_importer.cli.SharesightApiClient") as client,
            redirect_stdout(stdout),
        ):
            result = main([
                "validate", "--file-name", str(ROOT / "examples" / "eur-trade.csv"),
                "--portfolio-currency", "EUR",
            ])
        self.assertEqual(result, 0)
        client.assert_not_called()
        self.assertIn("Validation succeeded", stdout.getvalue())

    def test_cad_and_eur_dynamic_payload_fields(self):
        row = {
            "unique_identifier": "dynamic-1", "transaction_type": "BUY",
            "transaction_date": "2025-01-01", "symbol": "FUND", "market": "TSX",
            "quantity": "1", "price_in_instrument_currency": "10",
            "brokerage_in_instrument_currency": "0", "instrument_currency": "USD",
            "exchange_rate_cad": "0.7", "amount_in_cad": "14.29",
            "exchange_rate_eur": "0.9", "amount_in_eur": "11.11",
            "amount_in_instrument_currency": "10", "amount": "10",
            "amount_currency": "USD", "description": "Synthetic",
        }
        self.assertEqual(build_trade_payload(1, "CAD", row)["exchange_rate"], "0.7")
        self.assertEqual(build_payout_payload(1, 2, "EUR", row)["banked_amount"], "11.11")


class ImportSafetyTests(unittest.TestCase):
    def setUp(self):
        self.api = MagicMock()
        self.api.get_portfolios.return_value = {
            "portfolios": [{"id": 42, "name": "Exact Portfolio", "currency_code": "EUR"}]
        }
        self.api.get_cash_accounts.return_value = {
            "cash_accounts": [{"id": 7, "name": "Broker (EUR)", "currency": "EUR"}]
        }
        self.api.get_portfolio_holdings.return_value = {"holdings": []}
        self.api.get_custom_investments.return_value = {"custom_investments": []}
        self.api.get_payouts.return_value = {"payouts": []}
        self.importer = SharesightCsvImporter(self.api)
        self.file = ROOT / "examples" / "eur-trade.csv"

    def options(self, **overrides):
        values = {"resync_cash_accounts": False}
        values.update(overrides)
        return ImportOptions(**values)

    def assert_no_mutation(self):
        mutation_prefixes = ("create", "delete", "try_create", "update", "put", "resync")
        mutations = [
            call for call in self.api.mock_calls
            if call[0].split(".")[-1].startswith(mutation_prefixes)
        ]
        self.assertEqual(mutations, [])

    def test_missing_portfolio_does_not_create_without_explicit_option(self):
        self.api.get_portfolios.return_value = {"portfolios": []}
        with self.assertRaisesRegex(ValueError, "Use --create-portfolio"):
            self.importer.import_file(self.file, "Typo", "EUR", self.options())
        self.assert_no_mutation()

    def test_ambiguous_exact_name_fails_before_mutation(self):
        portfolio = {"name": "Exact Portfolio", "currency_code": "EUR"}
        self.api.get_portfolios.return_value = {
            "portfolios": [{"id": 1, **portfolio}, {"id": 2, **portfolio}]
        }
        with self.assertRaisesRegex(ValueError, "matched IDs: 1, 2"):
            self.importer.import_file(self.file, "Exact Portfolio", "EUR", self.options())
        self.assert_no_mutation()

    def test_currency_mismatch_fails_before_mutation(self):
        with self.assertRaisesRegex(ValueError, "currency mismatch"):
            self.importer.import_file(
                ROOT / "examples" / "cad-merger-custom.csv",
                "Exact Portfolio",
                "CAD",
                self.options(),
            )
        self.assert_no_mutation()

    def test_created_portfolio_currency_mismatch_stops_before_contents_are_created(self):
        self.api.get_portfolios.side_effect = [
            {"portfolios": []},
            {
                "portfolios": [
                    {"id": 99, "name": "New Portfolio", "currency_code": "CAD"}
                ]
            },
        ]
        self.api.create_portfolio.return_value = {"id": 99}

        with self.assertRaisesRegex(ValueError, "Created portfolio currency mismatch"):
            self.importer.import_file(
                self.file,
                "New Portfolio",
                "EUR",
                options=self.options(create_portfolio=True),
                country_code="CA",
            )

        self.api.create_portfolio.assert_called_once()
        self.api.create_cash_account.assert_not_called()
        self.api.try_create_trade.assert_not_called()
        self.api.try_create_payout.assert_not_called()
        self.api.try_create_cash_transaction.assert_not_called()

    def test_country_is_rejected_without_explicit_portfolio_creation(self):
        with self.assertRaisesRegex(ValueError, "only valid when creating"):
            self.importer.import_file(
                self.file,
                "Exact Portfolio",
                "EUR",
                options=self.options(),
                country_code="GB",
            )
        self.assert_no_mutation()

    def test_destructive_dry_run_never_prompts_or_mutates(self):
        with patch("builtins.input", side_effect=AssertionError("must not prompt")):
            result = self.importer.import_file(
                self.file, "Exact Portfolio", "EUR",
                self.options(delete_existing=True, dry_run=True),
            )
        self.assertTrue(result)
        self.assert_no_mutation()

    def test_destructive_mismatch_aborts_before_mutation(self):
        with (
            patch("sys.stdin.isatty", return_value=True),
            patch("builtins.input", return_value="Wrong Portfolio"),
            self.assertRaisesRegex(RuntimeError, "did not match"),
        ):
            self.importer.import_file(
                self.file, "Exact Portfolio", "EUR",
                self.options(delete_existing=True),
            )
        self.assert_no_mutation()

    def test_destructive_non_tty_and_eof_abort(self):
        with (
            patch("sys.stdin.isatty", return_value=False),
            self.assertRaisesRegex(RuntimeError, "non-interactive"),
        ):
            self.importer.import_file(
                self.file, "Exact Portfolio", "EUR",
                self.options(delete_existing=True),
            )
        self.assert_no_mutation()

        self.api.reset_mock()
        with (
            patch("sys.stdin.isatty", return_value=True),
            patch("builtins.input", side_effect=EOFError),
            self.assertRaisesRegex(RuntimeError, "input ended"),
        ):
            self.importer.import_file(
                self.file, "Exact Portfolio", "EUR",
                self.options(delete_existing=True),
            )
        self.assert_no_mutation()

    def test_yes_bypasses_prompt(self):
        state = {"holdings": [], "cash_accounts": [], "custom_instruments": []}
        with patch("builtins.input", side_effect=AssertionError("must not prompt")):
            self.importer._confirm_destructive(
                "Exact Portfolio", 42, state,
                self.options(delete_existing=True, yes=True),
            )

    def test_cash_resync_can_be_disabled(self):
        self.api.try_create_trade.return_value = ApiResult(
            422, {"errors": {}}, ("duplicate",), True, "/trades"
        )
        self.api.try_create_cash_transaction.return_value = ApiResult(
            201, {"cash_account_transaction": {}}, (), False, "/cash"
        )
        result = self.importer.import_file(
            self.file, "Exact Portfolio", "EUR", self.options()
        )
        self.assertTrue(result)
        self.api.resync_cash_account.assert_not_called()


if __name__ == "__main__":
    unittest.main()
