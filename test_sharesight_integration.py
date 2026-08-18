import datetime
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse

import requests

from sharesight_api_client import SharesightApiClient
from sharesight_csv_importer import SharesightCsvImporter


CSV_HEADER = (
    "unique_identifier,transaction_type,transaction_date,goes_ex_on,symbol,market,"
    "quantity,price_in_instrument_currency,amount,amount_currency,cash_account,description,"
    "brokerage_in_instrument_currency,instrument_currency,exchange_rate_gbp,exchange_rate_aud,"
    "amount_in_instrument_currency,amount_in_gbp,amount_in_aud,accrued_income,"
    "accrued_income_in_instrument_currency,accrued_income_in_gbp,accrued_income_in_aud,"
    "symbol_name,instrument_country_code,symbol_type\n"
)


class FakeResponse:
    def __init__(self, status_code=200, payload=None, url=""):
        self.status_code = status_code
        self._payload = payload if payload is not None else {}
        self.url = url
        self.text = str(self._payload)
        self.request = SimpleNamespace(method="", url=url, headers={}, body=None)

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code} for {self.url}", response=self)


class FakeSharesightTransport:
    """Small, strict in-memory substitute for the Sharesight endpoints used by imports."""

    def __init__(self):
        self.requests = []
        self.portfolios = []
        self.cash_accounts = {}
        self.holdings = {}
        self.trades = []
        self.payouts = []
        self.cash_transactions = []
        self.custom_investments = []
        self.prices = []
        self.merges = []
        self.resyncs = []
        self._next_id = 100

    def add_portfolio(self, name="Test Portfolio", country_code="GB", currency_code="GBP"):
        portfolio = {
            "id": self._id(), "name": name, "country_code": country_code,
            "currency_code": currency_code,
        }
        self.portfolios.append(portfolio)
        self.cash_accounts[portfolio["id"]] = []
        self.holdings[portfolio["id"]] = []
        return portfolio

    def add_cash_account(self, portfolio_id, currency="USD", name="Broker"):
        account = {"id": self._id(), "currency": currency, "name": f"{name} ({currency})"}
        self.cash_accounts.setdefault(portfolio_id, []).append(account)
        return account

    def add_holding(self, portfolio_id, code="FUND", market="LSE", currency="GBP"):
        holding = {
            "id": self._id(),
            "instrument": {"code": code, "market_code": market, "currency_code": currency},
        }
        self.holdings.setdefault(portfolio_id, []).append(holding)
        return holding

    def _id(self):
        self._next_id += 1
        return self._next_id

    def __call__(self, method, url, json=None, headers=None):
        method = method.lower()
        parsed = urlparse(url)
        path = parsed.path
        query = parse_qs(parsed.query)
        self.requests.append({"method": method, "url": url, "json": json, "headers": headers or {}})

        if path == "/oauth2/token":
            return self._response(url, {"access_token": "offline-token"}, method, headers, json)
        if path == "/api/v2/portfolios.json" and method == "get":
            return self._response(url, {"portfolios": self.portfolios}, method, headers, json)
        if path == "/api/v2/portfolios.json" and method == "post":
            data = json["portfolio"]
            currency = "GBP" if data["country_code"] == "GB" else "AUD"
            portfolio = self.add_portfolio(data["name"], data["country_code"], currency)
            return self._response(url, portfolio, method, headers, json)

        parts = path.strip("/").split("/")
        if len(parts) >= 5 and parts[:3] == ["api", "v2", "portfolios"]:
            portfolio_id = int(parts[3])
            resource = parts[4]
            if resource == "cash_accounts.json" and method == "get":
                return self._response(url, {"cash_accounts": self.cash_accounts.get(portfolio_id, [])}, method, headers, json)
            if resource == "cash_accounts.json" and method == "post":
                data = json["cash_account"]
                account = self.add_cash_account(portfolio_id, data["currency"], data["name"].removesuffix(f" ({data['currency']})"))
                return self._response(url, {"cash_account": account}, method, headers, json)
            if resource == "payouts.json" and method == "get":
                payouts = [p for p in self.payouts if p["portfolio_id"] == portfolio_id]
                return self._response(url, {"payouts": payouts}, method, headers, json)
            if resource == "holding_merges.json" and method == "post":
                self.merges.append(json)
                return self._response(url, {"holding_merge": {"id": self._id()}}, method, headers, json)

        if len(parts) == 5 and parts[:3] == ["api", "v3", "portfolios"] and parts[4] == "holdings":
            portfolio_id = int(parts[3])
            return self._response(url, {"holdings": self.holdings.get(portfolio_id, [])}, method, headers, json)
        if len(parts) == 4 and parts[:3] == ["api", "v3", "holdings"] and method == "get":
            holding_id = int(parts[3])
            holding = next(h for values in self.holdings.values() for h in values if h["id"] == holding_id)
            return self._response(url, {"holding": holding}, method, headers, json)
        if len(parts) == 4 and parts[:3] == ["api", "v3", "holdings"] and method == "delete":
            holding_id = int(parts[3])
            for values in self.holdings.values():
                values[:] = [holding for holding in values if holding["id"] != holding_id]
            return self._response(url, {}, method, headers, json)

        if path == "/api/v2/trades.json" and method == "post":
            trade = dict(json["trade"])
            duplicate = next((t for t in self.trades if t["unique_identifier"] == trade["unique_identifier"]), None)
            if duplicate:
                return self._response(url, {"errors": {"unique_identifier": ["A trade with this unique_identifier already exists in the portfolio."]}}, method, headers, json, 422)
            holding = next((h for h in self.holdings.setdefault(trade["portfolio_id"], [])
                            if h["instrument"]["code"].lower() == str(trade["symbol"]).lower()
                            and h["instrument"]["market_code"].lower() == str(trade["market"]).lower()), None)
            if holding is None:
                holding = self.add_holding(trade["portfolio_id"], trade["symbol"], trade["market"], trade.get("brokerage_currency_code") or "GBP")
            trade["holding_id"] = holding["id"]
            self.trades.append(trade)
            # SPLIT avoids coupling this integration fake to Sharesight's calculated response fields.
            return self._response(url, {"trade": {"holding_id": holding["id"], "transaction_type": "SPLIT"}}, method, headers, json)
        if path == "/api/v2/payouts.json" and method == "post":
            payout = dict(json["payout"])
            payout["id"] = self._id()
            self.payouts.append(payout)
            return self._response(url, {"payout": payout}, method, headers, json)

        if len(parts) >= 5 and parts[:3] == ["api", "v2", "cash_accounts"]:
            account_id = int(parts[3])
            if parts[4] == "cash_account_transactions.json" and method == "post":
                transaction = dict(json["cash_account_transaction"])
                transaction["cash_account_id"] = account_id
                self.cash_transactions.append(transaction)
                return self._response(url, {"cash_account_transaction": transaction}, method, headers, json)
            if parts[4] == "reset.json" and method == "post":
                self.resyncs.append(account_id)
                return self._response(url, {}, method, headers, json)
        if len(parts) == 4 and parts[:3] == ["api", "v2", "cash_accounts"] and method == "delete":
            account_id = int(parts[3])
            for values in self.cash_accounts.values():
                values[:] = [account for account in values if account["id"] != account_id]
            self.cash_transactions[:] = [
                transaction for transaction in self.cash_transactions
                if transaction["cash_account_id"] != account_id
            ]
            return self._response(url, {}, method, headers, json)

        if path == "/api/v3/custom_investments" and method == "get":
            portfolio_id = int(query["portfolio_id"][0])
            values = [c for c in self.custom_investments if c["portfolio_id"] == portfolio_id]
            return self._response(url, {"custom_investments": values}, method, headers, json)
        if path == "/api/v3/custom_investments" and method == "post":
            investment = dict(json)
            investment["id"] = self._id()
            self.custom_investments.append(investment)
            return self._response(url, investment, method, headers, json)
        if len(parts) == 4 and parts[:3] == ["api", "v3", "custom_investments"] and method == "delete":
            investment_id = int(parts[3])
            self.custom_investments[:] = [
                investment for investment in self.custom_investments if investment["id"] != investment_id
            ]
            return self._response(url, {}, method, headers, json)

        raise AssertionError(f"Unimplemented fake Sharesight request: {method.upper()} {url} {json}")

    @staticmethod
    def _response(url, payload, method, headers, body, status=200):
        response = FakeResponse(status, payload, url)
        response.request = SimpleNamespace(method=method.upper(), url=url, headers=headers or {}, body=body)
        return response


def csv_row(**overrides):
    fields = {
        "unique_identifier": "tx-1", "transaction_type": "BUY", "transaction_date": "2024-01-10",
        "goes_ex_on": "", "symbol": "FUND", "market": "LSE", "quantity": "10",
        "price_in_instrument_currency": "5", "amount": "50", "amount_currency": "GBP",
        "cash_account": "Broker", "description": "Synthetic transaction",
        "brokerage_in_instrument_currency": "0", "instrument_currency": "GBP",
        "exchange_rate_gbp": "1", "exchange_rate_aud": "2", "amount_in_instrument_currency": "50",
        "amount_in_gbp": "50", "amount_in_aud": "100", "accrued_income": "0",
        "accrued_income_in_instrument_currency": "0", "accrued_income_in_gbp": "0",
        "accrued_income_in_aud": "0", "symbol_name": "", "instrument_country_code": "",
        "symbol_type": "",
    }
    fields.update(overrides)
    return ",".join(str(fields[name]) for name in CSV_HEADER.strip().split(",")) + "\n"


class ImporterHttpIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.transport = FakeSharesightTransport()
        self.request_patch = patch("sharesight_api_client.requests.request", side_effect=self.transport)
        self.request_patch.start()
        self.addCleanup(self.request_patch.stop)
        self.client = SharesightApiClient("synthetic-client", "synthetic-secret", False)
        self.importer = SharesightCsvImporter(self.client)

    def run_import(self, rows, **overrides):
        options = {
            "portfolio_name": "Test Portfolio", "country_code": "GB", "delete_existing": False,
            "min_date": None, "exclude_exdate_transactions_before_min_date": False,
            "opening_balance_on": None, "opening_balance_from": None, "min_line": None,
            "max_line": None, "prices_file_path": None, "exchange_rates_file_path": None,
        }
        options.update(overrides)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "transactions.csv"
            path.write_text(CSV_HEADER + "".join(rows), encoding="utf-8-sig")
            return self.importer.import_file(path, **options)

    def test_mixed_workflow_reaches_real_http_client_and_preserves_cash_effects(self):
        self.run_import([
            csv_row(unique_identifier="buy-1"),
            csv_row(unique_identifier="div-1", transaction_type="DIVIDEND", quantity="", amount="7",
                    amount_in_instrument_currency="7", amount_in_gbp="7", amount_in_aud="14"),
            csv_row(unique_identifier="fee-1", transaction_type="FEE", symbol="", market="", quantity="",
                    amount="-2", amount_in_instrument_currency="-2", amount_in_gbp="-2", amount_in_aud="-4"),
            csv_row(unique_identifier="split-1", transaction_type="SPLIT", quantity="20", amount="0"),
        ])

        self.assertEqual([t["unique_identifier"] for t in self.transport.trades], ["buy-1", "split-1"])
        self.assertEqual([p["comments"].split()[0] for p in self.transport.payouts], ["div-1"])
        self.assertEqual([t["foreign_identifier"] for t in self.transport.cash_transactions], ["buy-1", "div-1", "fee-1"])
        self.assertEqual(len(self.transport.resyncs), 1)
        authenticated = [r for r in self.transport.requests if "/oauth2/token" not in r["url"]]
        self.assertTrue(all(r["headers"]["Authorization"] == "Bearer offline-token" for r in authenticated))

    def test_retained_income_expands_to_two_non_cash_records(self):
        portfolio = self.transport.add_portfolio()
        self.transport.add_cash_account(portfolio["id"], "GBP", "Broker")
        self.transport.add_holding(portfolio["id"])

        self.run_import([csv_row(
            unique_identifier="retained-1", transaction_type="RETAINED_NET_INCOME", quantity="0",
            amount="12", amount_in_instrument_currency="12", amount_in_gbp="12", amount_in_aud="24",
        )])

        self.assertEqual([p["comments"].split()[0] for p in self.transport.payouts], ["retained-1"])
        self.assertEqual([t["unique_identifier"] for t in self.transport.trades], ["retained-1_CALL"])
        self.assertEqual(self.transport.cash_transactions, [])

    def test_custom_instrument_is_qualified_and_used_by_trade(self):
        self.run_import([csv_row(
            unique_identifier="custom-1", symbol="PRIVATE", market="OTHER", symbol_name="Synthetic Private Fund",
            instrument_country_code="GB", symbol_type="MANAGED_FUND",
        )])
        portfolio_id = self.transport.portfolios[0]["id"]
        expected_code = f"PRIVATE-{portfolio_id}"
        self.assertEqual(self.transport.custom_investments[0]["code"], expected_code)
        self.assertEqual(self.transport.custom_investments[0]["name"], "Synthetic Private Fund (AUTO)")
        self.assertEqual(self.transport.trades[0]["symbol"], expected_code)

    def test_supported_transaction_families_have_expected_cash_effects(self):
        rows = [
            csv_row(unique_identifier="buy", transaction_type="BUY"),
            csv_row(unique_identifier="sell", transaction_type="SELL"),
            csv_row(unique_identifier="opening", transaction_type="OPENING_BALANCE", amount="0"),
            csv_row(unique_identifier="call", transaction_type="CAPITAL_CALL", amount="5",
                    amount_in_instrument_currency="5"),
            csv_row(unique_identifier="return", transaction_type="CAPITAL_RETURN", amount="5",
                    amount_in_instrument_currency="5"),
            csv_row(unique_identifier="split", transaction_type="SPLIT", amount="0"),
            csv_row(unique_identifier="bonus", transaction_type="BONUS", amount="0"),
            csv_row(unique_identifier="consolidation", transaction_type="CONSOLD", amount="0"),
            csv_row(unique_identifier="cancel", transaction_type="CANCEL", amount="0"),
        ]
        for transaction_type in (
            "DEPOSIT", "WITHDRAWAL", "INTEREST_PAYMENT", "INTEREST_CHARGED", "FEE", "FEE_REIMBURSEMENT"
        ):
            rows.append(csv_row(
                unique_identifier=transaction_type.lower(), transaction_type=transaction_type,
                symbol="", market="", quantity="", amount="1",
            ))

        self.run_import(rows)

        self.assertEqual(
            [trade["transaction_type"] for trade in self.transport.trades],
            ["BUY", "SELL", "OPENING_BALANCE", "CAPITAL_CALL", "CAPITAL_RETURN",
             "SPLIT", "BONUS", "CONSOLD", "CANCEL"],
        )
        self.assertEqual(
            [transaction["foreign_identifier"] for transaction in self.transport.cash_transactions],
            ["buy", "sell", "call", "return", "deposit", "withdrawal", "interest_payment",
             "interest_charged", "fee", "fee_reimbursement"],
        )

    def test_au_portfolio_selects_aud_conversion_fields(self):
        self.run_import([
            csv_row(unique_identifier="au-buy", exchange_rate_aud="1.75", amount_in_aud="87.50"),
            csv_row(unique_identifier="au-opening", transaction_type="OPENING_BALANCE", amount="0",
                    exchange_rate_aud="1.75", amount_in_aud="87.50"),
        ], country_code="AU")

        self.assertEqual(self.transport.trades[0]["exchange_rate"], "1.75")
        self.assertEqual(self.transport.trades[1]["exchange_rate"], "1.75")
        self.assertEqual(self.transport.trades[1]["cost_base"], "87.50")

    def test_retained_equalisation_expands_to_two_non_cash_trades(self):
        portfolio = self.transport.add_portfolio()
        self.transport.add_cash_account(portfolio["id"], "GBP", "Broker")
        self.transport.add_holding(portfolio["id"])
        self.run_import([csv_row(
            unique_identifier="equalisation-1", transaction_type="RETAINED_EQUALISATION", quantity="0",
            amount="8", amount_in_instrument_currency="8", amount_in_gbp="8", amount_in_aud="16",
        )])
        self.assertEqual(
            [(trade["unique_identifier"], trade["transaction_type"]) for trade in self.transport.trades],
            [("equalisation-1", "CAPITAL_RETURN"), ("equalisation-1_CALL", "CAPITAL_CALL")],
        )
        self.assertEqual(self.transport.cash_transactions, [])

    def test_adjacent_merge_pair_emits_one_merge_and_no_cash(self):
        portfolio = self.transport.add_portfolio()
        self.transport.add_cash_account(portfolio["id"], "GBP", "Broker")
        old_holding = self.transport.add_holding(portfolio["id"], "OLD", "LSE")
        self.run_import([
            csv_row(unique_identifier="merge-cancel", transaction_type="MERGE_CANCEL", symbol="OLD", quantity="10"),
            csv_row(unique_identifier="merge-buy", transaction_type="MERGE_BUY", symbol="NEW", quantity="20"),
        ])
        self.assertEqual(len(self.transport.merges), 1)
        self.assertEqual(self.transport.merges[0]["holding_id"], old_holding["id"])
        self.assertEqual(self.transport.merges[0]["symbol"], "NEW")
        self.assertEqual(self.transport.cash_transactions, [])

    def test_destructive_replacement_removes_old_state_before_reimport(self):
        portfolio = self.transport.add_portfolio()
        old_account = self.transport.add_cash_account(portfolio["id"], "GBP", "Old Broker")
        old_holding = self.transport.add_holding(portfolio["id"], "OLD", "LSE")
        self.transport.cash_transactions.append({
            "cash_account_id": old_account["id"], "foreign_identifier": "old-cash"
        })
        auto_instrument_id = self.transport._id()
        self.transport.custom_investments.extend([
            {"id": auto_instrument_id, "portfolio_id": portfolio["id"], "code": "AUTO-OLD",
             "name": "Generated (AUTO)", "country_code": "GB", "investment_type": "MANAGED_FUND"},
            {"id": self.transport._id(), "portfolio_id": portfolio["id"], "code": "MANUAL",
             "name": "Manual instrument", "country_code": "GB", "investment_type": "MANAGED_FUND"},
        ])

        self.run_import([csv_row(unique_identifier="replacement")], delete_existing=True)

        self.assertNotIn(old_holding, self.transport.holdings[portfolio["id"]])
        self.assertNotIn(old_account, self.transport.cash_accounts[portfolio["id"]])
        self.assertEqual([c["name"] for c in self.transport.custom_investments], ["Manual instrument"])
        self.assertEqual([t["unique_identifier"] for t in self.transport.trades], ["replacement"])
        destructive_urls = [
            request["url"] for request in self.transport.requests if request["method"] == "delete"
        ]
        self.assertEqual(destructive_urls, [
            f"https://api.sharesight.com/api/v2/cash_accounts/{old_account['id']}",
            f"https://api.sharesight.com/api/v3/holdings/{old_holding['id']}",
            f"https://api.sharesight.com/api/v3/custom_investments/{auto_instrument_id}",
        ])

    def test_cost_base_adjustment_has_no_cash_effect(self):
        self.run_import([csv_row(
            unique_identifier="adjust", transaction_type="ADJUST_COST_BASE", amount="0",
        )])
        self.assertEqual(self.transport.cash_transactions, [])

    def test_filtered_out_custom_instrument_causes_no_setup_mutation(self):
        self.run_import([
            csv_row(unique_identifier="old-custom", transaction_date="2023-01-01", symbol="OLD", market="OTHER",
                    symbol_name="Excluded Fund", instrument_country_code="GB", symbol_type="MANAGED_FUND"),
            csv_row(unique_identifier="current", transaction_date="2024-01-01"),
        ], min_date=datetime.date(2024, 1, 1))
        self.assertEqual(self.transport.custom_investments, [])

    def test_missing_cash_account_is_rejected_before_trade_mutation(self):
        self.transport.add_portfolio()
        with self.assertRaisesRegex(ValueError, "missing required cash accounts"):
            self.run_import([csv_row(unique_identifier="unsafe-buy")])
        self.assertEqual(self.transport.trades, [])

    def test_filter_cannot_select_only_one_member_of_merge_pair(self):
        portfolio = self.transport.add_portfolio()
        self.transport.add_cash_account(portfolio["id"], "GBP", "Broker")
        self.transport.add_holding(portfolio["id"], "OLD", "LSE")
        with self.assertRaisesRegex(ValueError, "merge pair"):
            self.run_import([
                csv_row(unique_identifier="merge-cancel", transaction_type="MERGE_CANCEL", symbol="OLD"),
                csv_row(unique_identifier="merge-buy", transaction_type="MERGE_BUY", symbol="NEW"),
            ], min_line=3, max_line=3)
        self.assertEqual(self.transport.merges, [])

    def test_incomplete_merge_pair_is_rejected_before_portfolio_setup(self):
        with self.assertRaisesRegex(ValueError, "merge pair is incomplete"):
            self.run_import([
                csv_row(unique_identifier="merge-cancel", transaction_type="MERGE_CANCEL", symbol="OLD"),
            ])
        self.assertEqual(self.transport.portfolios, [])

    def test_unsupported_country_is_rejected_before_portfolio_setup(self):
        with self.assertRaisesRegex(ValueError, "Unsupported country code"):
            self.run_import([csv_row()], country_code="US")
        self.assertEqual(self.transport.portfolios, [])

    def test_unsupported_transaction_is_rejected_before_portfolio_setup(self):
        with self.assertRaisesRegex(ValueError, "unsupported transaction type"):
            self.run_import([csv_row(transaction_type="UNKNOWN")])
        self.assertEqual(self.transport.portfolios, [])


if __name__ == "__main__":
    unittest.main()
