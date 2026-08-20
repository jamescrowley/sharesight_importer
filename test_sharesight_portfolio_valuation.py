import datetime
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock

from sharesight_portfolio_valuation import PortfolioValuationReader


class PortfolioValuationReaderTests(unittest.TestCase):
    def setUp(self):
        self.api = MagicMock()
        self.api.get_valuation_on.return_value = {
            "holdings": [
                {
                    "id": 11,
                    "symbol": "FUND",
                    "market": "LSE",
                    "quantity": "4",
                    "value": "100",
                }
            ],
            "cash_accounts": [],
        }
        self.api.get_holding.return_value = {
            "holding": {"instrument": {"currency_code": "USD"}}
        }
        self.api.get_custom_investments.return_value = {"custom_investments": []}

    def test_holding_rows_preserve_source_value_and_record_fallback_fx_date(self):
        with tempfile.TemporaryDirectory() as directory:
            rates = Path(directory) / "rates.csv"
            rates.write_text(
                "date,GBP/USD,AUD/USD,GBP/AUD\n"
                "2024-06-29,1.25,0.625,2\n",
                encoding="utf-8",
            )
            rows = PortfolioValuationReader(self.api).generate_holding_rows(
                7, "Source", "GBP", datetime.date(2024, 7, 1), rates
            )

        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["price_in_instrument_currency"], "31.25")
        self.assertEqual(row["amount_in_instrument_currency"], "125.00")
        self.assertEqual(row["amount_in_gbp"], "100")
        self.assertEqual(row["amount_in_aud"], "200")
        self.assertEqual(row["opening_balance_source_value"], "100")
        self.assertEqual(row["opening_balance_exchange_rate_date"], "2024-06-29")
        self.assertEqual(row["skip_cash_account_transaction"], "true")
        self.api.get_valuation_on.assert_called_once_with(7, "2024-06-30")

    def test_inconsistent_cross_rates_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            rates = Path(directory) / "rates.csv"
            rates.write_text(
                "date,GBP/USD,AUD/USD,GBP/AUD\n"
                "2024-07-01,1.25,0.5,1.5\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "Exchange rates disagree"):
                PortfolioValuationReader(self.api).generate_holding_rows(
                    7, "Source", "GBP", datetime.date(2024, 7, 1), rates
                )


if __name__ == "__main__":
    unittest.main()
