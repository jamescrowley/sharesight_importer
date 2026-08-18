import csv
import datetime
import io
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from sharesight_csv_importer import SharesightCsvImporter
from sharesight_import_options import ImportOptions
from sharesight_opening_balances import OpeningBalanceExporter, _warn


class TtyBuffer(io.StringIO):
    def isatty(self):
        return True


class OpeningBalanceExporterTests(unittest.TestCase):
    def setUp(self):
        self.api = MagicMock()
        self.api.get_portfolios.return_value = {
            "portfolios": [{
                "id": 7, "name": "Source", "currency_code": "GBP"
            }]
        }
        self.api.get_valuation_on.return_value = {
            "holdings": [{
                "id": 11, "symbol": "FUND", "market": "LSE",
                "quantity": "4", "value": "100",
            }],
            "cash_accounts": [],
        }
        self.api.get_holding.return_value = {
            "holding": {"instrument": {"currency_code": "USD"}}
        }
        self.api.get_custom_investments.return_value = {"custom_investments": []}

    def test_export_preserves_source_value_and_records_fallback_fx_date(self):
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            rates = directory / "rates.csv"
            output = directory / "opening.csv"
            rates.write_text(
                "date,GBP/USD,AUD/USD,GBP/AUD\n"
                "2024-06-29,1.25,0.625,2\n",
                encoding="utf-8",
            )
            OpeningBalanceExporter(self.api).export(
                "Source", datetime.date(2024, 7, 1), rates, output
            )
            with output.open(encoding="utf-8") as file:
                rows = list(csv.DictReader(file))

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

    def test_export_refuses_to_overwrite_before_calling_api(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "opening.csv"
            output.write_text("keep", encoding="utf-8")
            with self.assertRaises(FileExistsError):
                OpeningBalanceExporter(self.api).export(
                    "Source", datetime.date(2024, 7, 1), "unused.csv", output
                )
        self.api.get_portfolios.assert_not_called()

    def test_inconsistent_cross_rates_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            rates = directory / "rates.csv"
            rates.write_text(
                "date,GBP/USD,AUD/USD,GBP/AUD\n"
                "2024-07-01,1.25,0.5,1.5\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "Exchange rates disagree"):
                OpeningBalanceExporter(self.api).export(
                    "Source", datetime.date(2024, 7, 1), rates,
                    directory / "opening.csv",
                )

    def test_cash_valuation_mismatch_warns_and_still_exports(self):
        self.api.get_valuation_on.return_value = {
            "holdings": [],
            "cash_accounts": [{
                "cash_account_id": 12,
                "name": "Broker (GBP)",
                "currency_code": "GBP",
                "value": "101",
            }],
        }
        self.api.get_cash_account_transactions.return_value = {
            "cash_account_transactions": [{"amount": "100", "balance": "100"}]
        }
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            rates = directory / "rates.csv"
            rates.write_text(
                "date,GBP/AUD,AUD/GBP\n2024-07-01,2,0.5\n",
                encoding="utf-8",
            )
            with patch("sharesight_opening_balances._warn") as warn:
                rows = OpeningBalanceExporter(self.api).export(
                    "Source", datetime.date(2024, 7, 1), rates,
                    directory / "opening.csv",
                )

        warn.assert_called_once()
        self.assertIn("transactions total 100", warn.call_args.args[0])
        self.assertEqual(rows[0]["amount"], "100")

    def test_warning_is_yellow_on_a_colour_capable_terminal(self):
        stream = TtyBuffer()
        with patch.dict("os.environ", {}, clear=True):
            _warn("Something differs", stream=stream)
        self.assertEqual(
            stream.getvalue(),
            "\033[33mWARNING: Something differs\033[0m\n",
        )


class FrozenOpeningBalanceValidationTests(unittest.TestCase):
    def test_ordinary_transaction_before_boundary_fails_before_api_calls(self):
        api = MagicMock()
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            ordinary = directory / "transactions.csv"
            opening = directory / "opening.csv"
            ordinary.write_text(
                "unique_identifier,transaction_type,transaction_date\n"
                "old,DEPOSIT,2023-12-31\n",
                encoding="utf-8",
            )
            opening.write_text(
                "unique_identifier,transaction_type,transaction_date,amount,amount_currency,cash_account\n"
                "GENERATED-CASH,DEPOSIT,2024-01-01,1,GBP,Account\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "before opening-balance date"):
                SharesightCsvImporter(api).import_file(
                    ordinary, "Target", "GB",
                    ImportOptions(opening_balances_file_path=opening),
                )
        api.get_portfolios.assert_not_called()

    def test_invalid_opening_row_fails_before_api_calls(self):
        api = MagicMock()
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            ordinary = directory / "transactions.csv"
            opening = directory / "opening.csv"
            ordinary.write_text(
                "unique_identifier,transaction_type,transaction_date\n",
                encoding="utf-8",
            )
            opening.write_text(
                "unique_identifier,transaction_type,transaction_date\n"
                "bad,BUY,2024-01-01\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "non-cash BUY"):
                SharesightCsvImporter(api).import_file(
                    ordinary, "Target", "GB",
                    ImportOptions(opening_balances_file_path=opening),
                )
        api.get_portfolios.assert_not_called()


if __name__ == "__main__":
    unittest.main()
