import csv
import datetime
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from sharesight_csv_input import load_transactions
from sharesight_residency_reset import (
    ResidencyResetExporter,
    load_and_validate_residency_reset,
)


RESET_HEADER = (
    "unique_identifier,transaction_type,transaction_date,symbol,market,quantity,"
    "price_in_instrument_currency,instrument_currency,exchange_rate_aud,exchange_rate_gbp,"
    "amount_in_instrument_currency,amount_in_aud,amount_in_gbp,"
    "skip_cash_account_transaction\n"
)


def reset_pair(symbol="NEW", quantity="30", residency_date="2024-07-01"):
    common = f"{symbol},LSE,{quantity},5,GBP,0.5,1,150,300,150,true"
    return (
        RESET_HEADER
        + f"reset-sell,SELL,2024-06-30,{common}\n"
        + f"reset-buy,BUY,{residency_date},{common}\n"
    )


class ResidencyResetTests(unittest.TestCase):
    def test_export_writes_prior_day_sell_and_residency_day_buy(self):
        api = MagicMock()
        api.get_portfolios.return_value = {
            "portfolios": [{"id": 7, "name": "Source", "currency_code": "GBP"}]
        }
        api.get_valuation_on.return_value = {
            "holdings": [{
                "id": 11, "symbol": "FUND", "market": "LSE",
                "quantity": "4", "value": "100",
            }],
            "cash_accounts": [{"cash_account_id": 12}],
        }
        api.get_holding.return_value = {
            "holding": {"instrument": {"currency_code": "GBP"}}
        }
        api.get_custom_investments.return_value = {"custom_investments": []}
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            rates = directory / "rates.csv"
            output = directory / "reset.csv"
            rates.write_text(
                "date,GBP/AUD,AUD/GBP\n2024-07-01,2,0.5\n",
                encoding="utf-8",
            )
            ResidencyResetExporter(api).export(
                "Source", datetime.date(2024, 7, 1), rates, output
            )
            with output.open(encoding="utf-8") as file:
                rows = list(csv.DictReader(file))

        self.assertEqual([row["transaction_type"] for row in rows], ["SELL", "BUY"])
        self.assertEqual([row["transaction_date"] for row in rows], ["2024-06-30", "2024-07-01"])
        self.assertEqual(rows[0]["quantity"], rows[1]["quantity"])
        self.assertEqual(rows[0]["amount_in_aud"], rows[1]["amount_in_aud"])
        self.assertEqual(rows[0]["skip_cash_account_transaction"], "true")
        self.assertEqual(rows[1]["residency_reset_date"], "2024-07-01")
        api.get_cash_account_transactions.assert_not_called()

    def test_quantity_replay_handles_splits_mergers_bonus_and_sale(self):
        transactions = (
            "unique_identifier,transaction_type,transaction_date,symbol,market,quantity\n"
            "buy,BUY,2020-01-01,OLD,LSE,10\n"
            "split,SPLIT,2021-01-01,OLD,LSE,10\n"
            "merge-cancel,MERGE_CANCEL,2022-01-01,OLD,LSE,20\n"
            "merge-buy,MERGE_BUY,2022-01-01,NEW,LSE,30\n"
            "bonus,BONUS,2023-01-01,NEW,LSE,2\n"
            "consolidation,CONSOLD,2023-06-01,NEW,LSE,1\n"
            "replacement,BUY,2023-07-01,NEW,LSE,1\n"
            "sale,SELL,2024-01-01,NEW,LSE,2\n"
        )
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            transactions_path = directory / "transactions.csv"
            reset_path = directory / "reset.csv"
            transactions_path.write_text(transactions, encoding="utf-8")
            reset_path.write_text(reset_pair(), encoding="utf-8")

            rows, residency_date = load_and_validate_residency_reset(
                reset_path, transactions_path
            )

        self.assertEqual(len(rows), 2)
        self.assertEqual(residency_date, datetime.date(2024, 7, 1))

    def test_quantity_mismatch_is_rejected(self):
        transactions = (
            "unique_identifier,transaction_type,transaction_date,symbol,market,quantity\n"
            "buy,BUY,2020-01-01,NEW,LSE,29\n"
        )
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            transactions_path = directory / "transactions.csv"
            reset_path = directory / "reset.csv"
            transactions_path.write_text(transactions, encoding="utf-8")
            reset_path.write_text(reset_pair(), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "history=29, reset=30"):
                load_and_validate_residency_reset(reset_path, transactions_path)

    def test_tiny_scale_relative_residual_warns_and_uses_frozen_quantity(self):
        transactions = (
            "unique_identifier,transaction_type,transaction_date,symbol,market,quantity\n"
            "buy,BUY,2020-01-01,FUND,LSE,100000\n"
            "sale,SELL,2023-01-01,FUND,LSE,100000\n"
        )
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            transactions_path = directory / "transactions.csv"
            reset_path = directory / "reset.csv"
            transactions_path.write_text(transactions, encoding="utf-8")
            reset_path.write_text(
                reset_pair(symbol="FUND", quantity="0.002"), encoding="utf-8"
            )
            with patch("sharesight_residency_reset.warn") as warning:
                load_and_validate_residency_reset(reset_path, transactions_path)

        warning.assert_called_once()
        self.assertIn("rounding residual", warning.call_args.args[0])

    def test_reset_is_inserted_between_pre_and_post_residency_rows(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "transactions.csv"
            path.write_text(
                "unique_identifier,transaction_type,transaction_date,amount,amount_currency,cash_account\n"
                "post,DEPOSIT,2024-07-02,1,GBP,Account\n"
                "pre,DEPOSIT,2024-06-30,1,GBP,Account\n",
                encoding="utf-8",
            )
            transactions = load_transactions(
                path,
                [
                    {"unique_identifier": "reset-sell", "transaction_type": "SELL", "transaction_date": "2024-06-30"},
                    {"unique_identifier": "reset-buy", "transaction_type": "BUY", "transaction_date": "2024-07-01"},
                ],
                None, False, None, None,
                inject_before_date=datetime.date(2024, 7, 1),
            )

        self.assertEqual(
            [transaction.data["unique_identifier"] for transaction in transactions],
            ["pre", "reset-sell", "reset-buy", "post"],
        )


if __name__ == "__main__":
    unittest.main()
