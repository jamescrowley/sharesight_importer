import unittest

from sharesight_payloads import (
    build_cash_payload,
    build_merge_payload,
    build_payout_payload,
    build_trade_payload,
)


def transaction_row(**overrides):
    row = {
        "unique_identifier": "synthetic-1",
        "transaction_type": "BUY",
        "transaction_date": "2024-01-10",
        "goes_ex_on": "",
        "symbol": "FUND",
        "market": "lse",
        "quantity": "10",
        "price_in_instrument_currency": "5",
        "brokerage_in_instrument_currency": "1",
        "instrument_currency": "GBP",
        "exchange_rate_gbp": "1.0",
        "exchange_rate_aud": "1.9",
        "amount": "51",
        "amount_currency": "GBP",
        "amount_in_instrument_currency": "51",
        "amount_in_gbp": "51",
        "amount_in_aud": "96.9",
        "accrued_income": "0",
        "description": "Synthetic transaction",
    }
    row.update(overrides)
    return row


class SharesightPayloadTests(unittest.TestCase):
    def test_gbp_trade_payload_uses_gbp_exchange_rate(self):
        payload = build_trade_payload(7, "GBP", transaction_row())
        self.assertEqual(payload, {
            "unique_identifier": "synthetic-1",
            "transaction_type": "BUY",
            "transaction_date": "2024-01-10",
            "portfolio_id": 7,
            "symbol": "FUND",
            "market": "lse",
            "quantity": "10",
            "price": "5",
            "brokerage": "1",
            "brokerage_currency_code": "GBP",
            "exchange_rate": "1.0",
            "cost_base": "",
            "capital_return_value": "",
            "paid_on": "",
            "comments": "synthetic-1 Synthetic transaction",
        })

    def test_aud_opening_balance_uses_aud_cost_base(self):
        payload = build_trade_payload(
            7, "AUD", transaction_row(transaction_type="OPENING_BALANCE")
        )
        self.assertEqual(payload["exchange_rate"], "1.9")
        self.assertEqual(payload["cost_base"], "96.9")

    def test_capital_return_uses_ex_date_and_absolute_value(self):
        payload = build_trade_payload(7, "GBP", transaction_row(
            transaction_type="CAPITAL_RETURN",
            goes_ex_on="2024-01-08",
            amount_in_instrument_currency="-12.5",
        ))
        self.assertEqual(payload["transaction_date"], "2024-01-08")
        self.assertEqual(payload["capital_return_value"], 12.5)
        self.assertEqual(payload["paid_on"], "2024-01-10")

    def test_payout_uses_currency_specific_banked_amount(self):
        row = transaction_row(transaction_type="DIVIDEND", amount="8")
        self.assertEqual(build_payout_payload(7, 9, "GBP", row)["banked_amount"], "51")
        self.assertEqual(build_payout_payload(7, 9, "AUD", row)["banked_amount"], "96.9")

    def test_cash_payload_removes_accrued_income(self):
        payload = build_cash_payload(transaction_row(amount="51", accrued_income="3"))
        self.assertEqual(payload, {
            "date_time": "2024-01-10",
            "description": "synthetic-1 Synthetic transaction",
            "amount": 48.0,
            "type_name": "BUY",
            "foreign_identifier": "synthetic-1",
        })

    def test_merge_payload_normalizes_market_and_prefers_ex_date(self):
        payload = build_merge_payload(9, transaction_row(
            transaction_type="MERGE_BUY", goes_ex_on="2024-01-08"
        ))
        self.assertEqual(payload, {
            "holding_id": 9,
            "merge_date": "2024-01-08",
            "quantity": 10.0,
            "symbol": "FUND",
            "market": "LSE",
            "comments": "synthetic-1 Synthetic transaction",
        })

    def test_invalid_portfolio_currency_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "Unsupported portfolio currency"):
            build_trade_payload(7, "US", transaction_row())


if __name__ == "__main__":
    unittest.main()
