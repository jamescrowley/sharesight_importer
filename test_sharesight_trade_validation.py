import unittest

from sharesight_trade_validation import validate_trade


def imported_trade(**overrides):
    data = {
        "symbol": "FUND",
        "instrument_currency": "GBP",
        "amount_in_instrument_currency": "51",
        "accrued_income_in_instrument_currency": "0",
    }
    data.update(overrides)
    return data


def sharesight_trade(**overrides):
    data = {
        "transaction_type": "BUY",
        "price": "5",
        "quantity": "10",
        "brokerage": "1",
        "exchange_rate": "1",
        "value": "51",
    }
    data.update(overrides)
    return data


class TradeValidationTests(unittest.TestCase):
    def test_matching_trade_has_no_messages(self):
        self.assertEqual(
            validate_trade(imported_trade(), sharesight_trade(), "GBP"),
            [],
        )

    def test_currency_and_amount_mismatches_are_reported(self):
        messages = validate_trade(
            imported_trade(amount_in_instrument_currency="60"),
            sharesight_trade(value="52"),
            "USD",
        )

        self.assertEqual(len(messages), 3)
        self.assertFalse(messages[0].is_warning)
        self.assertTrue(messages[1].is_warning)
        self.assertTrue(messages[2].is_warning)
        self.assertTrue(messages[0].message.startswith("FUND has instrument currency"))
        self.assertIn("portfolio currency", messages[1].message)
        self.assertIn("instrument currency", messages[2].message)

    def test_sell_brokerage_reduces_expected_proceeds(self):
        messages = validate_trade(
            imported_trade(amount_in_instrument_currency="49"),
            sharesight_trade(transaction_type="SELL", value="-49"),
            "GBP",
        )

        self.assertEqual(messages, [])


if __name__ == "__main__":
    unittest.main()
