import unittest
from unittest.mock import MagicMock

from sharesight_csv_input import TransactionRow
from sharesight_custom_instruments import CustomInstrumentSynchronizer


def custom_row(line_number, symbol_type="", currency="GBP"):
    return TransactionRow(line_number, {
        "symbol": "CUSTOM",
        "market": "OTHER",
        "symbol_name": "Custom Fund",
        "instrument_country_code": "GB",
        "instrument_currency": currency,
        "symbol_type": symbol_type,
    })


class CustomInstrumentDeduplicationTests(unittest.TestCase):
    def test_blank_and_explicit_default_type_are_one_instrument(self):
        synchronizer = CustomInstrumentSynchronizer(MagicMock())

        instruments = synchronizer._instruments_from_transactions(
            [custom_row(2, ""), custom_row(3, "MANAGED_FUND")],
            portfolio_id=123,
        )

        self.assertEqual(instruments, [{
            "symbol": "CUSTOM-123",
            "symbol_name": "Custom Fund",
            "instrument_country_code": "GB",
            "instrument_currency": "GBP",
            "symbol_type": "MANAGED_FUND",
        }])

    def test_real_metadata_conflict_is_rejected(self):
        synchronizer = CustomInstrumentSynchronizer(MagicMock())

        with self.assertRaisesRegex(ValueError, "Line 3: conflicting metadata"):
            synchronizer._instruments_from_transactions(
                [custom_row(2, currency="GBP"), custom_row(3, currency="EUR")],
                portfolio_id=123,
            )


if __name__ == "__main__":
    unittest.main()
