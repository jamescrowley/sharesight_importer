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


class CustomInstrumentCleanupTests(unittest.TestCase):
    def test_delete_obsolete_preserves_required_and_manual_instruments(self):
        api_client = MagicMock()
        api_client.get_custom_investments.return_value = {
            "custom_investments": [
                {
                    "id": 1,
                    "code": "CUSTOM-123",
                    "name": "Custom Fund (AUTO)",
                    "country_code": "GB",
                    "investment_type": "MANAGED_FUND",
                },
                {
                    "id": 2,
                    "code": "OBSOLETE-123",
                    "name": "Obsolete Fund (AUTO)",
                    "country_code": "GB",
                    "investment_type": "MANAGED_FUND",
                },
                {
                    "id": 3,
                    "code": "MANUAL",
                    "name": "Manual Fund",
                    "country_code": "GB",
                    "investment_type": "MANAGED_FUND",
                },
            ]
        }
        synchronizer = CustomInstrumentSynchronizer(api_client)

        synchronizer.sync(123, [custom_row(2)], delete_obsolete=True)

        api_client.delete_custom_investment.assert_called_once_with(2)
        api_client.create_custom_investment.assert_not_called()
        api_client.update_custom_investment.assert_not_called()


if __name__ == "__main__":
    unittest.main()
