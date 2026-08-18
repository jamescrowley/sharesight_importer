import csv

from sharesight_console import warn
from sharesight_csv_input import iter_transaction_rows


AUTO_NAME_SUFFIX = "(AUTO)"


def qualify_custom_instrument_symbol(data_row, portfolio_id):
    symbol = data_row.get("symbol")
    if data_row.get("market", "").lower() == "other":
        return f"{symbol}-{portfolio_id}"
    return symbol


def remove_portfolio_qualifier(symbol, portfolio_id):
    suffix = f"-{portfolio_id}"
    return symbol[:-len(suffix)] if symbol.endswith(suffix) else symbol


class CustomInstrumentSynchronizer:
    def __init__(self, api_client):
        self._api_client = api_client

    def sync(self, portfolio_id, transactions, prices_file_path=None):
        instruments = self._instruments_from_transactions(transactions, portfolio_id)
        print(f"Found {len(instruments)} custom instruments")
        print("    " + "\n    ".join(str(instrument) for instrument in instruments))
        print("Creating custom instruments")
        self._sync_instruments(portfolio_id, instruments)

        if prices_file_path:
            self._sync_prices(portfolio_id, prices_file_path)

    def delete_generated(self, portfolio_id):
        self._api_client.delete_custom_instruments(portfolio_id, AUTO_NAME_SUFFIX)

    def _instruments_from_transactions(self, transactions, portfolio_id):
        instrument_values = {
            (
                qualify_custom_instrument_symbol(row.data, portfolio_id),
                row.data.get("symbol_name"),
                row.data.get("instrument_country_code"),
                row.data.get("instrument_currency"),
                row.data.get("symbol_type"),
            )
            for row in iter_transaction_rows(transactions)
            if row.data.get("market", "").lower() == "other"
            and row.data.get("symbol_name")
        }
        return [
            {
                "symbol": symbol,
                "symbol_name": name,
                "instrument_country_code": country_code,
                "instrument_currency": currency,
                "symbol_type": instrument_type,
            }
            for symbol, name, country_code, currency, instrument_type in instrument_values
        ]

    def _sync_instruments(self, portfolio_id, instruments):
        existing_instruments = self._api_client.get_custom_investments(portfolio_id).get(
            "custom_investments", []
        )
        existing_by_code = {
            instrument["code"]: instrument for instrument in existing_instruments
        }
        for instrument in instruments:
            self._create_or_update(
                portfolio_id,
                existing_by_code.get(instrument["symbol"]),
                instrument,
            )

    def _create_or_update(self, portfolio_id, existing, instrument):
        payload = {
            "portfolio_id": portfolio_id,
            "code": instrument["symbol"],
            "name": f"{instrument['symbol_name']} {AUTO_NAME_SUFFIX}",
            "country_code": instrument["instrument_country_code"],
            "currency_code": instrument["instrument_currency"],
            "investment_type": instrument["symbol_type"] or "MANAGED_FUND",
        }
        saved_instrument = None
        if existing and self._requires_recreation(existing, payload):
            print(
                f"Custom instrument {payload['code']} has changed country code or "
                f"investment type from {existing['country_code']} "
                f"{existing['investment_type']} to {payload['country_code']} "
                f"{payload['investment_type']}. Re-creating instrument"
            )
            self._api_client.delete_custom_investment(existing["id"])
            saved_instrument = self._api_client.create_custom_investment(payload)
        elif existing and existing["name"] != payload["name"]:
            print(
                f"Updating custom instrument {payload['code']} name from "
                f"{existing['name']} to {payload['name']}"
            )
            saved_instrument = self._api_client.update_custom_investment(
                existing["id"], payload
            )
        elif not existing:
            print(f"Creating custom instrument {payload['code']}")
            saved_instrument = self._api_client.create_custom_investment(payload)

        if saved_instrument and saved_instrument.get("currency_code") != payload["currency_code"]:
            warn(
                f"Sharesight has set {payload['code']} currency code to "
                f"{saved_instrument.get('currency_code')} based on domicile, but instrument "
                f"currency is set to {payload['currency_code']}"
            )

    @staticmethod
    def _requires_recreation(existing, payload):
        return (
            existing["country_code"] != payload["country_code"]
            or existing["investment_type"] != payload["investment_type"]
        )

    def _sync_prices(self, portfolio_id, prices_file_path):
        print("Syncing custom instrument prices")
        instruments = self._api_client.get_custom_investments(portfolio_id)[
            "custom_investments"
        ]
        instrument_ids = {
            remove_portfolio_qualifier(instrument["code"], portfolio_id): instrument["id"]
            for instrument in instruments
        }
        with open(prices_file_path, mode="r", encoding="utf-8-sig") as file:
            for price in csv.DictReader(file):
                instrument_id = instrument_ids.get(price["symbol"])
                if instrument_id:
                    self._sync_price(instrument_id, price)

    def _sync_price(self, instrument_id, price):
        print(f"Syncing custom instrument price for {price['symbol']} on {price['date']}")
        payload = {
            "last_traded_price": price["price"],
            "last_traded_on": price["date"],
        }
        existing_prices = self._api_client.get_custom_investment_prices(
            instrument_id, price["date"], price["date"]
        )["prices"]
        if not existing_prices:
            print(
                f"Adding new price of {payload['last_traded_price']} for "
                f"{price['symbol']} on {price['date']}"
            )
            self._api_client.create_custom_investment_price(instrument_id, payload)
            return

        existing_price = existing_prices[0]
        if float(existing_price["last_traded_price"]) != float(payload["last_traded_price"]):
            print(
                f"Replacing existing price of {existing_price['last_traded_price']} with "
                f"{payload['last_traded_price']} for {price['symbol']} on {price['date']}"
            )
            self._api_client.put_custom_investment_price(existing_price["id"], payload)
