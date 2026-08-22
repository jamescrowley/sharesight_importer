import csv
from decimal import Decimal

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
    return symbol[: -len(suffix)] if symbol.endswith(suffix) else symbol


class CustomInstrumentSynchronizer:
    def __init__(self, api_client):
        self._api_client = api_client

    def sync(
        self,
        portfolio_id,
        transactions,
        prices_file_path=None,
        delete_obsolete=False,
        managed_suffix=AUTO_NAME_SUFFIX,
        existing_instruments=None,
    ):
        instruments = self._instruments_from_transactions(transactions, portfolio_id)
        print(f"Found {len(instruments)} custom instruments")
        print("    " + "\n    ".join(str(instrument) for instrument in instruments))
        print("Creating custom instruments")
        self._sync_instruments(
            portfolio_id, instruments, delete_obsolete, managed_suffix, existing_instruments
        )

        if prices_file_path:
            self._sync_prices(portfolio_id, prices_file_path)

    def _instruments_from_transactions(self, transactions, portfolio_id):
        instruments_by_symbol = {}
        for row in iter_transaction_rows(transactions):
            data = row.data
            if data.get("market", "").lower() != "other" or not data.get("symbol_name"):
                continue
            instrument = {
                "symbol": qualify_custom_instrument_symbol(data, portfolio_id),
                "symbol_name": data.get("symbol_name"),
                "instrument_country_code": data.get("instrument_country_code"),
                "instrument_currency": data.get("instrument_currency"),
                "symbol_type": data.get("symbol_type") or "MANAGED_FUND",
            }
            existing = instruments_by_symbol.get(instrument["symbol"])
            if existing is not None and existing != instrument:
                raise ValueError(
                    f"Line {row.line_number}: conflicting metadata for custom instrument "
                    f"{instrument['symbol']}: {existing} versus {instrument}"
                )
            instruments_by_symbol[instrument["symbol"]] = instrument
        return list(instruments_by_symbol.values())

    def _sync_instruments(
        self,
        portfolio_id,
        instruments,
        delete_obsolete=False,
        managed_suffix=AUTO_NAME_SUFFIX,
        existing_instruments=None,
    ):
        if existing_instruments is None:
            existing_instruments = self._api_client.get_custom_investments(portfolio_id).get(
                "custom_investments", []
            )
        if delete_obsolete:
            required_codes = {instrument["symbol"] for instrument in instruments}
            obsolete_instruments = [
                instrument
                for instrument in existing_instruments
                if instrument["name"].endswith(managed_suffix)
                and instrument["code"] not in required_codes
            ]
            if obsolete_instruments:
                print("Removing obsolete custom instruments")
            for instrument in obsolete_instruments:
                print(f"Removing custom instrument {instrument['code']}")
                self._api_client.delete_custom_investment(instrument["id"])
            obsolete_ids = {instrument["id"] for instrument in obsolete_instruments}
            existing_instruments = [
                instrument
                for instrument in existing_instruments
                if instrument["id"] not in obsolete_ids
            ]
        existing_by_code = {instrument["code"]: instrument for instrument in existing_instruments}
        for instrument in instruments:
            self._create_or_update(
                portfolio_id,
                existing_by_code.get(instrument["symbol"]),
                instrument,
                managed_suffix,
            )

    def _create_or_update(
        self, portfolio_id, existing, instrument, managed_suffix=AUTO_NAME_SUFFIX
    ):
        payload = {
            "portfolio_id": portfolio_id,
            "code": instrument["symbol"],
            "name": f"{instrument['symbol_name']} {managed_suffix}",
            "country_code": instrument["instrument_country_code"],
            "currency_code": instrument["instrument_currency"],
            "investment_type": instrument["symbol_type"] or "MANAGED_FUND",
        }
        saved_instrument = None
        if existing and not existing["name"].endswith(managed_suffix):
            raise ValueError(
                f"Custom instrument code {payload['code']} already belongs to an instrument "
                f"not marked with managed suffix {managed_suffix!r}"
            )
        if existing and self._requires_recreation(existing, payload):
            raise ValueError(
                f"Managed custom instrument {payload['code']} has incompatible country/type "
                "metadata. Refusing to delete and recreate a referenced instrument"
            )
        elif existing and existing["name"] != payload["name"]:
            print(
                f"Updating custom instrument {payload['code']} name from "
                f"{existing['name']} to {payload['name']}"
            )
            saved_instrument = self._api_client.update_custom_investment(existing["id"], payload)
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
        instruments = self._api_client.get_custom_investments(portfolio_id)["custom_investments"]
        instrument_ids = {
            remove_portfolio_qualifier(instrument["code"], portfolio_id): instrument["id"]
            for instrument in instruments
        }
        with open(prices_file_path, encoding="utf-8-sig") as file:
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
        if Decimal(str(existing_price["last_traded_price"])) != Decimal(
            str(payload["last_traded_price"])
        ):
            print(
                f"Replacing existing price of {existing_price['last_traded_price']} with "
                f"{payload['last_traded_price']} for {price['symbol']} on {price['date']}"
            )
            self._api_client.put_custom_investment_price(existing_price["id"], payload)
