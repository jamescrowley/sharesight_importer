import csv
import datetime
from decimal import Decimal, InvalidOperation

from sharesight_custom_instruments import AUTO_NAME_SUFFIX, remove_portfolio_qualifier
from sharesight_import_plan import SKIP_CASH_TRANSACTION_FLAG


class PortfolioValuationReader:
    def __init__(self, api_client):
        self._api_client = api_client

    def find_portfolio(self, name):
        portfolios = self._api_client.get_portfolios().get("portfolios", [])
        matches = [item for item in portfolios if item["name"] == name]
        if not matches:
            raise ValueError(f"Source portfolio not found: {name}")
        if len(matches) > 1:
            ids = ", ".join(str(item.get("id")) for item in matches)
            raise ValueError(f"Multiple source portfolios named {name!r} matched IDs: {ids}")
        return matches[0]

    def generate_holding_rows(
        self,
        source_portfolio_id,
        source_portfolio_name,
        source_currency,
        valuation_date,
        exchange_rates_file_path,
        target_currencies=("AUD", "GBP"),
        managed_instrument_name_suffix=AUTO_NAME_SUFFIX,
    ):
        valuation = self._api_client.get_valuation_on(
            source_portfolio_id,
            (valuation_date - datetime.timedelta(days=1)).isoformat(),
        )
        rates, selected_rate_date = load_exchange_rates(exchange_rates_file_path, valuation_date)
        custom_instruments = {
            item.get("code"): item
            for item in self._api_client.get_custom_investments(source_portfolio_id).get(
                "custom_investments", []
            )
        }
        audit = {
            "residency_reset_source_portfolio_name": source_portfolio_name,
            "residency_reset_source_portfolio_currency": source_currency,
            "residency_reset_valuation_date": valuation_date.isoformat(),
            "residency_reset_exchange_rate_date": selected_rate_date.isoformat(),
        }
        return [
            self._holding_row(
                holding,
                source_portfolio_id,
                source_currency,
                valuation_date,
                rates,
                custom_instruments,
                audit,
                target_currencies,
                managed_instrument_name_suffix,
            )
            for holding in valuation.get("holdings", [])
        ]

    def _holding_row(
        self,
        holding,
        source_portfolio_id,
        source_currency,
        valuation_date,
        rates,
        custom_instruments,
        audit,
        target_currencies,
        managed_instrument_name_suffix,
    ):
        instrument = self._api_client.get_holding(holding["id"])["holding"]["instrument"]
        instrument_currency = instrument["currency_code"]
        source_value = _decimal(holding["value"], "holding value")
        quantity = _decimal(holding["quantity"], "holding quantity")
        if quantity == 0:
            raise ValueError(f"Holding {holding.get('symbol')} has zero quantity")
        source_to_instrument = _rate(rates, source_currency, instrument_currency)
        amount_in_instrument = source_value * source_to_instrument
        converted = {
            target: _convert_from_instrument(
                amount_in_instrument, rates, target, instrument_currency
            )
            for target in target_currencies
        }
        for target, value in converted.items():
            _validate_cross_rate(source_value, value, rates, source_currency, target)

        row = {
            **audit,
            SKIP_CASH_TRANSACTION_FLAG: "true",
            "unique_identifier": f"GENERATED-HOLDING-{holding['symbol']}",
            "transaction_type": "BUY",
            "transaction_date": valuation_date.isoformat(),
            "symbol": remove_portfolio_qualifier(holding["symbol"], source_portfolio_id),
            "market": holding["market"],
            "quantity": _plain(quantity),
            "instrument_currency": instrument_currency,
            "brokerage_in_instrument_currency": "0",
            "price_in_instrument_currency": _plain(amount_in_instrument / quantity),
            "amount_in_instrument_currency": _plain(amount_in_instrument),
            "residency_reset_holding_value_in_source_currency": _plain(source_value),
            "description": "Deemed acquisition at residency commencement",
        }
        for target, value in converted.items():
            suffix = target.lower()
            row[f"exchange_rate_{suffix}"] = _plain(_rate(rates, target, instrument_currency))
            row[f"amount_in_{suffix}"] = _plain(value)
        if holding["market"].lower() == "other":
            custom = custom_instruments.get(holding["symbol"], {})
            row.update(
                {
                    "symbol_name": str(custom.get("name", "")).removesuffix(
                        f" {managed_instrument_name_suffix}"
                    ),
                    "instrument_country_code": custom.get("country_code", ""),
                    "symbol_type": custom.get("investment_type", ""),
                }
            )
        return row


def load_exchange_rates(file_path, requested_date):
    with open(file_path, encoding="utf-8-sig") as file:
        rows = {row["date"]: row for row in csv.DictReader(file)}
    for days_prior in range(4):
        selected = requested_date - datetime.timedelta(days=days_prior)
        if selected.isoformat() in rows:
            return rows[selected.isoformat()], selected
    raise ValueError(f"No exchange rates found for {requested_date} or the three preceding days")


def _rate(rates, base, quote):
    if base == quote:
        return Decimal(1)
    key = f"{base}/{quote}"
    if not rates.get(key):
        raise ValueError(f"Missing exchange rate {key}")
    return _decimal(rates[key], f"exchange rate {key}")


def _convert_from_instrument(value, rates, target_currency, instrument_currency):
    return value / _rate(rates, target_currency, instrument_currency)


def _validate_cross_rate(source_value, converted_value, rates, source_currency, target_currency):
    if source_currency == target_currency:
        direct_value = source_value
    else:
        direct_value = source_value * _rate(rates, source_currency, target_currency)
    if _cents(direct_value) != _cents(converted_value):
        raise ValueError(
            f"Exchange rates disagree converting {source_currency} to {target_currency}: "
            f"direct value {direct_value}, instrument-mediated value {converted_value}"
        )


def _decimal(value, label):
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError) as error:
        raise ValueError(f"Invalid {label}: {value!r}") from error


def _cents(value):
    return value.quantize(Decimal("0.01"))


def _plain(value):
    return format(value, "f")
