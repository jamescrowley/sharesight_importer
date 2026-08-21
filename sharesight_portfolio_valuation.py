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
        portfolio = next((item for item in portfolios if item["name"] == name), None)
        if portfolio is None:
            raise ValueError(f"Source portfolio not found: {name}")
        return portfolio

    def generate_holding_rows(
        self,
        source_portfolio_id,
        source_portfolio_name,
        source_currency,
        valuation_date,
        exchange_rates_file_path,
    ):
        valuation = self._api_client.get_valuation_on(
            source_portfolio_id,
            (valuation_date - datetime.timedelta(days=1)).isoformat(),
        )
        rates, selected_rate_date = load_exchange_rates(
            exchange_rates_file_path, valuation_date
        )
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
    ):
        instrument = self._api_client.get_holding(holding["id"])["holding"]["instrument"]
        instrument_currency = instrument["currency_code"]
        source_value = _decimal(holding["value"], "holding value")
        quantity = _decimal(holding["quantity"], "holding quantity")
        if quantity == 0:
            raise ValueError(f"Holding {holding.get('symbol')} has zero quantity")
        source_to_instrument = _rate(rates, source_currency, instrument_currency)
        amount_in_instrument = source_value * source_to_instrument
        amount_in_aud = _convert_from_instrument(
            amount_in_instrument, rates, "AUD", instrument_currency
        )
        amount_in_gbp = _convert_from_instrument(
            amount_in_instrument, rates, "GBP", instrument_currency
        )
        _validate_cross_rate(source_value, amount_in_aud, rates, source_currency, "AUD")
        _validate_cross_rate(source_value, amount_in_gbp, rates, source_currency, "GBP")

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
            "exchange_rate_aud": _plain(_rate(rates, "AUD", instrument_currency)),
            "exchange_rate_gbp": _plain(_rate(rates, "GBP", instrument_currency)),
            "price_in_instrument_currency": _plain(amount_in_instrument / quantity),
            "amount_in_instrument_currency": _plain(amount_in_instrument),
            "amount_in_aud": _plain(amount_in_aud),
            "amount_in_gbp": _plain(amount_in_gbp),
            "residency_reset_holding_value_in_source_currency": _plain(source_value),
            "description": "Deemed acquisition at residency commencement",
        }
        if holding["market"].lower() == "other":
            custom = custom_instruments.get(holding["symbol"], {})
            row.update(
                {
                    "symbol_name": str(custom.get("name", "")).removesuffix(
                        f" {AUTO_NAME_SUFFIX}"
                    ),
                    "instrument_country_code": custom.get("country_code", ""),
                    "symbol_type": custom.get("investment_type", ""),
                }
            )
        return row


def load_exchange_rates(file_path, requested_date):
    with open(file_path, mode="r", encoding="utf-8-sig") as file:
        rows = {row["date"]: row for row in csv.DictReader(file)}
    for days_prior in range(4):
        selected = requested_date - datetime.timedelta(days=days_prior)
        if selected.isoformat() in rows:
            return rows[selected.isoformat()], selected
    raise ValueError(
        f"No exchange rates found for {requested_date} or the three preceding days"
    )


def _rate(rates, base, quote):
    if base == quote:
        return Decimal(1)
    key = f"{base}/{quote}"
    if not rates.get(key):
        raise ValueError(f"Missing exchange rate {key}")
    return _decimal(rates[key], f"exchange rate {key}")


def _convert_from_instrument(value, rates, target_currency, instrument_currency):
    return value / _rate(rates, target_currency, instrument_currency)


def _validate_cross_rate(
    source_value, converted_value, rates, source_currency, target_currency
):
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
