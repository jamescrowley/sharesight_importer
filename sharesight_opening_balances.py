import csv
import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

from sharesight_csv_input import TRANSACTION_CSV_FIELDS
from sharesight_console import warn
from sharesight_custom_instruments import AUTO_NAME_SUFFIX, remove_portfolio_qualifier
from sharesight_import_plan import SKIP_CASH_TRANSACTION_FLAG


class OpeningBalanceExporter:
    def __init__(self, api_client):
        self._api_client = api_client

    def export(self, source_portfolio_name, valuation_date, exchange_rates_file_path,
               output_file_path, overwrite=False):
        output_path = Path(output_file_path)
        if output_path.exists() and not overwrite:
            raise FileExistsError(
                f"Opening-balance output already exists: {output_path}. Use --overwrite to replace it"
            )
        portfolio = self._find_portfolio(source_portfolio_name)
        rows = self.generate(
            portfolio["id"], portfolio["name"], portfolio["currency_code"],
            valuation_date, exchange_rates_file_path,
        )
        mode = "w" if overwrite else "x"
        with output_path.open(mode, encoding="utf-8", newline="") as output_file:
            writer = csv.DictWriter(output_file, fieldnames=TRANSACTION_CSV_FIELDS)
            writer.writeheader()
            writer.writerows(rows)
        return rows

    def generate(self, source_portfolio_id, source_portfolio_name, source_currency,
                 valuation_date, exchange_rates_file_path, include_cash=True):
        last_balance_date = valuation_date - datetime.timedelta(days=1)
        valuation = self._api_client.get_valuation_on(
            source_portfolio_id, last_balance_date.isoformat()
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
            "opening_balance_source_portfolio": source_portfolio_name,
            "opening_balance_source_currency": source_currency,
            "opening_balance_valuation_date": valuation_date.isoformat(),
            "opening_balance_exchange_rate_date": selected_rate_date.isoformat(),
        }
        rows = [
            self._holding_row(
                holding, source_portfolio_id, source_currency, valuation_date,
                rates, custom_instruments, audit,
            )
            for holding in valuation.get("holdings", [])
        ]
        if include_cash:
            rows.extend(
                self._cash_row(
                    account, source_currency, valuation_date, last_balance_date, rates, audit
                )
                for account in valuation.get("cash_accounts", [])
            )
        return rows

    def _holding_row(self, holding, source_portfolio_id, source_currency,
                     valuation_date, rates, custom_instruments, audit):
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
            "opening_balance_source_value": _plain(source_value),
            "description": "Deemed acquisition at residency commencement",
        }
        if holding["market"].lower() == "other":
            custom = custom_instruments.get(holding["symbol"], {})
            row.update({
                "symbol_name": str(custom.get("name", "")).removesuffix(
                    f" {AUTO_NAME_SUFFIX}"
                ),
                "instrument_country_code": custom.get("country_code", ""),
                "symbol_type": custom.get("investment_type", ""),
            })
        return row

    def _cash_row(self, account, source_currency, valuation_date,
                  last_balance_date, rates, audit):
        transactions = self._api_client.get_cash_account_transactions(
            account["cash_account_id"], "2000-01-01", last_balance_date.isoformat()
        ).get("cash_account_transactions", [])
        total = sum((_decimal(item["amount"], "cash transaction amount") for item in transactions), Decimal())
        account_currency = account["currency_code"]
        source_value = _decimal(account["value"], "cash account value")
        expected_total = source_value * _rate(rates, source_currency, account_currency)
        if _cents(expected_total) != _cents(total):
            warn(
                f"Cash account {account['name']} transactions total {total} does not match "
                f"source valuation {expected_total}"
            )
        if transactions and _cents(total) != _cents(_decimal(transactions[0]["balance"], "cash balance")):
            raise ValueError(
                f"Cash account {account['name']} transaction total does not match its latest balance"
            )
        account_name = normalize_cash_account_name(account["name"], account_currency)
        return {
            **audit,
            "unique_identifier": f"GENERATED-CASH-{account_currency}-{account_name}",
            "transaction_type": "DEPOSIT",
            "transaction_date": valuation_date.isoformat(),
            "amount": _plain(total),
            "amount_currency": account_currency,
            "cash_account": account_name,
            "description": "Opening Balance",
            "opening_balance_source_value": _plain(source_value),
            "amount_in_aud": _plain(_convert_from_instrument(total, rates, "AUD", account_currency)),
            "amount_in_gbp": _plain(_convert_from_instrument(total, rates, "GBP", account_currency)),
        }

    def _find_portfolio(self, name):
        portfolios = self._api_client.get_portfolios().get("portfolios", [])
        portfolio = next((item for item in portfolios if item["name"] == name), None)
        if portfolio is None:
            raise ValueError(f"Source portfolio not found: {name}")
        return portfolio


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


def normalize_cash_account_name(name, currency):
    suffix = f" ({currency})"
    return name[:-len(suffix)] if name.endswith(suffix) else name
