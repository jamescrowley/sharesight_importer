import csv
import datetime
import sys

from sharesight_import_plan import SKIP_CASH_TRANSACTION_FLAG


class OpeningBalanceGenerator:
    def __init__(self, api_client):
        self._api_client = api_client

    def generate(self, source_portfolio_id, source_currency, valuation_date,
                 exchange_rates_file_path):
        last_balance_date = valuation_date - datetime.timedelta(days=1)
        valuation = self._api_client.get_valuation_on(
            source_portfolio_id, last_balance_date.strftime("%Y-%m-%d")
        )
        exchange_rates = load_exchange_rates(exchange_rates_file_path, valuation_date)
        print(
            f"Loaded valuation for {source_portfolio_id} on {valuation_date} "
            f"with exchange rates {exchange_rates}"
        )

        rows = [
            self._holding_row(
                holding,
                source_portfolio_id,
                source_currency,
                valuation_date,
                exchange_rates,
            )
            for holding in valuation.get("holdings", [])
        ]
        rows.extend(
            self._cash_row(
                cash_account,
                source_currency,
                valuation_date,
                last_balance_date,
                exchange_rates,
            )
            for cash_account in valuation.get("cash_accounts", [])
        )
        return rows

    def _holding_row(self, holding, source_portfolio_id, source_currency,
                     valuation_date, exchange_rates):
        instrument_currency = self._holding_currency(holding["id"])
        print(holding)
        print(
            f"Creating deemed aquisition using holding {holding.get('symbol')} in "
            f"{instrument_currency} with value {holding.get('value')} from portfolio "
            f"with currency {source_currency}"
        )
        source_exchange_rate = float(
            exchange_rates[f"{source_currency}/{instrument_currency}"]
        )
        value = float(holding["value"])
        quantity = float(holding["quantity"])
        return {
            SKIP_CASH_TRANSACTION_FLAG: True,
            "unique_identifier": f"GENERATED-{holding['symbol']}",
            "transaction_type": "BUY",
            "transaction_date": valuation_date.strftime("%Y-%m-%d"),
            "symbol": remove_portfolio_qualifier(holding["symbol"], source_portfolio_id),
            "instrument_currency": instrument_currency,
            "market": holding["market"],
            "quantity": holding["quantity"],
            "brokerage_in_amount_currency": 0,
            "exchange_rate_aud": float(exchange_rates[f"AUD/{instrument_currency}"]),
            "exchange_rate_gbp": float(exchange_rates[f"GBP/{instrument_currency}"]),
            "price_in_instrument_currency": value / quantity * source_exchange_rate,
            "amount_in_instrument_currency": value * source_exchange_rate,
            "description": "Deemed aquisition at residency commencement",
        }

    def _cash_row(self, cash_account, source_currency, valuation_date,
                  last_balance_date, exchange_rates):
        transactions = self._api_client.get_cash_account_transactions(
            cash_account["cash_account_id"],
            "2000-01-01",
            last_balance_date.strftime("%Y-%m-%d"),
        )["cash_account_transactions"]
        total_amount = sum(float(transaction["amount"]) for transaction in transactions)
        last_balance = transactions[0]["balance"]
        account_currency = cash_account["currency_code"]
        exchange_rate = (
            float(exchange_rates[f"{source_currency}/{account_currency}"])
            if source_currency != account_currency else 1
        )
        account_name = normalize_cash_account_name(cash_account["name"], account_currency)
        calculated_total = float(cash_account["value"]) * exchange_rate
        if round(calculated_total, 2) != round(total_amount, 2):
            print(
                f"WARN Calculated total amount {calculated_total} does not match total amount "
                f"{total_amount} for {account_name}. Sharesight valuation reports will show "
                "the calculated total, which does not match the balances shown in the cash "
                "account itself."
            )
        if round(total_amount, 2) != round(float(last_balance), 2):
            print(
                f"ERROR Total amount {total_amount} does not match last balance {last_balance} "
                f"for {account_name}. This should not happen!",
                file=sys.stderr,
            )
        return {
            "unique_identifier": f"GENERATED-{account_currency}-{account_name}",
            "transaction_type": "DEPOSIT",
            "transaction_date": valuation_date.strftime("%Y-%m-%d"),
            "amount": total_amount,
            "amount_currency": account_currency,
            "cash_account": account_name,
            "description": "Opening Balance",
        }

    def _holding_currency(self, holding_id):
        holding = self._api_client.get_holding(holding_id)
        return holding["holding"]["instrument"]["currency_code"]


def load_exchange_rates(exchange_rates_file_path, requested_date):
    with open(exchange_rates_file_path, mode="r", encoding="utf-8-sig") as file:
        exchange_rates = {row["date"]: row for row in csv.DictReader(file)}

    permitted_days_prior = 3
    for days_prior in range(permitted_days_prior + 1):
        lookup_date = (requested_date - datetime.timedelta(days=days_prior)).strftime("%Y-%m-%d")
        if lookup_date in exchange_rates:
            return exchange_rates[lookup_date]
        if days_prior < permitted_days_prior:
            print(f"Looking for exchange rates for {lookup_date} as {requested_date} not found")
    raise ValueError(
        f"No exchange rates found for date {requested_date} or {permitted_days_prior} days "
        "prior. Please ensure the exchange rates file contains rates for this date."
    )


def remove_portfolio_qualifier(symbol, portfolio_id):
    suffix = f"-{portfolio_id}"
    return symbol[:-len(suffix)] if symbol.endswith(suffix) else symbol


def normalize_cash_account_name(name, currency):
    suffix = f" ({currency})"
    return name[:-len(suffix)] if name.endswith(suffix) else name
