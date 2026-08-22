from decimal import Decimal


def build_trade_payload(portfolio_id, portfolio_currency, data_row):
    exchange_rate_field, amount_field = _currency_fields(portfolio_currency)
    transaction_type = data_row["transaction_type"]
    is_capital_call_or_return = transaction_type in {"CAPITAL_CALL", "CAPITAL_RETURN"}
    transaction_date = data_row.get("transaction_date")
    if is_capital_call_or_return and data_row.get("goes_ex_on"):
        transaction_date = data_row["goes_ex_on"]

    return {
        "unique_identifier": data_row.get("unique_identifier"),
        "transaction_type": transaction_type,
        "transaction_date": transaction_date,
        "portfolio_id": portfolio_id,
        "symbol": data_row.get("symbol"),
        "market": data_row.get("market"),
        "quantity": data_row.get("quantity"),
        "price": data_row.get("price_in_instrument_currency"),
        "brokerage": data_row.get("brokerage_in_instrument_currency"),
        "brokerage_currency_code": data_row.get("instrument_currency"),
        "exchange_rate": data_row.get(exchange_rate_field),
        "cost_base": data_row.get(amount_field) if transaction_type == "OPENING_BALANCE" else "",
        "capital_return_value": (
            _api_number(abs(Decimal(str(data_row.get("amount_in_instrument_currency")))))
            if is_capital_call_or_return
            else ""
        ),
        "paid_on": data_row.get("transaction_date") if is_capital_call_or_return else "",
        "comments": _comments(data_row),
    }


def build_payout_payload(portfolio_id, holding_id, portfolio_currency, data_row):
    _, amount_field = _currency_fields(portfolio_currency)
    return {
        "portfolio_id": portfolio_id,
        "holding_id": holding_id,
        "paid_on": data_row.get("transaction_date"),
        "amount": data_row.get("amount"),
        "goes_ex_on": data_row.get("goes_ex_on"),
        "currency_code": data_row.get("amount_currency"),
        # Supplying banked_amount preserves Sharesight income-report behavior.
        "banked_amount": data_row.get(amount_field),
        "comments": _comments(data_row),
    }


def build_cash_payload(data_row):
    accrued_income = Decimal(str(data_row.get("accrued_income") or 0))
    return {
        "date_time": data_row.get("transaction_date"),
        "description": _comments(data_row),
        "amount": _api_number(Decimal(str(data_row.get("amount"))) - accrued_income),
        "type_name": data_row.get("transaction_type"),
        "foreign_identifier": data_row.get("unique_identifier"),
    }


def build_merge_payload(holding_id, data_row):
    return {
        "holding_id": holding_id,
        "merge_date": data_row.get("goes_ex_on") or data_row.get("transaction_date"),
        "quantity": _api_number(Decimal(str(data_row.get("quantity")))),
        "symbol": data_row.get("symbol"),
        "market": data_row.get("market").upper(),
        "comments": _comments(data_row),
    }


def _currency_fields(portfolio_currency):
    if (
        not isinstance(portfolio_currency, str)
        or len(portfolio_currency) != 3
        or not portfolio_currency.isalpha()
        or not portfolio_currency.isupper()
    ):
        raise ValueError(
            f"Unsupported portfolio currency: {portfolio_currency}. Expected uppercase ISO 4217 code"
        )
    suffix = portfolio_currency.lower()
    return f"exchange_rate_{suffix}", f"amount_in_{suffix}"


def _comments(data_row):
    return f"{data_row.get('unique_identifier')} {data_row.get('description')}"


def _api_number(value):
    """Convert a completed Decimal calculation at the requests JSON boundary."""
    return value.__float__()
