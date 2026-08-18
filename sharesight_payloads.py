COUNTRY_FIELDS = {
    "AU": ("exchange_rate_aud", "amount_in_aud"),
    "GB": ("exchange_rate_gbp", "amount_in_gbp"),
}


def build_trade_payload(portfolio_id, country_code, data_row):
    exchange_rate_field, amount_field = _country_fields(country_code)
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
            abs(float(data_row.get("amount_in_instrument_currency")))
            if is_capital_call_or_return else ""
        ),
        "paid_on": data_row.get("transaction_date") if is_capital_call_or_return else "",
        "comments": _comments(data_row),
    }


def build_payout_payload(portfolio_id, holding_id, country_code, data_row):
    _, amount_field = _country_fields(country_code)
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
    accrued_income = float(data_row.get("accrued_income") or 0)
    return {
        "date_time": data_row.get("transaction_date"),
        "description": _comments(data_row),
        "amount": float(data_row.get("amount")) - accrued_income,
        "type_name": data_row.get("transaction_type"),
        "foreign_identifier": data_row.get("unique_identifier"),
    }


def build_merge_payload(holding_id, data_row):
    return {
        "holding_id": holding_id,
        "merge_date": data_row.get("goes_ex_on") or data_row.get("transaction_date"),
        "quantity": float(data_row.get("quantity")),
        "symbol": data_row.get("symbol"),
        "market": data_row.get("market").upper(),
        "comments": _comments(data_row),
    }


def _country_fields(country_code):
    try:
        return COUNTRY_FIELDS[country_code]
    except KeyError as error:
        raise ValueError(f"Unsupported country code: {country_code}. Expected AU or GB") from error


def _comments(data_row):
    return f"{data_row.get('unique_identifier')} {data_row.get('description')}"
