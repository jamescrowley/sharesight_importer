from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True)
class TradeValidationIssue:
    message: str
    is_warning: bool


def validate_trade(data, response_data, holding_currency):
    messages = []
    if holding_currency != data.get("instrument_currency"):
        messages.append(
            TradeValidationIssue(
                f"{data.get('symbol')} has instrument currency code "
                f"{data.get('instrument_currency')} but Sharesight has set it to {holding_currency}",
                is_warning=False,
            )
        )
    if response_data["transaction_type"] not in {"BUY", "SELL"}:
        return messages

    gross_in_instrument_currency = _decimal(response_data["price"]) * _decimal(
        response_data["quantity"]
    )
    exchange_rate = _decimal(response_data["exchange_rate"])
    brokerage_sign = 1 if response_data["transaction_type"] == "BUY" else -1
    brokerage_in_instrument_currency = _decimal(response_data["brokerage"]) * brokerage_sign
    net_in_portfolio_currency = _cents(
        gross_in_instrument_currency / exchange_rate
        + brokerage_in_instrument_currency / exchange_rate,
    )
    if abs(net_in_portfolio_currency) != abs(_decimal(response_data["value"])):
        messages.append(
            TradeValidationIssue(
                f"Sharesight net amount in portfolio currency {net_in_portfolio_currency} "
                f"does not match value {response_data.get('value')} for {data.get('symbol')}: "
                f"{response_data}",
                is_warning=True,
            )
        )

    net_in_instrument_currency = _cents(
        gross_in_instrument_currency + brokerage_in_instrument_currency
    )
    accrued_income = _decimal(data.get("accrued_income_in_instrument_currency") or 0)
    expected_amount = abs(
        _cents(_decimal(data.get("amount_in_instrument_currency")) - accrued_income)
    )
    if net_in_instrument_currency != expected_amount:
        messages.append(
            TradeValidationIssue(
                f"Sharesight net amount in instrument currency {net_in_instrument_currency} "
                f"does not match amount in instrument currency {expected_amount} for "
                f"{data.get('symbol')}: {response_data}",
                is_warning=True,
            )
        )
    return messages


def _decimal(value):
    return Decimal(str(value))


def _cents(value):
    return value.quantize(Decimal("0.01"))
