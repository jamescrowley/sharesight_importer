import datetime
from dataclasses import dataclass

from sharesight_csv_input import MergePair, TransactionRow


@dataclass(frozen=True)
class PlannedOperation:
    line_number: int
    endpoint_type: str
    data: dict
    cash_effect: bool


@dataclass(frozen=True)
class PlannedMerge:
    cancel: TransactionRow
    buy: TransactionRow


def build_import_plan(transactions, transaction_endpoints, non_cash_transaction_types,
                      skip_cash_flag):
    plan = []
    for transaction in transactions:
        if isinstance(transaction, MergePair):
            plan.append(PlannedMerge(cancel=transaction.cancel, buy=transaction.buy))
            continue
        plan.extend(_expand_row(
            transaction,
            transaction_endpoints,
            non_cash_transaction_types,
            skip_cash_flag,
        ))
    return plan


def _expand_row(row, transaction_endpoints, non_cash_transaction_types, skip_cash_flag):
    data = dict(row.data)
    transaction_type = data["transaction_type"]
    if transaction_type in {"RETAINED_NET_INCOME", "RETAINED_EQUALISATION"}:
        return _expand_retained(row.line_number, data, transaction_type)
    if transaction_type in {"CAPITAL_CALL", "CAPITAL_RETURN"} and float(data["amount"]) == 0:
        return []

    endpoint_type = transaction_endpoints[transaction_type]
    operations = [PlannedOperation(
        line_number=row.line_number,
        endpoint_type=endpoint_type,
        data=data,
        cash_effect=(
            not data.get(skip_cash_flag)
            and transaction_type not in non_cash_transaction_types
        ),
    )]

    accrued_income = float(data.get("accrued_income") or 0)
    if accrued_income and transaction_type in {"BUY", "SELL"}:
        accrued_data = dict(data)
        for field in (
            "accrued_income",
            "accrued_income_in_instrument_currency",
            "accrued_income_in_gbp",
            "accrued_income_in_aud",
        ):
            accrued_data.pop(field, None)
        accrued_data.update({
            "unique_identifier": f"{data['unique_identifier']}-accrued_income",
            "amount": data.get("accrued_income"),
            "amount_in_instrument_currency": data.get("accrued_income_in_instrument_currency"),
            "amount_in_gbp": data.get("accrued_income_in_gbp"),
            "amount_in_aud": data.get("accrued_income_in_aud"),
        })
        if transaction_type == "BUY":
            accrued_data["transaction_type"] = "CAPITAL_CALL"
            accrued_endpoint = "trade"
        else:
            accrued_data["goes_ex_on"] = (
                datetime.datetime.strptime(data["transaction_date"], "%Y-%m-%d")
                - datetime.timedelta(days=1)
            ).strftime("%Y-%m-%d")
            accrued_endpoint = "payout"
        operations.append(PlannedOperation(
            line_number=row.line_number,
            endpoint_type=accrued_endpoint,
            data=accrued_data,
            cash_effect=True,
        ))
    return operations


def _expand_retained(line_number, data, transaction_type):
    if float(data["amount"]) == 0:
        return []

    primary_data = dict(data)
    if transaction_type == "RETAINED_NET_INCOME":
        primary_endpoint = "payout"
    else:
        primary_endpoint = "trade"
        primary_data["transaction_type"] = "CAPITAL_RETURN"

    capital_call_data = dict(data)
    capital_call_data.update({
        "unique_identifier": f"{data['unique_identifier']}_CALL",
        "transaction_type": "CAPITAL_CALL",
        "amount": float(data["amount"]) * -1,
        "amount_in_instrument_currency": float(data["amount_in_instrument_currency"]) * -1,
        "amount_in_aud": float(data["amount_in_aud"]) * -1,
        "amount_in_gbp": float(data["amount_in_gbp"]) * -1,
    })
    return [
        PlannedOperation(line_number, primary_endpoint, primary_data, False),
        PlannedOperation(line_number, "trade", capital_call_data, False),
    ]
