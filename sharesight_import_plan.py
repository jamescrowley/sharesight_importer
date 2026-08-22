import datetime
from dataclasses import dataclass
from decimal import Decimal

from sharesight_csv_input import MergePair, TransactionRow

SKIP_CASH_TRANSACTION_FLAG = "skip_cash_account_transaction"


@dataclass(frozen=True)
class PlannedOperation:
    line_number: int
    data: dict


@dataclass(frozen=True)
class PlannedTrade(PlannedOperation):
    pass


@dataclass(frozen=True)
class PlannedPayout(PlannedOperation):
    pass


@dataclass(frozen=True)
class PlannedCash(PlannedOperation):
    pass


@dataclass(frozen=True)
class PlannedMerge:
    cancel: TransactionRow
    buy: TransactionRow


@dataclass(frozen=True)
class TransactionPolicy:
    operation_type: type[PlannedOperation]
    creates_cash_transaction: bool


# This is the canonical definition of how ordinary CSV transaction types expand.
# Compound retained-income rows and merger pairs are expanded separately below.
TRANSACTION_POLICIES = {
    "DISTRIBUTION": TransactionPolicy(PlannedPayout, True),
    "DIVIDEND": TransactionPolicy(PlannedPayout, True),
    "BUY": TransactionPolicy(PlannedTrade, True),
    "SELL": TransactionPolicy(PlannedTrade, True),
    "SPLIT": TransactionPolicy(PlannedTrade, False),
    "BONUS": TransactionPolicy(PlannedTrade, False),
    "CONSOLD": TransactionPolicy(PlannedTrade, False),
    "CANCEL": TransactionPolicy(PlannedTrade, False),
    "CAPITAL_RETURN": TransactionPolicy(PlannedTrade, True),
    "OPENING_BALANCE": TransactionPolicy(PlannedTrade, False),
    "ADJUST_COST_BASE": TransactionPolicy(PlannedTrade, False),
    "CAPITAL_CALL": TransactionPolicy(PlannedTrade, True),
    "DEPOSIT": TransactionPolicy(PlannedCash, False),
    "WITHDRAWAL": TransactionPolicy(PlannedCash, False),
    "INTEREST_PAYMENT": TransactionPolicy(PlannedCash, False),
    "INTEREST_CHARGED": TransactionPolicy(PlannedCash, False),
    "FEE": TransactionPolicy(PlannedCash, False),
    "FEE_REIMBURSEMENT": TransactionPolicy(PlannedCash, False),
}

COMPOUND_TRANSACTION_TYPES = {"RETAINED_NET_INCOME", "RETAINED_EQUALISATION"}
MERGE_TRANSACTION_TYPES = {"MERGE_CANCEL", "MERGE_BUY"}
SUPPORTED_TRANSACTION_TYPES = frozenset(
    TRANSACTION_POLICIES.keys() | COMPOUND_TRANSACTION_TYPES | MERGE_TRANSACTION_TYPES
)


def build_import_plan(transactions):
    plan = []
    for transaction in transactions:
        if isinstance(transaction, MergePair):
            plan.append(PlannedMerge(cancel=transaction.cancel, buy=transaction.buy))
            continue
        plan.extend(_expand_row(transaction))
    return plan


def _expand_row(row):
    data = dict(row.data)
    transaction_type = data["transaction_type"]
    if transaction_type in COMPOUND_TRANSACTION_TYPES:
        return _expand_retained(row.line_number, data, transaction_type)
    if transaction_type in {"CAPITAL_CALL", "CAPITAL_RETURN"} and _decimal(data["amount"]) == 0:
        return []

    policy = TRANSACTION_POLICIES[transaction_type]
    operations = [policy.operation_type(row.line_number, data)]
    if policy.creates_cash_transaction and not _is_true(data.get(SKIP_CASH_TRANSACTION_FLAG)):
        operations.append(PlannedCash(row.line_number, data))

    accrued_income = _decimal(data.get("accrued_income") or 0)
    if accrued_income and transaction_type in {"BUY", "SELL"}:
        accrued_data = _accrued_income_data(data)
        accrued_operation = PlannedTrade if transaction_type == "BUY" else PlannedPayout
        operations.extend(
            [
                accrued_operation(row.line_number, accrued_data),
                PlannedCash(row.line_number, accrued_data),
            ]
        )
    return operations


def _is_true(value):
    return value is True or (isinstance(value, str) and value.strip().lower() == "true")


def _accrued_income_data(data):
    accrued_data = dict(data)
    for field in tuple(accrued_data):
        if field == "accrued_income" or field.startswith("accrued_income_in_"):
            accrued_data.pop(field, None)
    accrued_data.update(
        {
            "unique_identifier": f"{data['unique_identifier']}-accrued_income",
            "amount": data.get("accrued_income"),
            "amount_in_instrument_currency": data.get("accrued_income_in_instrument_currency"),
        }
    )
    for field, value in data.items():
        if field.startswith("accrued_income_in_"):
            accrued_data[field.replace("accrued_income", "amount", 1)] = value
    if data["transaction_type"] == "BUY":
        accrued_data["transaction_type"] = "CAPITAL_CALL"
    else:
        accrued_data["goes_ex_on"] = (
            datetime.datetime.strptime(data["transaction_date"], "%Y-%m-%d")
            - datetime.timedelta(days=1)
        ).strftime("%Y-%m-%d")
    return accrued_data


def _expand_retained(line_number, data, transaction_type):
    if _decimal(data["amount"]) == 0:
        return []

    primary_data = dict(data)
    if transaction_type == "RETAINED_NET_INCOME":
        primary_operation = PlannedPayout
    else:
        primary_operation = PlannedTrade
        primary_data["transaction_type"] = "CAPITAL_RETURN"

    capital_call_data = dict(data)
    capital_call_data.update(
        {
            "unique_identifier": f"{data['unique_identifier']}_CALL",
            "transaction_type": "CAPITAL_CALL",
            "amount": _plain(-_decimal(data["amount"])),
            "amount_in_instrument_currency": _plain(
                -_decimal(data["amount_in_instrument_currency"])
            ),
        }
    )
    for field, value in data.items():
        if (
            field.startswith("amount_in_")
            and field != "amount_in_instrument_currency"
            and value not in (None, "")
        ):
            capital_call_data[field] = _plain(-_decimal(value))
    return [
        primary_operation(line_number, primary_data),
        PlannedTrade(line_number, capital_call_data),
    ]


def _decimal(value):
    return Decimal(str(value))


def _plain(value):
    return format(value, "f")
