import csv
import datetime
from dataclasses import dataclass


TRANSACTION_CSV_FIELDS = [
    "unique_identifier", "transaction_type", "transaction_date", "goes_ex_on",
    "symbol", "market", "quantity", "price_in_instrument_currency", "amount",
    "amount_currency", "cash_account", "description",
    "brokerage_in_instrument_currency", "instrument_currency", "exchange_rate_aud",
    "exchange_rate_gbp", "amount_in_instrument_currency", "amount_in_aud",
    "amount_in_gbp", "accrued_income", "accrued_income_in_instrument_currency",
    "accrued_income_in_aud", "accrued_income_in_gbp", "symbol_name",
    "instrument_country_code", "symbol_type", "tax_withheld",
    "tax_withheld_currency", "tax_credit", "skip_cash_account_transaction",
    "opening_balance_source_portfolio", "opening_balance_source_currency",
    "opening_balance_source_value", "opening_balance_valuation_date",
    "opening_balance_exchange_rate_date",
    "residency_reset_date",
]


@dataclass(frozen=True)
class TransactionRow:
    line_number: int
    data: dict


@dataclass(frozen=True)
class MergePair:
    cancel: TransactionRow
    buy: TransactionRow


def iter_transaction_rows(transactions):
    for transaction in transactions:
        if isinstance(transaction, MergePair):
            yield transaction.cancel
            yield transaction.buy
        else:
            yield transaction


def load_transactions(file_path, injected_rows, min_date,
                      exclude_exdate_transactions_before_min_date, min_line, max_line,
                      inject_before_date=None):
    with open(file_path, mode="r", encoding="utf-8-sig") as file:
        reader = csv.DictReader(file)
        print(f"Found columns in CSV: {reader.fieldnames}")
        rows = [TransactionRow(reader.line_num, dict(data_row)) for data_row in reader]

    if min_line:
        print(f"Filtering transactions before line {min_line}")
    if max_line:
        print(f"Filtering transactions after line {max_line}")
    if min_date:
        print(f"Filtering transactions with a tx date before {min_date}")

    transactions = []
    row_index = 0
    while row_index < len(rows):
        row = rows[row_index]
        transaction_type = row.data.get("transaction_type")
        if transaction_type in {"MERGE_CANCEL", "MERGE_BUY"}:
            if row_index + 1 >= len(rows):
                raise ValueError(f"Line {row.line_number}: merge pair is incomplete")
            partner = rows[row_index + 1]
            expected_partner = "MERGE_BUY" if transaction_type == "MERGE_CANCEL" else "MERGE_CANCEL"
            if partner.data.get("transaction_type") != expected_partner:
                raise ValueError(
                    f"Line {row.line_number}: merge pair must contain adjacent MERGE_CANCEL and MERGE_BUY rows"
                )
            row_selected = _row_is_selected(
                row, min_date, exclude_exdate_transactions_before_min_date, min_line, max_line
            )
            partner_selected = _row_is_selected(
                partner, min_date, exclude_exdate_transactions_before_min_date, min_line, max_line
            )
            if row_selected != partner_selected:
                raise ValueError(
                    f"Lines {row.line_number}-{partner.line_number}: filters cannot select only one row of a merge pair"
                )
            if row_selected:
                cancel, buy = (row, partner) if transaction_type == "MERGE_CANCEL" else (partner, row)
                transactions.append(MergePair(cancel=cancel, buy=buy))
            row_index += 2
            continue
        if _row_is_selected(
            row, min_date, exclude_exdate_transactions_before_min_date, min_line, max_line
        ):
            transactions.append(row)
        row_index += 1

    generated_rows = [TransactionRow(1, dict(data_row)) for data_row in injected_rows]
    if inject_before_date is None:
        return generated_rows + transactions
    before_boundary = [
        transaction for transaction in transactions
        if _transaction_date(transaction) < inject_before_date
    ]
    from_boundary = [
        transaction for transaction in transactions
        if _transaction_date(transaction) >= inject_before_date
    ]
    return before_boundary + generated_rows + from_boundary


def _read_rows(file_path):
    with open(file_path, mode="r", encoding="utf-8-sig") as file:
        reader = csv.DictReader(file)
        return [TransactionRow(reader.line_num, dict(data_row)) for data_row in reader]


def _parse_date(row, field):
    try:
        return datetime.datetime.strptime(row.data[field], "%Y-%m-%d").date()
    except (KeyError, TypeError, ValueError) as error:
        raise ValueError(
            f"Line {row.line_number}: invalid {field} {row.data.get(field)!r}"
        ) from error


def _transaction_date(transaction):
    row = transaction.cancel if isinstance(transaction, MergePair) else transaction
    return _parse_date(row, "transaction_date")


def validate_transactions(transactions, country_code, supported_transaction_types):
    if country_code not in {"AU", "GB"}:
        raise ValueError(f"Unsupported country code: {country_code}. Expected AU or GB")
    for row in iter_transaction_rows(transactions):
        transaction_type = row.data.get("transaction_type")
        if transaction_type not in supported_transaction_types:
            raise ValueError(f"Line {row.line_number}: unsupported transaction type {transaction_type}")


def _row_is_selected(row, min_date, exclude_exdate_transactions_before_min_date, min_line, max_line):
    if min_line and row.line_number < min_line:
        return False
    if max_line and row.line_number > max_line:
        return False
    if min_date:
        transaction_date = datetime.datetime.strptime(row.data["transaction_date"], "%Y-%m-%d").date()
        if transaction_date < min_date:
            return False
        goes_ex_on = row.data.get("goes_ex_on") or ""
        if (
            exclude_exdate_transactions_before_min_date
            and goes_ex_on
            and datetime.datetime.strptime(goes_ex_on, "%Y-%m-%d").date() < min_date
        ):
            return False
    return True
