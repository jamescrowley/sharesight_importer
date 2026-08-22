"""Offline validation for the documented canonical CSV inputs."""

import csv
import datetime
import re
from decimal import Decimal, InvalidOperation

from sharesight_csv_input import TRANSACTION_CSV_FIELDS, iter_transaction_rows, load_transactions
from sharesight_import_plan import SUPPORTED_TRANSACTION_TYPES

ISO_CURRENCY = re.compile(r"^[A-Z]{3}$")
ISO_COUNTRY = re.compile(r"^[A-Z]{2}$")
DYNAMIC_FIELD = re.compile(r"^(exchange_rate|amount_in|accrued_income_in)_[a-z]{3}$")
COMMON_REQUIRED = {"unique_identifier", "transaction_type", "transaction_date"}
INSTRUMENT_TYPES = {
    "BUY",
    "SELL",
    "SPLIT",
    "BONUS",
    "CONSOLD",
    "CANCEL",
    "CAPITAL_RETURN",
    "OPENING_BALANCE",
    "ADJUST_COST_BASE",
    "CAPITAL_CALL",
    "DISTRIBUTION",
    "DIVIDEND",
    "RETAINED_NET_INCOME",
    "RETAINED_EQUALISATION",
    "MERGE_CANCEL",
    "MERGE_BUY",
}
CASH_TYPES = {
    "DEPOSIT",
    "WITHDRAWAL",
    "INTEREST_PAYMENT",
    "INTEREST_CHARGED",
    "FEE",
    "FEE_REIMBURSEMENT",
}
DECIMAL_FIELDS = {
    "quantity",
    "price_in_instrument_currency",
    "amount",
    "brokerage_in_instrument_currency",
    "amount_in_instrument_currency",
    "accrued_income",
    "accrued_income_in_instrument_currency",
    "tax_withheld",
    "tax_credit",
    "residency_reset_holding_value_in_source_currency",
}


def validate_import_files(
    transaction_path,
    portfolio_currency,
    prices_file_path=None,
    residency_reset_file_path=None,
):
    currency = normalize_currency(portfolio_currency)
    transactions = load_transactions(transaction_path, [], None, False, None, None)
    validate_transaction_rows(transactions, currency)
    if prices_file_path:
        validate_price_file(prices_file_path)
    if residency_reset_file_path:
        # Full pair/history reconciliation is performed by the residency-reset loader.
        from sharesight_residency_reset import load_and_validate_residency_reset

        load_and_validate_residency_reset(
            residency_reset_file_path, transaction_path, portfolio_currency=currency
        )
    return transactions


def validate_transaction_rows(transactions, portfolio_currency):
    currency = normalize_currency(portfolio_currency)
    required_conversion_fields = {
        f"exchange_rate_{currency.lower()}",
        f"amount_in_{currency.lower()}",
    }
    seen = {}
    custom_metadata = {}
    for row in iter_transaction_rows(transactions):
        data = row.data
        missing = sorted(field for field in COMMON_REQUIRED if not _value(data, field))
        if missing:
            raise ValueError(
                f"Line {row.line_number}: missing required field(s): {', '.join(missing)}"
            )
        kind = data["transaction_type"]
        if kind not in SUPPORTED_TRANSACTION_TYPES:
            raise ValueError(f"Line {row.line_number}: unsupported transaction type {kind}")
        _date(row, "transaction_date", required=True)
        _date(row, "goes_ex_on", required=False)
        identifier = data["unique_identifier"]
        if identifier in seen:
            raise ValueError(
                f"Line {row.line_number}: duplicate unique_identifier {identifier!r} "
                f"(first used on line {seen[identifier]})"
            )
        seen[identifier] = row.line_number

        if kind in INSTRUMENT_TYPES:
            _require(row, "symbol", "market")
        cash_suppressed = str(data.get("skip_cash_account_transaction", "")).lower() == "true"
        if kind in CASH_TYPES or (
            not cash_suppressed
            and kind
            in {
                "BUY",
                "SELL",
                "CAPITAL_CALL",
                "CAPITAL_RETURN",
                "DIVIDEND",
                "DISTRIBUTION",
            }
        ):
            _require(row, "amount", "amount_currency")
            _currency(row, "amount_currency")
        if kind in {
            "BUY",
            "SELL",
            "OPENING_BALANCE",
            "SPLIT",
            "BONUS",
            "CONSOLD",
            "CANCEL",
            "MERGE_CANCEL",
            "MERGE_BUY",
        }:
            _require(row, "quantity")
        if kind in {"BUY", "SELL", "OPENING_BALANCE"}:
            _require(row, "instrument_currency", *required_conversion_fields)
            _currency(row, "instrument_currency")
        if kind in {"DIVIDEND", "DISTRIBUTION", "RETAINED_NET_INCOME", "RETAINED_EQUALISATION"}:
            _require(
                row,
                "instrument_currency",
                "amount_in_instrument_currency",
                f"amount_in_{currency.lower()}",
            )
            _currency(row, "instrument_currency")
        for field, value in data.items():
            if value in (None, ""):
                continue
            if field in DECIMAL_FIELDS or DYNAMIC_FIELD.match(field):
                number = _decimal(row, field)
                if field.startswith("exchange_rate_") and number <= 0:
                    raise ValueError(f"Line {row.line_number}: {field} must be greater than zero")
                if field == "brokerage_in_instrument_currency" and number < 0:
                    raise ValueError(
                        f"Line {row.line_number}: brokerage_in_instrument_currency cannot be negative"
                    )
        _validate_sign(row, kind)
        if data.get("market", "").upper() == "OTHER":
            _require(row, "symbol_name", "instrument_country_code", "instrument_currency")
            if not ISO_COUNTRY.fullmatch(data["instrument_country_code"]):
                raise ValueError(
                    f"Line {row.line_number}: invalid instrument_country_code "
                    f"{data['instrument_country_code']!r}"
                )
            metadata = tuple(
                data.get(field) or ""
                for field in (
                    "symbol_name",
                    "instrument_country_code",
                    "instrument_currency",
                    "symbol_type",
                )
            )
            prior = custom_metadata.setdefault(data["symbol"], metadata)
            if prior != metadata:
                raise ValueError(
                    f"Line {row.line_number}: conflicting metadata for custom instrument {data['symbol']}"
                )
    return transactions


def validate_price_file(file_path):
    with open(file_path, encoding="utf-8-sig", newline="") as source:
        reader = csv.DictReader(source)
        required = {"symbol", "date", "price"}
        missing = sorted(required - set(reader.fieldnames or ()))
        if missing:
            raise ValueError(f"Custom-price CSV is missing columns: {', '.join(missing)}")
        seen = set()
        for row in reader:
            line = reader.line_num
            if not row.get("symbol"):
                raise ValueError(f"Line {line}: custom price requires symbol")
            _parse_date_value(row.get("date"), line, "date")
            try:
                price = Decimal(str(row.get("price")))
            except (InvalidOperation, TypeError) as error:
                raise ValueError(f"Line {line}: invalid price {row.get('price')!r}") from error
            if price <= 0:
                raise ValueError(f"Line {line}: price must be greater than zero")
            key = row["symbol"], row["date"]
            if key in seen:
                raise ValueError(f"Line {line}: duplicate custom price for {key[0]} on {key[1]}")
            seen.add(key)


def report_unknown_columns(file_path, output=print):
    with open(file_path, encoding="utf-8-sig", newline="") as source:
        fields = csv.DictReader(source).fieldnames or []
    known = set(TRANSACTION_CSV_FIELDS)
    unknown = sorted(
        field for field in fields if field not in known and not DYNAMIC_FIELD.match(field)
    )
    if unknown:
        output(f"Ignored auxiliary columns: {', '.join(unknown)}")
    return unknown


def normalize_currency(value):
    if not isinstance(value, str) or not ISO_CURRENCY.fullmatch(value):
        raise ValueError(
            f"Invalid portfolio currency {value!r}; expected an uppercase ISO 4217 code"
        )
    return value


def validate_country(value):
    if not isinstance(value, str) or not ISO_COUNTRY.fullmatch(value):
        raise ValueError(f"Invalid country code {value!r}; expected an uppercase two-letter code")
    return value


def _require(row, *fields):
    missing = [field for field in fields if not _value(row.data, field)]
    if missing:
        raise ValueError(f"Line {row.line_number}: missing required field(s): {', '.join(missing)}")


def _value(data, field):
    value = data.get(field)
    return value is not None and str(value).strip() != ""


def _currency(row, field):
    value = row.data.get(field)
    if not ISO_CURRENCY.fullmatch(value or ""):
        raise ValueError(f"Line {row.line_number}: invalid {field} {value!r}")


def _decimal(row, field):
    try:
        return Decimal(str(row.data[field]))
    except (InvalidOperation, TypeError, KeyError) as error:
        raise ValueError(
            f"Line {row.line_number}: invalid {field} {row.data.get(field)!r}"
        ) from error


def _date(row, field, required):
    value = row.data.get(field)
    if not value and not required:
        return None
    return _parse_date_value(value, row.line_number, field)


def _parse_date_value(value, line, field):
    try:
        return datetime.datetime.strptime(value, "%Y-%m-%d").date()
    except (TypeError, ValueError) as error:
        raise ValueError(f"Line {line}: invalid {field} {value!r}; expected YYYY-MM-DD") from error


def _validate_sign(row, kind):
    if not _value(row.data, "amount"):
        return
    amount = _decimal(row, "amount")
    positive = {
        "BUY",
        "SELL",
        "CAPITAL_CALL",
        "CAPITAL_RETURN",
        "DIVIDEND",
        "DISTRIBUTION",
        "RETAINED_NET_INCOME",
        "RETAINED_EQUALISATION",
        "DEPOSIT",
        "INTEREST_PAYMENT",
        "FEE_REIMBURSEMENT",
    }
    negative = {"WITHDRAWAL", "INTEREST_CHARGED", "FEE"}
    if kind in positive and amount < 0:
        raise ValueError(f"Line {row.line_number}: {kind} amount must be nonnegative")
    if kind in negative and amount > 0:
        raise ValueError(f"Line {row.line_number}: {kind} amount must be nonpositive")
    for field, value in row.data.items():
        if field.startswith("amount_in_") and value not in (None, ""):
            converted = _decimal(row, field)
            if amount and converted and (amount > 0) != (converted > 0):
                raise ValueError(f"Line {row.line_number}: {field} sign must match amount")
