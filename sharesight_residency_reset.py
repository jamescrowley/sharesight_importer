import csv
import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

from sharesight_csv_input import TRANSACTION_CSV_FIELDS, _parse_date, _read_rows
from sharesight_import_plan import SKIP_CASH_TRANSACTION_FLAG
from sharesight_portfolio_valuation import PortfolioValuationReader


class ResidencyResetExporter:
    def __init__(self, api_client):
        self._valuation_reader = PortfolioValuationReader(api_client)

    def export(
        self,
        source_portfolio_name,
        residency_date,
        exchange_rates_file_path,
        output_file_path,
        overwrite=False,
        portfolio_currency="AUD",
        managed_instrument_name_suffix="(AUTO)",
    ):
        if portfolio_currency != "AUD":
            raise ValueError(
                "Australian residency reset requires AUD destination portfolio currency"
            )
        output_path = Path(output_file_path)
        if output_path.exists() and not overwrite:
            raise FileExistsError(
                f"Residency-reset output already exists: {output_path}. "
                "Use --overwrite to replace it"
            )
        portfolio = self._valuation_reader.find_portfolio(source_portfolio_name)
        holding_rows = self._valuation_reader.generate_holding_rows(
            portfolio["id"],
            portfolio["name"],
            portfolio["currency_code"],
            residency_date,
            exchange_rates_file_path,
            target_currencies=("AUD",),
            managed_instrument_name_suffix=managed_instrument_name_suffix,
        )
        rows = []
        sale_date = residency_date - datetime.timedelta(days=1)
        for holding in holding_rows:
            symbol_key = f"{holding['market']}-{holding['symbol']}"
            common = {
                **holding,
                "residency_reset_date": residency_date.isoformat(),
                SKIP_CASH_TRANSACTION_FLAG: "true",
            }
            rows.extend(
                [
                    {
                        **common,
                        "unique_identifier": (
                            f"GENERATED-RESIDENCY-SELL-{residency_date}-{symbol_key}"
                        ),
                        "transaction_type": "SELL",
                        "transaction_date": sale_date.isoformat(),
                        "description": "Synthetic disposal immediately before Australian tax residency",
                    },
                    {
                        **common,
                        "unique_identifier": (
                            f"GENERATED-RESIDENCY-BUY-{residency_date}-{symbol_key}"
                        ),
                        "transaction_type": "BUY",
                        "transaction_date": residency_date.isoformat(),
                        "description": "Deemed acquisition on commencement of Australian tax residency",
                    },
                ]
            )
        mode = "w" if overwrite else "x"
        with output_path.open(mode, encoding="utf-8", newline="") as output_file:
            fieldnames = [
                field
                for field in TRANSACTION_CSV_FIELDS
                if not field.endswith("_gbp") and field != "exchange_rate_gbp"
            ]
            writer = csv.DictWriter(output_file, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        return rows


def load_and_validate_residency_reset(
    reset_file_path, transactions_file_path, portfolio_currency="AUD"
):
    reset_rows = _read_rows(reset_file_path)
    if not reset_rows or len(reset_rows) % 2:
        raise ValueError("Residency-reset CSV must contain one SELL/BUY pair per holding")

    identifiers = set()
    pairs = []
    residency_dates = set()
    for index in range(0, len(reset_rows), 2):
        sell, buy = reset_rows[index : index + 2]
        _validate_pair(sell, buy, portfolio_currency)
        residency_date = _parse_date(buy, "transaction_date")
        residency_dates.add(residency_date)
        for row in (sell, buy):
            identifier = row.data.get("unique_identifier")
            if not identifier or identifier in identifiers:
                raise ValueError(
                    f"Line {row.line_number}: missing or duplicate residency-reset identifier"
                )
            identifiers.add(identifier)
        pairs.append((sell, buy))
    if len(residency_dates) != 1:
        raise ValueError("All residency-reset BUY rows must use the same residency date")
    residency_date = residency_dates.pop()

    ordinary_rows = _read_rows(transactions_file_path)
    for row in ordinary_rows:
        if row.data.get("unique_identifier") in identifiers:
            raise ValueError(
                f"Line {row.line_number}: identifier also occurs in residency-reset CSV"
            )
    _validate_quantities(ordinary_rows, pairs, residency_date)
    return reset_rows, residency_date


def _validate_pair(sell, buy, portfolio_currency="AUD"):
    if sell.data.get("transaction_type") != "SELL" or buy.data.get("transaction_type") != "BUY":
        raise ValueError(
            f"Lines {sell.line_number}-{buy.line_number}: expected adjacent SELL then BUY"
        )
    sell_date = _parse_date(sell, "transaction_date")
    buy_date = _parse_date(buy, "transaction_date")
    if sell_date != buy_date - datetime.timedelta(days=1):
        raise ValueError(
            f"Lines {sell.line_number}-{buy.line_number}: reset SELL must be one day before BUY"
        )
    currency = portfolio_currency.lower()
    for field in (
        "symbol",
        "market",
        "quantity",
        "price_in_instrument_currency",
        "instrument_currency",
        f"exchange_rate_{currency}",
        "amount_in_instrument_currency",
        f"amount_in_{currency}",
    ):
        if sell.data.get(field) != buy.data.get(field):
            raise ValueError(
                f"Lines {sell.line_number}-{buy.line_number}: reset field {field} does not match"
            )
    for row in (sell, buy):
        if str(row.data.get(SKIP_CASH_TRANSACTION_FLAG, "")).lower() != "true":
            raise ValueError(
                f"Line {row.line_number}: residency-reset trades must suppress cash movement"
            )


def _validate_quantities(ordinary_rows, pairs, residency_date):
    quantities = {}
    for row in ordinary_rows:
        if _parse_date(row, "transaction_date") >= residency_date:
            continue
        data = row.data
        transaction_type = data.get("transaction_type")
        if transaction_type not in {
            "BUY",
            "SELL",
            "OPENING_BALANCE",
            "BONUS",
            "CANCEL",
            "SPLIT",
            "CONSOLD",
            "MERGE_CANCEL",
            "MERGE_BUY",
        }:
            continue
        key = _holding_key(data)
        quantity = _quantity(row)
        if transaction_type in {"SELL", "CANCEL", "CONSOLD", "MERGE_CANCEL"}:
            quantities[key] = quantities.get(key, Decimal()) - quantity
        else:
            quantities[key] = quantities.get(key, Decimal()) + quantity

    reset_quantities = {_holding_key(sell.data): _quantity(sell) for sell, _ in pairs}
    differences = []
    for key in sorted(set(quantities) | set(reset_quantities)):
        history_quantity = quantities.get(key, Decimal())
        reset_quantity = reset_quantities.get(key, Decimal())
        difference = abs(history_quantity - reset_quantity)
        if difference == 0:
            continue
        differences.append(
            f"{key}: history={history_quantity}, reset={reset_quantity}, difference={difference}"
        )
    if differences:
        raise ValueError(
            "Residency-reset quantities must exactly match the positions replayed "
            "from transaction history. Correct or regenerate the frozen reset CSV "
            "before importing: " + "; ".join(differences)
        )


def _holding_key(data):
    return f"{data.get('market', '').upper()}:{data.get('symbol', '').upper()}"


def _quantity(row):
    try:
        return Decimal(str(row.data.get("quantity")))
    except (InvalidOperation, TypeError) as error:
        raise ValueError(
            f"Line {row.line_number}: invalid quantity {row.data.get('quantity')!r}"
        ) from error
