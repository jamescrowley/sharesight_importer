from dataclasses import dataclass
from datetime import date
from os import PathLike


@dataclass(frozen=True)
class ImportOptions:
    delete_existing: bool = False
    min_date: date | None = None
    exclude_exdate_transactions_before_min_date: bool = False
    min_line: int | None = None
    max_line: int | None = None
    prices_file_path: str | PathLike | None = None
    residency_reset_file_path: str | PathLike | None = None
    ignore_retained_income: bool = False
    create_portfolio: bool = False
    dry_run: bool = False
    yes: bool = False
    resync_cash_accounts: bool = True
    managed_instrument_name_suffix: str = "(AUTO)"
