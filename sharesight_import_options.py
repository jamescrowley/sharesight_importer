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
