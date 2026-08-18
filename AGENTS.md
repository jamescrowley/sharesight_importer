# AGENTS.md

## Repository purpose

This repository is a personal Python CLI that imports a bespoke transaction CSV into Sharesight. It talks to live Sharesight APIs and can make destructive portfolio changes. Preserve the existing data model and be conservative around API behavior.

## Architecture

- `__main__.py` parses CLI arguments, reads `SHARESIGHT_CLIENT_ID` and `SHARESIGHT_CLIENT_SECRET`, constructs the API client, and calls the importer.
- `sharesight_csv_importer.py` owns business rules: portfolio/cash-account setup, CSV filtering, transaction dispatch, custom instruments/prices, opening balances, deduplication, and validation messages.
- `sharesight_api_client.py` owns authentication and HTTP requests. Keep raw endpoint details out of importer logic.
- `import_rates.py` and `extract_rates_to_csv.py` are maintenance scripts for opening-balance exchange rates. The former uses an undocumented authenticated internal endpoint.
- `extract_lse_data.py` is an incomplete standalone helper, not part of the CLI path.

## Local setup and checks

Use the uv-managed Python 3.12 environment. Runtime dependencies are declared in `pyproject.toml` and pinned transitively in `uv.lock`.

```sh
uv sync
uv run python -m unittest -v
```

Tests mock the API, use temporary CSV files, and should not require credentials or network access. Keep their fixtures aligned with the current `import_file` signature and converted-currency CSV fields.

There is no configured formatter, linter, type checker, build, or CI workflow. At minimum, run:

```sh
uv run python -m unittest -v
uv run python -m compileall -q .
```

## Change guidelines

- Never run the CLI against Sharesight as a validation step unless the user explicitly authorizes the portfolio, input data, and mutation.
- Treat `--delete_existing` as destructive. It clears cash transactions and holdings and removes custom instruments whose names end in `(AUTO)`.
- Do not commit credentials, browser cookies, API response captures, transaction CSVs, exchange-rate dumps, or other financial data.
- Preserve stable `unique_identifier` values. They drive trade idempotency and cash transaction `foreign_identifier` values.
- Keep AU/GB currency behavior explicit. The importer selects `exchange_rate_aud`/`amount_in_aud` for AU and GBP equivalents for GB; other country codes currently yield invalid placeholder values.
- Preserve CSV headers unless intentionally performing a documented migration. Many fields are conditional and accessed dynamically through `csv.DictReader`.
- Custom instruments use `market=OTHER`, receive a `-{portfolio_id}` symbol suffix, and receive `(AUTO)` in their display names. Both conventions work around Sharesight behavior and are also used during lookup/deletion.
- `MERGE_CANCEL` and `MERGE_BUY` are a coupled adjacent pair. Changes to filtering or iteration must not separate or reorder them.
- Retained income/equalisation expands into multiple non-cash Sharesight records. Verify both legs and ensure no cash transaction is introduced.
- Cash accounts are named `<name or Account> (<currency>)`; keep lookup and normalization symmetric.
- Preserve UTF-8 BOM-tolerant reads (`utf-8-sig`) for supplied CSVs.
- Keep HTTP status/error handling centralized. HTTP 502/504 responses currently retry up to three times with exponential backoff; mock waits in tests of this path.

## Testing expectations

For importer changes, add or update mocked tests covering the emitted API payload, cash side effects, duplicate behavior, and cash-account resync. Particularly high-risk cases are:

- destructive replacement;
- partial imports using date/line filters;
- custom instruments and prices;
- paired mergers;
- payouts and ex-dates;
- accrued or retained income;
- opening balances and cross-currency conversion.

Use realistic current field names such as `price_in_instrument_currency`, `brokerage_in_instrument_currency`, `amount_in_instrument_currency`, and their AU/GBP conversions. Assert that no API method is called when a row should be skipped.

## Working-tree hygiene

The checkout may contain untracked personal/generated artifacts (for example `rates/`, JSON responses, date lists, CSV outputs, and editor workspaces). They belong to the user. Do not delete, reformat, stage, or include them in a change unless explicitly requested. Before reporting completion, distinguish tracked modifications from pre-existing untracked files with `git status --short`.
