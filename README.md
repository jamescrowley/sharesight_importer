# Sharesight CSV importer

A small Python command-line tool for importing transaction and custom-price CSVs into a Sharesight portfolio. It creates portfolios, cash accounts, trades, payouts, custom instruments, and cash transactions through the Sharesight API, while using stable identifiers to make many repeated imports safe.

The code is tailored to Australian (`AU`) and United Kingdom (`GB`) portfolios and to the author's source CSV format. It is not a general-purpose Sharesight CSV importer.

## What it supports

- Trades and adjustments: `BUY`, `SELL`, `SPLIT`, `BONUS`, `CONSOLD`, `CANCEL`, `CAPITAL_RETURN`, `OPENING_BALANCE`, `ADJUST_COST_BASE`, and `CAPITAL_CALL`.
- Income: `DIVIDEND`, `DISTRIBUTION`, `RETAINED_NET_INCOME`, and `RETAINED_EQUALISATION`.
- Holding mergers expressed as adjacent `MERGE_CANCEL`/`MERGE_BUY` rows.
- Cash activity: `DEPOSIT`, `WITHDRAWAL`, `INTEREST_PAYMENT`, `INTEREST_CHARGED`, `FEE`, and `FEE_REIMBURSEMENT`.
- Sharesight custom instruments (`market=OTHER`) and a separate custom-price CSV.
- Date and source-line filters for restarting or importing part of a file.
- Generated opening balances from an existing Sharesight portfolio, using a local exchange-rate CSV.

## Requirements

- Python 3.12 (selected by `.python-version`; the source uses modern type syntax and Python 3.12 f-string parsing).
- A Sharesight API client ID and client secret.
- [uv](https://docs.astral.sh/uv/) for Python and dependency management.

Create the project environment from the checked-in lockfile:

```sh
uv sync
```

uv installs Python 3.12 if necessary and creates `.venv` with the locked `requests` and `curlify` dependencies. Commands can be run through `uv run` without manually activating the environment.

Set credentials without placing them in source files or CSVs:

```sh
export SHARESIGHT_CLIENT_ID='...'
export SHARESIGHT_CLIENT_SECRET='...'
```

## Usage

Run the entry point from the repository root:

```sh
uv run python __main__.py \
  --portfolio_name "My Portfolio" \
  --file_name transactions.csv \
  --country_code AU
```

Useful optional arguments:

```text
--prices_file_name prices.csv       Import custom-instrument prices
--min_date YYYY-MM-DD               Skip earlier transactions
--min_line N / --max_line N         Process a source line range (header is line 1)
--exclude_exdate_transactions_before_min_date
                                    Also apply the minimum date to ex-dates
--verbose                           Print requests as curl commands
--delete_existing                  Clear existing holdings/cash activity first
```

`--delete_existing` is destructive: it deletes portfolio cash transactions, holdings, and importer-created custom instruments before recreating data. The importer refuses to combine this option with date or line filters.

To create opening balances from another portfolio, all three options are required:

```sh
uv run python __main__.py \
  -p "New Portfolio" -f transactions.csv -c AU \
  --opening_balance_on 2025-07-01 \
  --opening_balance_from "Existing Portfolio" \
  --exchange_rates_file_name exchange_rates.csv
```

## CSV inputs

The transaction schema is implicit in `sharesight_csv_importer.py`; there is no formal schema validator. Every row should have the common fields below, leaving non-applicable values blank:

```text
unique_identifier,transaction_type,transaction_date,symbol,market,quantity,
amount,amount_currency,cash_account,description,instrument_currency,
price_in_instrument_currency,amount_in_instrument_currency,
brokerage_in_instrument_currency,exchange_rate_aud,exchange_rate_gbp,
amount_in_aud,amount_in_gbp,goes_ex_on
```

Additional conditional fields include:

- Custom instruments (`market=OTHER`): `symbol_name`, `instrument_country_code`, and optionally `symbol_type` (defaults to `MANAGED_FUND`). Their API symbols are automatically qualified with the portfolio ID.
- Payouts: `tax_withheld`, `tax_withheld_currency`, `tax_credit`, and portfolio-currency values such as `amount_in_aud` or `amount_in_gbp`.
- Accrued income: `accrued_income` plus its instrument/AUD/GBP converted values.
- Opening balances and retained-income rows require the relevant converted amount fields.

Custom prices use a separate three-column file:

```csv
symbol,date,price
MYFUND,2025-06-30,10.50
```

Dates use `YYYY-MM-DD`. Keep `unique_identifier` stable across reruns: Sharesight uses it to reject duplicate trades and the importer reuses it as the cash transaction's foreign identifier. Payout deduplication is less precise and uses holding plus paid date.

## Exchange-rate helper scripts

`import_rates.py` downloads internal Sharesight exchange-rate responses for dates listed one per line in `dates.txt`, placing JSON files in `rates/`. It uses a browser session cookie and an undocumented internal endpoint, so treat it as a personal maintenance helper rather than a stable API integration.

```sh
uv run python import_rates.py --cookie '...cookie value...'
uv run python extract_rates_to_csv.py
```

The second command consolidates `rates/*.json` into `exchange_rates_ss.csv`. Neither cookies nor generated/private financial data should be committed.

`extract_lse_data.py` is a standalone prototype for extracting dates and closing prices from a particular JSON shape; its hard-coded placeholder input means it is not part of the main workflow as written.

## Tests

The tests use `unittest` and mocked API responses, so they should not contact Sharesight:

```sh
uv run python -m unittest -v
```

The suite uses temporary CSV files and mocked Sharesight responses; it does not contact the live API.

## Project layout

- `__main__.py` — command-line argument parsing and application wiring.
- `sharesight_csv_importer.py` — CSV orchestration, conversions, deduplication, and payload construction.
- `sharesight_api_client.py` — OAuth and Sharesight HTTP endpoints.
- `test_sharesight_csv_importer.py` — mocked unit tests present in the working tree.
- `import_rates.py` / `extract_rates_to_csv.py` — exchange-rate maintenance helpers.
- `extract_lse_data.py` — standalone JSON extraction prototype.

## Known limitations

- No CI, formatter, or linter configuration is present.
- API calls can mutate or delete real portfolio data; there is no dry-run mode.
- Only AU and GB portfolio conversion fields are selected by the importer.
- Merge rows must be adjacent and correctly ordered.
- Shorts are unsupported by Sharesight and only produce a warning here.
- Payout identity is inferred from holding and paid date because the API does not expose the same unique-ID behavior as trades.
- The API client retries HTTP 502/504 responses up to three times with exponential backoff.

The project is licensed under the terms in [LICENSE](LICENSE).
