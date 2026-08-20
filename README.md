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
  import \
  --portfolio-name "My Portfolio" \
  --file-name transactions.csv \
  --country-code AU
```

Useful optional arguments:

```text
--prices-file-name prices.csv       Import custom-instrument prices
--residency-reset-file-name FILE    Insert a frozen residency reset into full history
--ignore-retained-income            Skip retained net income and retained equalisation rows
--min-date YYYY-MM-DD               Skip earlier transactions
--min-line N / --max-line N         Process a source line range (header is line 1)
--exclude-exdate-transactions-before-min-date
                                    Also apply the minimum date to ex-dates
--verbose                           Print requests as curl commands
--delete-existing                   Clear existing holdings/cash activity first
```

`--delete-existing` is destructive: it deletes portfolio cash transactions, holdings, and importer-created custom instruments before recreating data. The importer refuses to combine this option with date or line filters.

Use `--ignore-retained-income` when retained income and retained equalisation should not be represented in the destination portfolio (for example, where the destination's tax treatment does not require them). It skips both `RETAINED_NET_INCOME` and `RETAINED_EQUALISATION` rows before custom-instrument synchronization and transaction planning. Other income types are unaffected.

### Australian residency reset with complete history

For a portfolio containing its complete pre-residency transaction history, a separate reset export can crystallise each holding immediately before Australian residency and create its deemed-acquisition parcel on the commencement date:

```sh
uv run python __main__.py \
  export-residency-reset \
  --source-portfolio-name "Existing Portfolio" \
  --residency-date 2025-07-01 \
  --exchange-rates-file-name exchange_rates.csv \
  --output-file residency-reset-2025-07-01.csv
```

The export contains an adjacent pair for every holding: a synthetic `SELL` dated one day before residency and a `BUY` dated on the residency date. Both legs have identical quantity, price and converted values, zero brokerage, and `skip_cash_account_transaction=true`, so they do not create cash-account transactions.

Import the complete transaction history and the frozen reset together:

```sh
uv run python __main__.py \
  import -p "Australian Portfolio" -f complete-history.csv -c AU \
  --residency-reset-file-name residency-reset-2025-07-01.csv
```

The importer inserts the reset between pre-residency and residency-date transactions. Before any portfolio setup, it replays quantity-changing rows from the complete history and requires them to exactly match the exported reset quantities. Buys, opening balances, bonus issues, splits, and merger buys add units; sells, cancellations, consolidations, and merger cancellations remove units. In this bespoke CSV, split and consolidation quantities represent the number of units added or removed rather than the resulting balance. Any difference, including a small rounding residual, stops the import so the frozen reset CSV can be corrected or regenerated. Date and line filters are prohibited because quantity reconciliation requires complete history.

The pre-residency history in this single portfolio is intended for performance reporting. Australian tax reports should start on the residency date so the prior-day synthetic disposals and pre-residency income are excluded. Confirm the chosen residency date and deemed-acquisition valuation with an appropriate tax adviser.

## CSV inputs

The canonical output column order is defined in `sharesight_csv_input.py`. Every ordinary row should have the common fields below, leaving non-applicable values blank:

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
