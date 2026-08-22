# Sharesight Importer

Sharesight Importer is an MIT-licensed power-user CLI for validating and importing a documented, broker-neutral CSV into a Sharesight portfolio. You normalize your own source data and use API credentials connected to your own account.

This project is independent of Sharesight. It is not endorsed by Sharesight and does not provide tax, legal, or financial advice.

## Install

Python 3.12–3.14 is supported. From a release, download the wheel and install it in an isolated environment. For development:

```sh
uv sync --locked
uv run sharesight-importer --help
```

API access must be enabled for your Sharesight account. Store client credentials only in environment variables; command-line credential arguments are intentionally unsupported:

```sh
export SHARESIGHT_CLIENT_ID='...'
export SHARESIGHT_CLIENT_SECRET='...'
```

`validate` is completely offline and does not require these variables.

## Safe workflow

Read [the canonical CSV schema](docs/csv-schema.md), then validate locally:

```sh
sharesight-importer validate --file-name transactions.csv --portfolio-currency AUD
```

Back up/export the target portfolio in Sharesight, then inspect live state without changing it:

```sh
sharesight-importer import --portfolio-name 'Investments' \
  --portfolio-currency AUD --file-name transactions.csv --dry-run
```

Run the live import only after reviewing that output:

```sh
sharesight-importer import --portfolio-name 'Investments' \
  --portfolio-currency AUD --file-name transactions.csv
```

Portfolio names match exactly. A missing portfolio is an error. Creating one is deliberate and requires its domicile:

```sh
sharesight-importer import --portfolio-name 'New Portfolio' \
  --portfolio-currency CAD --country-code CA --create-portfolio \
  --file-name transactions.csv
```

`--portfolio-currency` is always required and selects the CSV conversion fields. For an
existing portfolio, it is checked against Sharesight before any mutation.
`--country-code` is accepted only with `--create-portfolio`; it supplies the new
portfolio's domicile and never selects currency fields.

Sharesight's create-portfolio request accepts the country but not an explicit currency.
The importer therefore creates the empty portfolio, reads it back, and verifies its
assigned currency before creating cash accounts, instruments, trades, or payouts. If
that check fails, the import stops and the empty portfolio remains for manual review or
removal.

## Destructive replacement and cash resync

`--delete-existing` removes the target's cash accounts/transactions and holdings before replacement. After all local and API preflight checks, the CLI prints the exact portfolio name/ID and affected counts, lists managed custom instruments, and requires the exact portfolio name on interactive stdin. Mismatch, EOF, and non-interactive input abort. Deliberate automation requires `--yes`. A dry run never prompts or mutates.

Managed instruments are identified by `--managed-instrument-name-suffix` (default `(AUTO)`). This is an ownership marker and should remain stable for a portfolio. Replacement preserves referenced managed instruments and deletes only managed instruments absent from the complete replacement input. An “instrument in use” rejection is fatal.

Cash-account resynchronization is enabled after a successful import. It uses an undocumented Sharesight endpoint and may change or disappear without notice. Disable it with `--no-resync-cash-accounts`.

Sharesight provides no transaction spanning a complete import. If execution is interrupted, inspect the portfolio before rerunning. Stable `unique_identifier` values provide trade/cash idempotency; payout deduplication is based on holding and paid date.

## Australian residency reset

This workflow is explicitly Australian and requires an AUD destination:

```sh
sharesight-importer export-residency-reset \
  --source-portfolio-name 'Pre-residency Portfolio' \
  --portfolio-currency AUD --residency-date 2025-07-01 \
  --exchange-rates-file-name exchange-rates.csv \
  --output-file residency-reset.csv
```

The export creates adjacent, cash-suppressed SELL/BUY pairs around the residency date using the prior valuation and supplied rates. Import it with complete history using `--residency-reset-file-name`. Every nonzero difference between replayed history quantity and reset quantity is a hard failure. Confirm the residency date, deemed-acquisition value, and tax treatment with a qualified adviser.

## Development and releases

All tests use mocks or the stateful fake HTTP service; never use a live portfolio for validation.

```sh
uv sync --locked
uv run python -m unittest -v
uv run python -m compileall -q sharesight_importer *.py
uv run ruff format --check .
uv run ruff check .
uv build
```

GitHub releases may attach wheel and source-distribution artifacts. PyPI publication is intentionally out of scope. See [CONTRIBUTING.md](CONTRIBUTING.md), [SECURITY.md](SECURITY.md), and [CHANGELOG.md](CHANGELOG.md).
