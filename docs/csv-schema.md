# Canonical CSV schema

Files are comma-separated UTF-8 (a BOM is accepted), with one header row and dates in `YYYY-MM-DD`. Blank means “not applicable”; it never means zero. Unknown auxiliary columns are reported and ignored. Decimal fields use plain base-10 notation without currency symbols or thousands separators.

## Portfolio country and currency

These codes have separate responsibilities:

| Code | Format | Purpose |
| --- | --- | --- |
| Portfolio currency | Uppercase three-letter code such as `AUD`, `GBP`, or `CAD` | Always required by the CLI. Selects the dynamic conversion columns and is checked against Sharesight. |
| Portfolio country | Uppercase two-letter code such as `AU`, `GB`, or `CA` | Required only with `import --create-portfolio`. Sets the new portfolio's domicile. |

Country never defaults or implies currency. Existing portfolios require no country
argument. After creating a portfolio, the importer reads it back and checks that
Sharesight assigned the declared currency before adding anything to it.

## Common columns

| Column | Meaning |
| --- | --- |
| `unique_identifier` | Required, nonblank, file-unique and permanently stable idempotency key. Also becomes the cash `foreign_identifier`. |
| `transaction_type` | One of the types below. |
| `transaction_date` | Trade/payment date. |
| `goes_ex_on` | Optional ex-date; used as the Sharesight transaction date for capital calls/returns. |
| `symbol`, `market` | Instrument code and Sharesight market (`OTHER` for custom instruments). |
| `quantity` | Units transacted. Splits/consolidations contain units added/removed, not the resulting holding. |
| `price_in_instrument_currency` | Price per unit in `instrument_currency`. |
| `brokerage_in_instrument_currency` | Brokerage in instrument currency; use `0` when applicable and free. |
| `instrument_currency` | Uppercase ISO 4217 currency of the instrument. |
| `amount` | Cash movement in `amount_currency`. |
| `amount_currency` | Uppercase ISO 4217 cash-account currency. |
| `cash_account` | Base cash-account name; Sharesight name is `<name or Account> (<currency>)`. |
| `description` | Free-text comment. |
| `amount_in_instrument_currency` | Total value in instrument currency. |
| `accrued_income` | Accrued portion included in `amount`; zero/blank when absent. |
| `accrued_income_in_instrument_currency` | Accrued portion converted to instrument currency. |
| `tax_withheld`, `tax_withheld_currency`, `tax_credit` | Optional payout tax metadata; decimals/currency as named. |
| `skip_cash_account_transaction` | Internal/advanced boolean; `true` suppresses an otherwise generated cash leg. |

## Dynamic portfolio-currency columns

For portfolio currency `CCC`, use lowercase `ccc`:

- `exchange_rate_ccc`: units of instrument currency per one unit of portfolio currency.
- `amount_in_ccc`: total amount converted to portfolio currency.
- `accrued_income_in_ccc`: accrued income converted to portfolio currency.

Thus GBP uses `exchange_rate_gbp`, `amount_in_gbp`; CAD uses `exchange_rate_cad`, `amount_in_cad`. AUD/GBP files remain compatible. Rates must be positive. The CLI verifies the declared portfolio currency against Sharesight before mutation.

## Requirements by type

| Type | Required values | Cash effect |
| --- | --- | --- |
| `BUY`, `SELL` | instrument identity, quantity, price, instrument/cash amount and currency, brokerage, dynamic rate/amount | Yes, unless suppressed; accrued income adds two ordered records. |
| `OPENING_BALANCE` | instrument identity, quantity, price, instrument currency, dynamic rate/amount | No; dynamic amount is cost base. |
| `SPLIT`, `BONUS`, `CONSOLD`, `CANCEL`, `ADJUST_COST_BASE` | instrument identity; quantity where quantity-changing | No. |
| `CAPITAL_CALL`, `CAPITAL_RETURN` | instrument identity and cash amount/currency; zero amounts are omitted | Yes. |
| `DIVIDEND`, `DISTRIBUTION` | instrument identity, amount/currency, dynamic amount, optional ex-date/tax | Payout plus cash. |
| `RETAINED_NET_INCOME` | payout fields and instrument amount | Payout plus offsetting capital call, no cash. |
| `RETAINED_EQUALISATION` | instrument/currency amounts | Capital return plus offsetting capital call, no cash. |
| `DEPOSIT`, `WITHDRAWAL`, `INTEREST_PAYMENT`, `INTEREST_CHARGED`, `FEE`, `FEE_REIMBURSEMENT` | amount, amount currency and cash account | One cash record. Use the real signed cash movement. |
| `MERGE_CANCEL`, `MERGE_BUY` | instrument identity and quantity | No. Must be adjacent, one of each, and selected together by filters. |

`BUY`, deposits, income received, and reimbursements conventionally use positive cash amounts; `SELL` proceeds are positive in this schema because Sharesight determines trade direction from the type; withdrawals, charged interest, and fees use negative amounts. Brokerage is nonnegative. Preserve the signs in `amount_in_*` conversions.

## Custom instruments and prices

For `market=OTHER`, supply `symbol_name`, uppercase two-letter `instrument_country_code`, `instrument_currency`, and optional `symbol_type` (default `MANAGED_FUND`). Metadata for one symbol must agree across the file. The API code is qualified with `-{portfolio_id}`.

The CLI appends `--managed-instrument-name-suffix` (default `(AUTO)`) to the display name. This suffix marks importer-owned instruments; keep it stable. A custom-price file has exactly the required semantic columns `symbol,date,price`; price must be positive and symbol/date pairs unique.

## Accrued and retained income

Accrued income on a buy expands after the buy/cash leg into a `CAPITAL_CALL` plus cash record. On a sell it expands into a payout plus cash record. Derived identifiers append `-accrued_income`.

Retained net income creates a non-cash payout and an offsetting capital call; retained equalisation creates a capital return and offsetting call. The call identifier appends `_CALL`. `--ignore-retained-income` skips both source types.

## Australian residency reset fields

Reset exports add these audit columns: `residency_reset_source_portfolio_name`, `residency_reset_source_portfolio_currency`, `residency_reset_holding_value_in_source_currency`, `residency_reset_valuation_date`, `residency_reset_exchange_rate_date`, and `residency_reset_date`.

Each holding is an adjacent SELL one day before residency followed by BUY on residency day, with matching instrument, quantity, valuation, AUD conversion fields, and `skip_cash_account_transaction=true`. The complete ordinary history is replayed and must exactly equal each reset quantity; no tolerance is applied. Reset rows are inserted between pre-residency and residency-day history.

## Synthetic examples

- [`aud-trades-opening.csv`](../examples/aud-trades-opening.csv): AUD trades and opening balance.
- [`gbp-income-cash.csv`](../examples/gbp-income-cash.csv): payouts, cash, accrued and retained income.
- [`cad-merger-custom.csv`](../examples/cad-merger-custom.csv): CAD merger and custom instrument.
- [`custom-prices.csv`](../examples/custom-prices.csv): custom prices.
- [`aud-residency-reset.csv`](../examples/aud-residency-reset.csv): reset pair format (paired with complete history in real use).
- [`aud-residency-history.csv`](../examples/aud-residency-history.csv): matching complete history for the reset example.
