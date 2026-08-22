# AGENTS.md

This CLI can mutate or destructively replace real Sharesight portfolios. Never run a live import or export as validation. Use unit tests and the stateful fake HTTP integration service.

## Boundaries

- `sharesight_importer/cli.py`: public CLI, credentials, exit codes.
- `sharesight_schema.py` and `sharesight_csv_input.py`: offline loading and validation.
- `sharesight_import_plan.py`: deterministic operation planning.
- `sharesight_csv_importer.py` and `sharesight_import_executor.py`: discovery and mutation.
- `sharesight_api_client.py`: authentication, endpoints, retries, timeouts, diagnostics.
- `sharesight_custom_instruments.py`: managed instruments and prices.
- `sharesight_residency_reset.py`: Australian reset export and exact reconciliation.

Preserve UTF-8 BOM reads, stable identifiers, merger adjacency, exact `Decimal` reset matching, cash-account naming symmetry, and the `-{portfolio_id}` custom-symbol qualifier. The managed suffix is an ownership marker. Never delete a manual instrument or bypass an “instrument in use” failure.

Local validation must finish before network access. API discovery must finish before mutation. Dry runs may issue only safe reads. Mutation requests are not retried. Destructive failures are fatal.

Use Python 3.12–3.14 through uv and run the complete checks in README. Do not commit credentials, cookies, portfolio data, CSV/JSON exports, `rates/`, editor workspaces, or `.agents/`. Synthetic fixtures under `examples/` and `tests/fixtures/` are allowed.
