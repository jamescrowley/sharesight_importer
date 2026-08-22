# Contributing

Open an issue before changing CSV semantics or API behavior. Keep changes broker-neutral and include synthetic offline tests for payloads, cash effects, duplicates, resync, and failure boundaries. Never include real account data or credentials.

Run all checks in README. Pull requests should describe compatibility impact and update the schema, examples, and changelog when the public contract changes.

Releases use semantic version tags. Build wheel/sdist in CI, attach them to a GitHub release, and summarize schema or safety changes. PyPI publication is not currently part of the release process.
