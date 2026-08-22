import sys
from collections import Counter

from sharesight_api_client import SharesightApiClient
from sharesight_csv_input import (
    load_transactions,
    validate_transactions,
)
from sharesight_custom_instruments import CustomInstrumentSynchronizer
from sharesight_import_executor import ImportExecutor
from sharesight_import_options import ImportOptions
from sharesight_import_plan import (
    COMPOUND_TRANSACTION_TYPES,
    SUPPORTED_TRANSACTION_TYPES,
    PlannedCash,
    build_import_plan,
)
from sharesight_residency_reset import load_and_validate_residency_reset
from sharesight_schema import (
    normalize_currency,
    report_unknown_columns,
    validate_country,
    validate_price_file,
    validate_transaction_rows,
)


def normalize_cash_account_name(name, currency):
    suffix = f" ({currency})"
    return name[: -len(suffix)] if name.endswith(suffix) else name


class SharesightCsvImporter:
    def __init__(self, api_client: SharesightApiClient):
        self._api_client = api_client
        self._custom_instruments = CustomInstrumentSynchronizer(api_client)

    def import_file(
        self,
        file_path,
        portfolio_name,
        portfolio_currency,
        options=None,
        country_code=None,
    ):
        options = options or ImportOptions()
        portfolio_currency = normalize_currency(portfolio_currency)
        if options.create_portfolio:
            if not country_code:
                raise ValueError("--country-code is required when creating a portfolio")
            validate_country(country_code)
        elif country_code is not None:
            raise ValueError("--country-code is only valid when creating a portfolio")
        opening_balances = []
        inject_before_date = None
        min_date = options.min_date
        if options.residency_reset_file_path and (
            options.min_date or options.min_line or options.max_line
        ):
            raise ValueError(
                "A residency reset requires the complete transaction history; "
                "date and line filters are not allowed"
            )
        if (min_date or options.min_line or options.max_line) and options.delete_existing:
            raise ValueError("--delete-existing cannot be combined with date or line filters")

        if options.residency_reset_file_path:
            reset_rows, residency_date = load_and_validate_residency_reset(
                options.residency_reset_file_path,
                file_path,
                portfolio_currency=portfolio_currency,
            )
            opening_balances = [row.data for row in reset_rows]
            inject_before_date = residency_date

        return self._process_transactions(
            file_path,
            portfolio_name,
            country_code,
            portfolio_currency,
            options,
            min_date,
            opening_balances,
            inject_before_date,
        )

    def _process_transactions(
        self,
        file_path,
        portfolio_name,
        country_code,
        portfolio_currency,
        options,
        min_date,
        injected_opening_balances,
        inject_before_date,
    ):
        transactions = load_transactions(
            file_path,
            injected_opening_balances,
            min_date,
            options.exclude_exdate_transactions_before_min_date,
            options.min_line,
            options.max_line,
            inject_before_date,
        )
        if options.ignore_retained_income:
            retained_count = sum(
                self._is_retained_transaction(transaction) for transaction in transactions
            )
            transactions = [
                transaction
                for transaction in transactions
                if not self._is_retained_transaction(transaction)
            ]
            if retained_count:
                print(f"Ignoring {retained_count} retained income/equalisation transaction(s)")
        # All local input files are checked before the first API request.
        validate_transactions(transactions, country_code, SUPPORTED_TRANSACTION_TYPES)
        validate_transaction_rows(transactions, portfolio_currency)
        report_unknown_columns(file_path)
        if options.prices_file_path:
            validate_price_file(options.prices_file_path)
        plan = build_import_plan(transactions)
        required_cash_accounts = self._required_cash_accounts(plan)
        portfolio_id, cash_accounts, state = self._resolve_portfolio(
            portfolio_name,
            country_code,
            portfolio_currency,
            required_cash_accounts,
            options,
        )

        if options.dry_run:
            self._describe_plan(portfolio_name, portfolio_id, plan, state, options)
            return True

        if options.delete_existing:
            self._confirm_destructive(portfolio_name, portfolio_id, state, options)
            self._delete_existing(portfolio_id)
            cash_accounts = self._create_cash_accounts(portfolio_id, required_cash_accounts)

        self._custom_instruments.sync(
            portfolio_id,
            transactions,
            options.prices_file_path,
            delete_obsolete=options.delete_existing,
            managed_suffix=options.managed_instrument_name_suffix,
            existing_instruments=state.get("custom_instruments"),
        )
        return ImportExecutor(
            self._api_client,
            portfolio_id,
            portfolio_currency,
            cash_accounts,
            resync_cash_accounts=options.resync_cash_accounts,
            existing_payouts=state.get("payouts"),
            existing_holdings=state.get("holdings"),
        ).execute(plan)

    @staticmethod
    def _is_retained_transaction(transaction):
        data = getattr(transaction, "data", None)
        return bool(data) and data.get("transaction_type") in COMPOUND_TRANSACTION_TYPES

    @staticmethod
    def _required_cash_accounts(plan):
        return {
            (operation.data.get("amount_currency"), operation.data.get("cash_account") or "")
            for operation in plan
            if isinstance(operation, PlannedCash)
        }

    def _get_portfolio_by_name(self, portfolio_name):
        portfolios = self._api_client.get_portfolios().get("portfolios", [])
        matches = [item for item in portfolios if item["name"] == portfolio_name]
        if len(matches) > 1:
            ids = ", ".join(str(item.get("id")) for item in matches)
            raise ValueError(f"Multiple portfolios named {portfolio_name!r} matched IDs: {ids}")
        portfolio = matches[0] if matches else None
        if not portfolio:
            return None, {}, None

        portfolio_id = portfolio["id"]
        cash_accounts = self._api_client.get_cash_accounts(portfolio_id).get("cash_accounts", [])
        cash_accounts_by_key = {
            self._cash_account_key(
                account["currency"],
                normalize_cash_account_name(account["name"], account["currency"]),
            ): account["id"]
            for account in cash_accounts
        }
        print(f"cash accounts: {cash_accounts_by_key}")
        return portfolio_id, cash_accounts_by_key, portfolio["currency_code"]

    @staticmethod
    def _cash_account_key(currency, name):
        return f"{currency}-{name or 'Account'}"

    def _create_portfolio(self, portfolio_name, country_code):
        payload = {
            "name": portfolio_name,
            "country_code": country_code,
            "disable_automatic_transactions": True,
            "broker_email_api_enabled": False,
        }
        print("Creating portfolio")
        print(f"Creating portfolio {payload}")
        return self._api_client.create_portfolio(payload)["id"]

    def _create_cash_accounts(self, portfolio_id, required_accounts):
        cash_accounts = {}
        print(f"Found cash accounts in file: {required_accounts}")
        for currency, name in required_accounts:
            full_name = f"{name or 'Account'} ({currency})"
            response = self._api_client.create_cash_account(
                portfolio_id, {"name": full_name, "currency": currency}
            )
            cash_account_id = response["cash_account"]["id"]
            cash_accounts[self._cash_account_key(currency, name)] = cash_account_id
            print(f"Created cash account {full_name} with id {cash_account_id}")
        return cash_accounts

    def _resolve_portfolio(
        self, portfolio_name, country_code, portfolio_currency, required_cash_accounts, options
    ):
        portfolio_id, cash_accounts, actual_currency = self._get_portfolio_by_name(portfolio_name)
        if portfolio_id is None:
            if not options.create_portfolio:
                raise ValueError(
                    f"Portfolio not found: {portfolio_name!r}. Use --create-portfolio to create it"
                )
            if options.dry_run:
                return (
                    None,
                    {},
                    {"holdings": [], "cash_accounts": [], "custom_instruments": [], "payouts": []},
                )
            created_portfolio_id = self._create_portfolio(portfolio_name, country_code)
            portfolio_id, _, actual_currency = self._get_portfolio_by_name(portfolio_name)
            if portfolio_id != created_portfolio_id:
                raise RuntimeError(
                    f"Created portfolio {portfolio_name!r} could not be resolved unambiguously"
                )
            normalized_actual = normalize_currency(actual_currency)
            if normalized_actual != portfolio_currency:
                raise ValueError(
                    f"Created portfolio currency mismatch for {portfolio_name!r}: Sharesight "
                    f"reports {normalized_actual}, but --portfolio-currency is "
                    f"{portfolio_currency}"
                )
            cash_accounts = self._create_cash_accounts(portfolio_id, required_cash_accounts)
            return (
                portfolio_id,
                cash_accounts,
                {"holdings": [], "cash_accounts": [], "custom_instruments": [], "payouts": []},
            )

        normalized_actual = normalize_currency(actual_currency)
        if normalized_actual != portfolio_currency:
            raise ValueError(
                f"Portfolio currency mismatch for {portfolio_name!r}: Sharesight reports "
                f"{normalized_actual}, but --portfolio-currency is {portfolio_currency}"
            )
        state = {
            "cash_accounts": self._api_client.get_cash_accounts(portfolio_id).get(
                "cash_accounts", []
            ),
            "holdings": self._api_client.get_portfolio_holdings(portfolio_id).get("holdings", []),
            "custom_instruments": self._api_client.get_custom_investments(portfolio_id).get(
                "custom_investments", []
            ),
            "payouts": self._api_client.get_payouts(portfolio_id).get("payouts", []),
        }
        if not options.delete_existing:
            required_keys = {
                self._cash_account_key(currency, name) for currency, name in required_cash_accounts
            }
            missing_accounts = sorted(required_keys - set(cash_accounts))
            if missing_accounts:
                raise ValueError(
                    f"Portfolio {portfolio_name} is missing required cash accounts: "
                    f"{', '.join(missing_accounts)}"
                )
        return portfolio_id, cash_accounts, state

    def _delete_existing(self, portfolio_id):
        print("Removing existing trades and cash account transactions")
        self._api_client.delete_all_cash_account_transactions_in_portfolio(portfolio_id)
        self._api_client.delete_all_holdings(portfolio_id)

    def _confirm_destructive(self, portfolio_name, portfolio_id, state, options):
        managed = [
            item
            for item in state["custom_instruments"]
            if item.get("name", "").endswith(options.managed_instrument_name_suffix)
        ]
        print(
            f"Destructive replacement target: {portfolio_name} (ID {portfolio_id}); "
            f"{len(state['holdings'])} holding(s), {len(state['cash_accounts'])} cash account(s), "
            f"{len(managed)} managed custom instrument(s)"
        )
        for instrument in managed:
            print(f"  managed instrument: {instrument.get('code')} - {instrument.get('name')}")
        if options.yes:
            return
        if not sys.stdin.isatty():
            raise RuntimeError(
                "Refusing destructive replacement on non-interactive stdin; use --yes"
            )
        try:
            confirmation = input(f"Type the exact portfolio name ({portfolio_name}) to continue: ")
        except EOFError as error:
            raise RuntimeError(
                "Destructive replacement aborted: confirmation input ended"
            ) from error
        if confirmation != portfolio_name:
            raise RuntimeError("Destructive replacement aborted: portfolio name did not match")

    def _describe_plan(self, portfolio_name, portfolio_id, plan, state, options):
        target = (
            f"{portfolio_name} (ID {portfolio_id})"
            if portfolio_id is not None
            else f"new portfolio {portfolio_name}"
        )
        print(f"Dry run for {target}: {len(plan)} planned operation(s); no changes will be made")
        counts = Counter(type(operation).__name__ for operation in plan)
        if counts:
            print(
                "Planned operation counts: "
                + ", ".join(f"{name}={count}" for name, count in sorted(counts.items()))
            )
        for operation in plan:
            if hasattr(operation, "data"):
                print(
                    f"  line {operation.line_number}: {type(operation).__name__} "
                    f"{operation.data.get('unique_identifier')}"
                )
            else:
                print(
                    f"  lines {operation.cancel.line_number}-{operation.buy.line_number}: "
                    f"PlannedMerge {operation.cancel.data.get('unique_identifier')}"
                )
        if options.delete_existing:
            managed = [
                item
                for item in state["custom_instruments"]
                if item.get("name", "").endswith(options.managed_instrument_name_suffix)
            ]
            print(
                f"Would replace {len(state['holdings'])} holding(s), "
                f"{len(state['cash_accounts'])} cash account(s), and inspect "
                f"{len(managed)} managed custom instrument(s)"
            )
            for instrument in managed:
                print(
                    f"  matched managed instrument: {instrument.get('code')} - {instrument.get('name')}"
                )
