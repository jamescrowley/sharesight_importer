import sys

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
    PlannedCash,
    SUPPORTED_TRANSACTION_TYPES,
    build_import_plan,
)
from sharesight_residency_reset import load_and_validate_residency_reset


def normalize_cash_account_name(name, currency):
    suffix = f" ({currency})"
    return name[:-len(suffix)] if name.endswith(suffix) else name


class SharesightCsvImporter:
    def __init__(self, api_client: SharesightApiClient):
        self._api_client = api_client
        self._custom_instruments = CustomInstrumentSynchronizer(api_client)

    def import_file(self, file_path, portfolio_name, country_code, options=ImportOptions()):
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
        if (
            min_date or options.min_line or options.max_line
        ) and options.delete_existing:
            print(
                "You probably didn't want to delete existing trades while restarting/"
                "running a specific line of the file. Exiting.",
                file=sys.stderr,
            )
            return None

        if options.residency_reset_file_path:
            reset_rows, residency_date = load_and_validate_residency_reset(
                options.residency_reset_file_path, file_path
            )
            opening_balances = [row.data for row in reset_rows]
            inject_before_date = residency_date

        self._process_transactions(
            file_path,
            portfolio_name,
            country_code,
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
                self._is_retained_transaction(transaction)
                for transaction in transactions
            )
            transactions = [
                transaction for transaction in transactions
                if not self._is_retained_transaction(transaction)
            ]
            if retained_count:
                print(
                    f"Ignoring {retained_count} retained income/equalisation "
                    "transaction(s)"
                )
        validate_transactions(transactions, country_code, SUPPORTED_TRANSACTION_TYPES)
        plan = build_import_plan(transactions)
        required_cash_accounts = self._required_cash_accounts(plan)
        portfolio_id, cash_accounts = self._get_or_create_portfolio(
            portfolio_name,
            country_code,
            required_cash_accounts,
            options.delete_existing,
        )

        self._custom_instruments.sync(
            portfolio_id, transactions, options.prices_file_path
        )
        ImportExecutor(
            self._api_client, portfolio_id, country_code, cash_accounts
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
        portfolio = next(
            (item for item in portfolios if item["name"] == portfolio_name), None
        )
        if not portfolio:
            return None, {}, None

        portfolio_id = portfolio["id"]
        cash_accounts = self._api_client.get_cash_accounts(portfolio_id).get(
            "cash_accounts", []
        )
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

    def _get_or_create_portfolio(
        self, portfolio_name, country_code, required_cash_accounts, delete_existing
    ):
        portfolio_id, cash_accounts, _ = self._get_portfolio_by_name(portfolio_name)
        if portfolio_id is None:
            portfolio_id = self._create_portfolio(portfolio_name, country_code)
            cash_accounts = self._create_cash_accounts(
                portfolio_id, required_cash_accounts
            )
        elif delete_existing:
            print("Removing existing trades and cash account transactions")
            self._api_client.delete_all_cash_account_transactions_in_portfolio(portfolio_id)
            self._api_client.delete_all_holdings(portfolio_id)
            print("Removing existing custom instruments")
            self._custom_instruments.delete_generated(portfolio_id)
            cash_accounts = self._create_cash_accounts(
                portfolio_id, required_cash_accounts
            )
        else:
            required_keys = {
                self._cash_account_key(currency, name)
                for currency, name in required_cash_accounts
            }
            missing_accounts = sorted(required_keys - set(cash_accounts))
            if missing_accounts:
                raise ValueError(
                    f"Portfolio {portfolio_name} is missing required cash accounts: "
                    f"{', '.join(missing_accounts)}"
                )
        return portfolio_id, cash_accounts
