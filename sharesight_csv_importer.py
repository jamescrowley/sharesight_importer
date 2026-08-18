import sys

from sharesight_api_client import SharesightApiClient
from sharesight_csv_input import load_transactions, validate_transactions
from sharesight_custom_instruments import CustomInstrumentSynchronizer
from sharesight_import_executor import ImportExecutor
from sharesight_import_options import ImportOptions
from sharesight_import_plan import (
    PlannedCash,
    SUPPORTED_TRANSACTION_TYPES,
    build_import_plan,
)
from sharesight_opening_balances import OpeningBalanceGenerator, normalize_cash_account_name


class SharesightCsvImporter:
    def __init__(self, api_client: SharesightApiClient):
        self._api_client = api_client
        self._custom_instruments = CustomInstrumentSynchronizer(api_client)

    def import_file(self, file_path, portfolio_name, country_code, options=ImportOptions()):
        opening_balances = []
        min_date = options.min_date
        if (
            min_date or options.min_line or options.max_line
        ) and options.delete_existing:
            print(
                "You probably didn't want to delete existing trades while restarting/"
                "running a specific line of the file. Exiting.",
                file=sys.stderr,
            )
            return None

        if options.opening_balance:
            opening_balances = self._generate_opening_balances(options.opening_balance)
            min_date = options.opening_balance.valuation_date

        self._process_transactions(
            file_path,
            portfolio_name,
            country_code,
            options,
            min_date,
            opening_balances,
        )

    def _generate_opening_balances(self, opening_options):
        portfolio_id, _, portfolio_currency = self._get_portfolio_by_name(
            opening_options.source_portfolio_name
        )
        print(
            f"Generating opening balances on {opening_options.valuation_date} from "
            f"{opening_options.source_portfolio_name}"
        )
        rows = OpeningBalanceGenerator(self._api_client).generate(
            portfolio_id,
            portfolio_currency,
            opening_options.valuation_date,
            opening_options.exchange_rates_file_path,
        )
        # Custom instrument prices must already agree between the two portfolios.
        print("    " + "\n    ".join(str(row) for row in rows))
        return rows

    def _process_transactions(
        self,
        file_path,
        portfolio_name,
        country_code,
        options,
        min_date,
        injected_opening_balances,
    ):
        transactions = load_transactions(
            file_path,
            injected_opening_balances,
            min_date,
            options.exclude_exdate_transactions_before_min_date,
            options.min_line,
            options.max_line,
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
