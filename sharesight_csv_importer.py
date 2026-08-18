import csv
import sys
from typing import TextIO
from sharesight_api_client import SharesightApiClient
from sharesight_csv_input import iter_transaction_rows, load_transactions, validate_transactions
from sharesight_import_executor import ImportExecutor, qualify_custom_instrument_symbol
from sharesight_import_plan import (
    PlannedCash,
    SUPPORTED_TRANSACTION_TYPES,
    build_import_plan,
)
from sharesight_import_options import ImportOptions
from sharesight_opening_balances import OpeningBalanceGenerator, normalize_cash_account_name


class SharesightCsvImporter:
    
    INCOME_ACCOUNT_SUFFIX = "Income Account"
    CAPITAL_ACCOUNT_SUFFIX = "Capital Account"
    CUSTOM_INSTRUMENT_SUFFIX = "(AUTO)"
    def __init__(self, api_client: SharesightApiClient):
        self._api_client = api_client

    def _remove_portfolio_qualifier_from_symbol(self, symbol, portfolio_id):
        suffix = f"-{portfolio_id}"
        return symbol[:-len(suffix)] if symbol.endswith(suffix) else symbol

    # sharesight has a bug which means merge_cancel and merge_buy do not work
    # if there are custom instruments with the same identifier in different portfolios
    # even if they are scoped to different portfolios. it also doesn't work for ones scoped globally.
    # so we ensure these have a globally unique identifier by adding the portfolio id to the end of the symbol
    def _get_symbol_key_with_portfolio_qualifier_for_custom_instruments(self, data_row, portfolio_id: str):
        return qualify_custom_instrument_symbol(data_row, portfolio_id)

    def _get_unique_cash_accounts(self, plan):
        return {
            (operation.data.get("amount_currency"), operation.data.get("cash_account") or "")
            for operation in plan
            if isinstance(operation, PlannedCash)
        }

    def _get_unique_custom_instruments(self, transactions, portfolio_id: str) -> list[dict]:
        return [
            {
                "symbol": symbol,
                "symbol_name": symbol_name,
                "instrument_country_code": country_code,
                "instrument_currency": instrument_currency,
                "symbol_type": symbol_type,
            }
            for symbol, symbol_name, country_code, instrument_currency, symbol_type in {
                (
                    self._get_symbol_key_with_portfolio_qualifier_for_custom_instruments(row.data, portfolio_id),
                    row.data.get("symbol_name"),
                    row.data.get("instrument_country_code"),
                    row.data.get("instrument_currency"),
                    row.data.get("symbol_type"),
                )
                for row in iter_transaction_rows(transactions)
                if row.data.get("market", "").lower() == "other"
                and row.data.get("symbol_name")
            }
        ]

    def import_file(self, file_path, portfolio_name, country_code, options=ImportOptions()):
        opening_balances = []
        min_date = options.min_date
        if ((min_date or options.min_line or options.max_line) and options.delete_existing):
            print(f"You probably didn't want to delete existing trades while restarting/running a specific line of the file. Exiting.", file=sys.stderr)
            return None
        if options.opening_balance:
            opening = options.opening_balance
            portfolio_id, _, portfolio_currency = self._get_portfolio_by_name(
                opening.source_portfolio_name
            )
            print(
                f"Generating opening balances on {opening.valuation_date} from "
                f"{opening.source_portfolio_name}"
            )
            opening_balances = OpeningBalanceGenerator(self._api_client).generate(
                portfolio_id,
                portfolio_currency,
                opening.valuation_date,
                opening.exchange_rates_file_path,
            )
            # TODO: if custom instrument prices are not synced between the two portfolios, the opening balances will be incorrect
            # not sure how we check this yet
            print('    ' + '\n    '.join(f"{p}" for p in opening_balances))
            min_date = opening.valuation_date

        self._process_transactions(
            file_path, portfolio_name, country_code, options, min_date, opening_balances
        )
        
    def _process_transactions(self, file_path, portfolio_name, country_code, options,
                              min_date, injected_opening_balances):
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
        cash_accounts_in_file = self._get_unique_cash_accounts(plan)
        portfolio_id, cash_accounts = self._get_or_create_portfolio(
            portfolio_name, country_code, cash_accounts_in_file, options.delete_existing
        )

        custom_instruments_in_file = self._get_unique_custom_instruments(transactions, portfolio_id)
        print(f"Found {len(custom_instruments_in_file)} custom instruments")
        print('    ' + '\n    '.join(f"{p}" for p in custom_instruments_in_file))
        print(f"Creating custom instruments")
        self._create_custom_instruments(portfolio_id, custom_instruments_in_file)

        if options.prices_file_path:
            self._process_prices(options.prices_file_path, portfolio_id, country_code)
        
        executor = ImportExecutor(self._api_client, portfolio_id, country_code, cash_accounts)
        executor.execute(plan)
    
    def _process_prices(self, prices_file_path: TextIO, portfolio_id: str, country_code: str):
        print(f"Syncing custom instruments prices")
        portfolio_custom_investments = self._api_client.get_custom_investments(portfolio_id)['custom_investments']
        portfolio_custom_investments_lookup = {self._remove_portfolio_qualifier_from_symbol(c['code'], portfolio_id): c['id'] for c in portfolio_custom_investments}
        with open(prices_file_path, mode='r', encoding='utf-8-sig') as file:
            reader = csv.DictReader(file)
            for data_row in reader:
                custom_investment_id = portfolio_custom_investments_lookup.get(data_row['symbol'])
                if custom_investment_id:
                    print(f"Syncing custom instrument price for {data_row['symbol']} on {data_row['date']}")
                    api_request_data = {
                        "last_traded_price": data_row['price'],
                        "last_traded_on": data_row['date']
                    }
                    existing_prices = self._api_client.get_custom_investment_prices(custom_investment_id, data_row['date'], data_row['date'])['prices']
                    if existing_prices:
                        if (float(existing_prices[0].get("last_traded_price")) != float(api_request_data['last_traded_price'])):
                            print(f"Replacing existing price of {existing_prices[0].get("last_traded_price")} with {api_request_data['last_traded_price']} for {data_row['symbol']} on {data_row['date']}")
                            self._api_client.put_custom_investment_price(existing_prices[0]['id'], api_request_data)
                    else:
                        print(f"Adding new price of {api_request_data['last_traded_price']} for {data_row['symbol']} on {data_row['date']}")
                        self._api_client.create_custom_investment_price(custom_investment_id, api_request_data)

    def _get_portfolio_by_name(self, portfolio_name: str):
        portfolios = self._api_client.get_portfolios().get('portfolios', [])
        portfolio = next((item for item in portfolios if item["name"] == portfolio_name), None)
        if portfolio:
            portfolio_id = portfolio['id']
            portfolio_currency_code = portfolio['currency_code']
            cash_accounts = self._api_client.get_cash_accounts(portfolio_id).get('cash_accounts', [])
            cash_accounts_lookup = {self._get_cash_account_lookup_key(item["currency"], self._get_cash_account_name_from_sharesight_cash_account(item["name"], item["currency"])): item["id"] for item in cash_accounts}
            print(f"cash accounts: {cash_accounts_lookup}")
            return portfolio_id,cash_accounts_lookup,portfolio_currency_code
        else:
            return None, {}, None
    
    def _get_cash_account_lookup_key(self, cash_account_currency: str, cash_account_name: str):
        return f"{cash_account_currency}-{cash_account_name or 'Account'}"

    def _get_cash_account_name_from_sharesight_cash_account(self, cash_account_name: str, cash_account_currency: str):
        # when fetching from cash_accounts api end point, the field is 'currency'
        # when fetching from valuation api end point, the field is 'currency_code'
        return normalize_cash_account_name(cash_account_name, cash_account_currency)

    def _create_portfolio(self, portfolio_name: str, country_code: str):
        print("Creating portfolio")
        portfolio_data = {
                "name": portfolio_name,
                "country_code": country_code,
                "disable_automatic_transactions": True,
                "broker_email_api_enabled": False
            }
        print(f"Creating portfolio {portfolio_data}")
        return self._api_client.create_portfolio(portfolio_data).get('id')

    def _create_cash_accounts(self, portfolio_id, cash_accounts_in_file):
        cash_accounts = {}
        print(f"Found cash accounts in file: {cash_accounts_in_file}")
        for (cash_account_currency,cash_account_name) in cash_accounts_in_file:
            cash_account_full_name = f"{cash_account_name or 'Account'} ({cash_account_currency})"
            cash_account_id = self._api_client.create_cash_account(portfolio_id, {"name": cash_account_full_name, "currency": cash_account_currency}).get('cash_account').get('id')
            cash_accounts[self._get_cash_account_lookup_key(cash_account_currency, cash_account_name)] = cash_account_id
            print(f"Created cash account {cash_account_full_name} with id {cash_account_id}")

        return cash_accounts
        

    def _get_or_create_portfolio(self, portfolio_name, country_code, cash_accounts_in_file, delete_existing):
        portfolio_id,cash_accounts,_ = self._get_portfolio_by_name(portfolio_name)
        if (portfolio_id == None):
            portfolio_id = self._create_portfolio(portfolio_name, country_code)
            cash_accounts = self._create_cash_accounts(portfolio_id, cash_accounts_in_file)
        elif (delete_existing):
            # print(f"Removing existing portfolio {portfolio_id}")
            # self._api_client.delete_portfolio(portfolio_id)
            # portfolio_id, cash_accounts = None, {}
            print(f"Removing existing trades and cash account transactions")
            self._api_client.delete_all_cash_account_transactions_in_portfolio(portfolio_id)
            self._api_client.delete_all_holdings(portfolio_id)
            print(f"Removing existing custom instruments")
            self._api_client.delete_custom_instruments(portfolio_id, self.CUSTOM_INSTRUMENT_SUFFIX)
            cash_accounts = self._create_cash_accounts(portfolio_id, cash_accounts_in_file)
        else:
            required_cash_account_keys = {
                self._get_cash_account_lookup_key(currency, name)
                for currency, name in cash_accounts_in_file
            }
            missing_cash_accounts = sorted(required_cash_account_keys - set(cash_accounts))
            if missing_cash_accounts:
                raise ValueError(
                    f"Portfolio {portfolio_name} is missing required cash accounts: "
                    f"{', '.join(missing_cash_accounts)}"
                )
        return portfolio_id,cash_accounts
    
    def _create_custom_instruments(self, portfolio_id, custom_instruments_in_file):
        existing_custom_instruments = self._api_client.get_custom_investments(portfolio_id).get('custom_investments', [])
        existing_custom_instruments_lookup = {c['code']: c for c in existing_custom_instruments}
        for data_row in custom_instruments_in_file:
            existing_custom_instrument = existing_custom_instruments_lookup.get(data_row.get("symbol"))
            self._create_or_update_custom_instrument("", portfolio_id, existing_custom_instrument, data_row)

    def _create_or_update_custom_instrument(self, log_line_prefix, portfolio_id, existing_custom_instrument, data_row):
        custom_investment_data = {
            "portfolio_id": portfolio_id,
            "code": data_row.get("symbol"),
            "name": data_row.get("symbol_name") + f" {self.CUSTOM_INSTRUMENT_SUFFIX}",
            "country_code": data_row.get("instrument_country_code"),
            "currency_code": data_row.get("instrument_currency"),
            "investment_type": data_row.get("symbol_type") if data_row.get("symbol_type") else "MANAGED_FUND" #  ORDINARY, WARRANT, SHAREFUND, PROPFUND, PREFERENCE, STAPLEDSEC, OPTIONS, RIGHTS, MANAGED_FUND, FIXED_INTEREST, PIE
        }
        response_json = None
        if (existing_custom_instrument):
            if (existing_custom_instrument["country_code"] != custom_investment_data["country_code"]
                    or existing_custom_instrument["investment_type"] != custom_investment_data["investment_type"]):
                print(f"Custom instrument  {custom_investment_data['code']} has changed country code or investment type from {existing_custom_instrument['country_code']} {existing_custom_instrument['investment_type']} to {custom_investment_data['country_code']} {custom_investment_data['investment_type']}. Re-creating instrument")
                self._api_client.delete_custom_investment(existing_custom_instrument['id'])
                response_json = self._api_client.create_custom_investment(custom_investment_data)
            elif(existing_custom_instrument["name"] != custom_investment_data["name"]):
                print(f"Updating custom instrument {custom_investment_data['code']} name from {existing_custom_instrument['name']} to {custom_investment_data['name']}")
                response_json = self._api_client.update_custom_investment(existing_custom_instrument['id'], custom_investment_data)
        else:
            print(f"Creating custom instrument {custom_investment_data['code']}")
            response_json = self._api_client.create_custom_investment(custom_investment_data)
        if (response_json and response_json.get("currency_code") != custom_investment_data["currency_code"]):
            print(f"{log_line_prefix}\tWARN Sharesight has set {custom_investment_data['code']} currency code to {response_json.get('currency_code')} based on domicile, but instrument currency is set to {custom_investment_data["currency_code"]}")
