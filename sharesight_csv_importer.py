import csv
import datetime
import os
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


class SharesightCsvImporter:
    
    INCOME_ACCOUNT_SUFFIX = "Income Account"
    CAPITAL_ACCOUNT_SUFFIX = "Capital Account"
    CUSTOM_INSTRUMENT_SUFFIX = "(AUTO)"
    INTERNAL_SKIP_CASH_TX_FLAG = "skip_cash_account_transaction"

    def __init__(self, api_client: SharesightApiClient):
        self._api_client = api_client

    def _remove_portfolio_qualifier_from_symbol(self, symbol: str, portfolio_id: str):
        return symbol[:-len(f"-{portfolio_id}")] if symbol.endswith(f"-{portfolio_id}") else symbol

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

    def get_internal_exchange_rates(self, exchange_rates_file_path: str, requested_date: datetime.date):
        with open(exchange_rates_file_path, mode='r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            exchange_rates = {row['date']: row for row in reader}
            permitted_days_prior = 3
            lookup_date = requested_date.strftime("%Y-%m-%d")
            if lookup_date not in exchange_rates:
                for i in range(1, permitted_days_prior + 1):
                    lookup_date = (requested_date - datetime.timedelta(days=i)).strftime("%Y-%m-%d")
                    print(f"Looking for exchange rates for {lookup_date} as {requested_date} not found")
                    if lookup_date in exchange_rates:
                        break
                if lookup_date not in exchange_rates:
                    raise ValueError(f"No exchange rates found for date {requested_date} or {permitted_days_prior} days prior. Please ensure the exchange rates file contains rates for this date.")
            return exchange_rates[lookup_date]
    
    def _generate_opening_balances_rows(self, source_portfolio_id: str, source_portfolio_currency_code, valuation_date: datetime.date, exchange_rates_file_path: TextIO | None):
        # get posiitions from the previous day, but value them on the valuation date
        last_balance_date = (valuation_date - datetime.timedelta(days=1))
        valuation = self._api_client.get_valuation_on(source_portfolio_id, last_balance_date.strftime("%Y-%m-%d"))
        exchange_rates = self.get_internal_exchange_rates(exchange_rates_file_path, valuation_date)

        print(f"Loaded valuation for {source_portfolio_id} on {valuation_date} with exchange rates {exchange_rates}")
        for holding in valuation.get("holdings"):
            instrument_currency = self._get_currency_for_holding(holding.get("id"))
            print(holding)
            print(f"Creating deemed aquisition using holding {holding.get('symbol')} in {instrument_currency} with value {holding.get('value')} from portfolio with currency {source_portfolio_currency_code}")
            portfolio_currency_to_instrument_currency_exchange_rate = float(exchange_rates[f"{source_portfolio_currency_code}/{instrument_currency}"])
            aud_to_instrument_currency_exchange_rate = float(exchange_rates[f"AUD/{instrument_currency}"])
            gbp_to_instrument_currency_exchange_rate = float(exchange_rates[f"GBP/{instrument_currency}"])
            price_in_source_portfolio_currency = float(holding.get("value")) / float(holding.get("quantity"))
            price_in_instrument_currency = price_in_source_portfolio_currency * portfolio_currency_to_instrument_currency_exchange_rate
            amount_in_instrument_currency = holding.get("value") * portfolio_currency_to_instrument_currency_exchange_rate

            yield {
                self.INTERNAL_SKIP_CASH_TX_FLAG: True,
                "unique_identifier": f"GENERATED-{holding.get('symbol')}",
                "transaction_type": "BUY",
                "transaction_date": valuation_date.strftime("%Y-%m-%d"),
                "symbol": self._remove_portfolio_qualifier_from_symbol(holding.get("symbol"), source_portfolio_id),
                "instrument_currency": instrument_currency,
                "market": holding.get("market"),
                "quantity": holding.get("quantity"),
                "brokerage_in_amount_currency": 0,
                "exchange_rate_aud": aud_to_instrument_currency_exchange_rate,
                "exchange_rate_gbp": gbp_to_instrument_currency_exchange_rate,
                "price_in_instrument_currency": price_in_instrument_currency,
                "amount_in_instrument_currency": amount_in_instrument_currency,
                "description": "Deemed aquisition at residency commencement"
            }
        # cash account valuations are not reliable from the valuation api endpoint, as they
        # convert from the account currency to the portfolio currency, and back again
        # so instead, load all the transactions, and total the balance
        for cash_account in valuation.get("cash_accounts"):
            transactions = self._api_client.get_cash_account_transactions(cash_account.get('cash_account_id'), "2000-01-01", last_balance_date.strftime("%Y-%m-%d")).get("cash_account_transactions")
            total_amount = sum(float(t.get("amount")) for t in transactions)
            last_balance = transactions[0].get("balance")
            # confusingly value is in the portfolio_currency_code
            # not the account currency
            currency_pair = f"{source_portfolio_currency_code}/{cash_account.get('currency_code')}"
            portfolio_to_cash_account_exchange_rate = float(exchange_rates[currency_pair]) if source_portfolio_currency_code != cash_account.get('currency_code') else 1
            cash_account_name = self._get_cash_account_name_from_sharesight_cash_account(cash_account.get('name'), cash_account.get('currency_code'))
            calculated_total_amount = cash_account.get("value") * portfolio_to_cash_account_exchange_rate
            if (round(calculated_total_amount,2) != round(total_amount,2)):
                print(f"WARN Calculated total amount {calculated_total_amount} does not match total amount {total_amount} for {cash_account_name}. Sharesight valuation reports will show the calculated total, which does not match the balances shown in the cash account itself.")
            if (round(total_amount,2) != round(last_balance,2)):
                print(f"ERROR Total amount {total_amount} does not match last balance {last_balance} for {cash_account_name}. This should not happen!", file=sys.stderr)
            yield {
                "unique_identifier": f"GENERATED-{cash_account.get('currency_code')}-{cash_account_name}",
                "transaction_type": "DEPOSIT",
                "transaction_date": valuation_date.strftime("%Y-%m-%d"),
                "amount": total_amount,
                "amount_currency": cash_account.get("currency_code"),
                "cash_account": cash_account_name,
                "description": "Opening Balance"
            }
    
    def import_file(self, file_path: TextIO, portfolio_name: str, country_code: str, delete_existing: bool, min_date: datetime.date, exclude_exdate_transactions_before_min_date, opening_balance_on: datetime.date | None, opening_balance_from: str | None, min_line: int, max_line: int, prices_file_path: TextIO, exchange_rates_file_path: TextIO | None):
        opening_balances = []
        if ((min_date or min_line or max_line) and delete_existing):
            print(f"You probably didn't want to delete existing trades while restarting/running a specific line of the file. Exiting.", file=sys.stderr)
            return None;
        if opening_balance_on and opening_balance_from:
            portfolio_id,_,portfolio_currency_code = self._get_portfolio_by_name(opening_balance_from)
            print(f"Generating opening balances on {opening_balance_on} from {opening_balance_from}")
            opening_balances = list(self._generate_opening_balances_rows(portfolio_id, portfolio_currency_code, opening_balance_on, exchange_rates_file_path))
            # TODO: if custom instrument prices are not synced between the two portfolios, the opening balances will be incorrect
            # not sure how we check this yet
            print('    ' + '\n    '.join(f"{p}" for p in opening_balances))
            min_date = opening_balance_on
            
        self._process_transactions(file_path, portfolio_name, country_code, delete_existing, min_date, exclude_exdate_transactions_before_min_date, min_line, max_line, opening_balances, prices_file_path)
        
    def _process_transactions(self, file_path: TextIO, portfolio_name: str, country_code: str, delete_existing: bool, min_date: datetime.date, exclude_exdate_transactions_before_min_date: bool, min_line: int, max_line: int, injected_opening_balances: list[dict], prices_file_path: TextIO):
        transactions = load_transactions(
            file_path,
            injected_opening_balances,
            min_date,
            exclude_exdate_transactions_before_min_date,
            min_line,
            max_line,
        )
        validate_transactions(transactions, country_code, SUPPORTED_TRANSACTION_TYPES)
        plan = build_import_plan(transactions, self.INTERNAL_SKIP_CASH_TX_FLAG)
        cash_accounts_in_file = self._get_unique_cash_accounts(plan)
        portfolio_id, cash_accounts = self._get_or_create_portfolio(
            portfolio_name, country_code, cash_accounts_in_file, delete_existing
        )

        custom_instruments_in_file = self._get_unique_custom_instruments(transactions, portfolio_id)
        print(f"Found {len(custom_instruments_in_file)} custom instruments")
        print('    ' + '\n    '.join(f"{p}" for p in custom_instruments_in_file))
        print(f"Creating custom instruments")
        self._create_custom_instruments(portfolio_id, custom_instruments_in_file)

        if (prices_file_path):
            self._process_prices(prices_file_path, portfolio_id, country_code)
        
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
        if cash_account_name.endswith(f" ({cash_account_currency})"):
            return cash_account_name[:-len(f" ({cash_account_currency})")]
        else:
            return cash_account_name

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

    def _get_currency_for_holding(self, holding_id):
        response_holding = self._api_client.get_holding(holding_id)
        return response_holding['holding']['instrument']['currency_code']
