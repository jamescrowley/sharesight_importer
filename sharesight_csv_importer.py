import csv
import datetime
import json
from typing import TextIO
from sharesight_api_client import SharesightApiClient

# to simplify usage, the following API fields are mapped to the same column values when required

# trade:
# capital_return_value -> amount
# paid_on -> transaction_date
# cost_base -> amount

# payout:
# paid_on -> transaction_date (used by trade)

# cash:
# type_name -> transaction_type (used by trade)
# foreign_identifier -> unique_identifier (used by trade)
# date_time -> transaction_date (used by trade)

class SharesightCsvImporter:
    
    INCOME_ACCOUNT_SUFFIX = "Income Account"
    CAPITAL_ACCOUNT_SUFFIX = "Capital Account"
    TRANSACTION_TYPE_TO_API_ENDPOINT = {
        "DISTRIBUTION": "payout",
        "DIVIDEND": "payout",
        "BUY": "trade",
        "SELL": "trade",
        "SPLIT": "trade",
        "BONUS": "trade",
        "CONSOLD": "trade",
        "CANCEL": "trade",
        "MERGE_CANCEL": "merge",
        "MERGE_BUY": "merge",
        "CAPITAL_RETURN": "trade",
        "OPENING_BALANCE": "trade",
        "ADJUST_COST_BASE": "trade",
        "CAPITAL_CALL": "trade",
        "DEPOSIT": "cash",
        "WITHDRAWAL": "cash",
        "INTEREST_PAYMENT": "cash",
        "FEE": "cash",
        "FEE_REIMBURSEMENT": "cash"
    }
    CUSTOM_INSTRUMENT_SUFFIX = "(AUTO)"

    def __init__(self, api_client: SharesightApiClient):
        self._api_client = api_client
        self.portfolio_custom_investments_lookup = {}

    def get_portfolio_holdings_lookup_key(self, portfolio_id: str, symbol: str, market: str):
        return f"{portfolio_id}-{market}-{symbol}".lower()

    def get_portfolio_payouts_lookup_key(self, portfolio_id: str, holding_id: str, paid_on):
        return f"{portfolio_id}-{holding_id}-{paid_on}".lower()
    
    def import_file(self, file_path: TextIO, portfolio_name: str, country_code: str, use_seperate_income_account: bool, use_usd_eur_account: bool, delete_existing: bool, min_date: datetime.date, min_line: int, max_line: int):
        portfolio_id, cash_accounts = self._get_or_create_portfolio(portfolio_name, country_code, use_seperate_income_account, use_usd_eur_account, delete_existing)
        # payouts don't have a unique id, so we have to fetch them and de-duplicate ourselves
        portfolio_payouts = self._api_client.get_payouts(portfolio_id).get('payouts')
        portfolio_payouts_lookup = {self.get_portfolio_payouts_lookup_key(portfolio_id, p['holding_id'], p['paid_on']): p['id'] for p in portfolio_payouts}
        portfolio_holdings = self._api_client.get_portfolio_holdings(portfolio_id)['holdings']
        portfolio_holdings_lookup = {self.get_portfolio_holdings_lookup_key(portfolio_id, h['instrument']['code'], h['instrument']['market_code']): h['id'] for h in portfolio_holdings}
        portfolio_custom_investments = self._api_client.get_custom_investments(portfolio_id)['custom_investments']
        self.portfolio_custom_investments_lookup = {self.get_portfolio_holdings_lookup_key(portfolio_id, c['code'], 'OTHER'): c['id'] for c in portfolio_custom_investments}

        with open(file_path, mode='r', encoding='utf-8-sig') as file:
            reader = csv.DictReader(file)
            print(f"Found columns in CSV: {reader.fieldnames}")
            filtered_reader = reader
            if min_line:
                print(f"Filtering transactions before line {min_line}")
                filtered_reader = filter(lambda row: reader.line_num >= min_line, filtered_reader)
            if max_line:
                print(f"Filtering transactions after line {max_line}")
                filtered_reader = filter(lambda row: reader.line_num <= max_line, filtered_reader)
            if min_date:
                print(f"Filtering transactions before {min_date}")
                filtered_reader = filter(lambda row: datetime.datetime.strptime(row['transaction_date'], "%Y-%m-%d").date() > min_date, filtered_reader)
            
            for data_row in filtered_reader:
                log_line_prefix = f"Line {reader.line_num} ({data_row['transaction_type']})"
                api_endpoint_type = self.TRANSACTION_TYPE_TO_API_ENDPOINT.get(data_row.get('transaction_type'))
                holding_id_lookup_key = self.get_portfolio_holdings_lookup_key(portfolio_id, data_row.get("symbol"), data_row.get("market"))
                match api_endpoint_type:
                    case 'trade':
                        holding_id = self._process_trade(portfolio_id, country_code, cash_accounts, log_line_prefix, data_row, portfolio_payouts_lookup)
                        if(holding_id):
                            print(f"{log_line_prefix}: Saved holding id {holding_id} in {holding_id_lookup_key}")
                            portfolio_holdings_lookup[holding_id_lookup_key] = holding_id
                        else:
                            print(f"{log_line_prefix}: Looking up holding id for {holding_id_lookup_key}")
                            existing_holding_id = portfolio_holdings_lookup.get(holding_id_lookup_key)
                            if (existing_holding_id == None):
                                print(f"{log_line_prefix}: Missing holding id for {holding_id_lookup_key}")
                    case 'payout':
                        # cannot rely on using symbol/market directly, as this doesn't work for custom instruments
                        existing_holding_id = portfolio_holdings_lookup.get(holding_id_lookup_key)
                        if (existing_holding_id == None):
                            print(f'{log_line_prefix}: ERROR Unable to find holding id matching {holding_id_lookup_key} - {data_row.get("symbol")}, {data_row.get("market")}, skipping payout')
                        else:
                            cash_account_id = cash_accounts[data_row.get("cash_account") or ("INCOME")]
                            self._process_payout(portfolio_id, country_code, cash_account_id, log_line_prefix, data_row, existing_holding_id, portfolio_payouts_lookup)
                    case 'merge':
                        existing_holding_id = portfolio_holdings_lookup.get(holding_id_lookup_key)
                        existing_custom_instrument_id = portfolio_custom_investments_lookup.get(holding_id_lookup_key)
                        print(f"{log_line_prefix}: existing_holding_id: {existing_holding_id}, existing_custom_instrument_id: {existing_custom_instrument_id}")
                        next_data_row = reader.__next__()
                        self._process_merge(portfolio_id, existing_custom_instrument_id or existing_holding_id, log_line_prefix, next_data_row)
                    case 'cash':
                        cash_account_id = cash_accounts[data_row.get("cash_account") or ("CAPITAL")]
                        self._process_cash(cash_account_id, log_line_prefix, data_row)
                    case _:
                        print(f"{log_line_prefix}: ERROR Unable to map {data_row.get('transaction_type')} to an API endpoint")
                        return None
        print(f"Syncing cash accounts")
        for cash_account in set(cash_accounts.values()):
            if cash_account:
                self._api_client.resync_cash_account(cash_account)

    def _get_portfolio_by_name(self, portfolio_name: str):
        portfolios = self._api_client.get_portfolios().get('portfolios', [])
        portfolio = next((item for item in portfolios if item["name"] == portfolio_name), None)
        if portfolio:
            portfolio_id = portfolio['id']
            cash_accounts = self._api_client.get_cash_accounts(portfolio_id).get('cash_accounts', [])
            capital_account = next((item for item in cash_accounts if item["name"].endswith(self.CAPITAL_ACCOUNT_SUFFIX)), {}).get('id')
            cash_accounts_lookup = {
                "CAPITAL": capital_account,
                "INCOME": next((item for item in cash_accounts if item["name"].endswith(self.INCOME_ACCOUNT_SUFFIX)), {}).get('id') or capital_account,
                "GBP": next((item for item in cash_accounts if item["currency"] == "GBP"), {}).get('id'),
                "USD": next((item for item in cash_accounts if item["currency"] == "EUR"), {}).get('id'),
                "EUR": next((item for item in cash_accounts if item["currency"] == "EUR"), {}).get('id'),
            }
            print(f"cash accounts: {cash_accounts_lookup}")
            return portfolio_id,cash_accounts_lookup
        else:
            return None, {}

    def _create_portfolio_and_cash_accounts(self, portfolio_name: str, country_code: str, use_seperate_income_account: bool, use_usd_eur_account: bool):
        print("Creating portfolio")
        portfolio_data = {
                "name": portfolio_name,
                "country_code": country_code,
                "disable_automatic_transactions": True,
                "broker_email_api_enabled": False
            }

        portfolio_id = self._api_client.create_portfolio(portfolio_data).get('id')
        print(f"Created portfolio {portfolio_id}")
        capital_cash_account_id = self._api_client.create_cash_account(portfolio_id, {"name": f"{portfolio_name} {self.CAPITAL_ACCOUNT_SUFFIX}", "currency": "GBP"}).get('cash_account').get('id')
        print(f"Created cash account {capital_cash_account_id}")
        if (use_seperate_income_account):
            income_cash_account_id = self._api_client.create_cash_account(portfolio_id, {"name": f"{portfolio_name} {self.INCOME_ACCOUNT_SUFFIX}", "currency": "GBP"}).get('cash_account').get('id')
            print(f"Created income cash account {income_cash_account_id}")
        else:
            income_cash_account_id = capital_cash_account_id

        if (use_usd_eur_account):
            usd_cash_account_id = self._api_client.create_cash_account(portfolio_id, {"name": f"{portfolio_name} USD Account", "currency": "USD"}).get('cash_account').get('id')
            print(f"Created USD cash account {usd_cash_account_id}")
            eur_cash_account_id = self._api_client.create_cash_account(portfolio_id, {"name": f"{portfolio_name} EUR Account", "currency": "EUR"}).get('cash_account').get('id')
            print(f"Created EUR cash account {eur_cash_account_id}")
            return portfolio_id,{
                "INCOME": income_cash_account_id,
                "CAPITAL": capital_cash_account_id,
                "GBP": capital_cash_account_id,
                "USD": usd_cash_account_id,
                "EUR": eur_cash_account_id
            }
        else:
            return portfolio_id,{
                "INCOME": income_cash_account_id,
                "CAPITAL": capital_cash_account_id
            }

    def _get_or_create_portfolio(self, portfolio_name, country_code, use_seperate_income_account, use_usd_eur_account, delete_existing):
        portfolio_id,cash_accounts = self._get_portfolio_by_name(portfolio_name)
        if (delete_existing):
            if (portfolio_id):
                print(f"Removing existing portfolio {portfolio_id}")
                self._api_client.delete_portfolio(portfolio_id)
            #     print(f"Removing existing trades and cash account transactions")
            #     self._api_client.delete_all_cash_account_transactions_in_portfolio(portfolio_id)
            #     self._api_client.delete_all_trades(portfolio_id)
                portfolio_id, cash_accounts = None, {}
        if (portfolio_id == None):
            portfolio_id, cash_accounts = self._create_portfolio_and_cash_accounts(portfolio_name, country_code, use_seperate_income_account, use_usd_eur_account)
        return portfolio_id,cash_accounts
    
    def _process_merge(self, portfolio_id, existing_holding_id, log_line_prefix, data_row):
        merge_data = {
            "holding_id": existing_holding_id,
            "merge_date": data_row.get("transaction_date"),
            "quantity": data_row.get("quantity"),
            "symbol": data_row.get("symbol"),
            "market": data_row.get("market")
        }
        response = self._api_client.try_create_holding_merge(portfolio_id, merge_data)
        errors,response_json  = self._get_errors(response)
        if (errors and ('symbol' in errors and errors['symbol'][0] == "^Can't find instrument for this market and share code")):
            errors = self._create_missing_instrument(log_line_prefix, portfolio_id, data_row)
            if (errors == []):
                response = self._api_client.try_create_holding_merge(portfolio_id, merge_data)
                self._print_response_status(log_line_prefix, merge_data, response)
        else:
            self._print_response_status(log_line_prefix, merge_data, response)

    def _create_missing_instrument(self, log_line_prefix, portfolio_id, data_row):
        # create custom instrument
        custom_investment_data = {
            "portfolio_id": portfolio_id,
            "code": data_row.get("symbol"),
            "name": data_row.get("symbol_name") + f" {self.CUSTOM_INSTRUMENT_SUFFIX}",
            "country_code": "LU" if data_row.get("instrument_currency_code") == "EUR" else "GB" if not data_row.get("instrument_currency_code") else data_row.get("instrument_currency_code")[:2],
            "investment_type": data_row.get("symbol_type") if data_row.get("symbol_type") else "MANAGED_FUND" #  ORDINARY, WARRANT, SHAREFUND, PROPFUND, PREFERENCE, STAPLEDSEC, OPTIONS, RIGHTS, MANAGED_FUND, FIXED_INTEREST, PIE
        }
        print(f"{log_line_prefix}: Creating custom instrument {custom_investment_data['code']}")
        response = self._api_client.try_create_custom_investment(custom_investment_data)
        errors, response_json = self._get_errors(response)
        self._print_response_status(log_line_prefix, custom_investment_data, response)
        if (response.status_code == 200):
            self.portfolio_custom_investments_lookup[self.get_portfolio_holdings_lookup_key(portfolio_id, response_json.get('code'), 'OTHER')] = response_json.get('id')
        
        return errors

    def _process_trade(self, portfolio_id, country_code, cash_accounts, log_line_prefix, data_row, portfolio_payouts_lookup):
        is_capital_call_or_return = data_row.get("transaction_type") == "CAPITAL_CALL" or data_row.get("transaction_type") == "CAPITAL_RETURN"
        # if (data_row.get("accrued_income") and data_row.get("transaction_type") == "SELL"):
        #     # add a seperate income payout for the accrued income
        #     # this is from fixed deposits where the interest is paid at term
        #     accrued_income_row = data_row.copy()
        #     accrued_income_row.pop("accrued_income")
        #     accrued_income_row.update({"amount_in_gbp": abs(float(data_row.get("accrued_income")))})
        #     accrued_income_row.update({"unique_identifier": f"{data_row.get('unique_identifier')}-accrued_income"})
        #     self._process_payout(portfolio_id, country_code, cash_account_id, log_line_prefix, accrued_income_row, holding_id, portfolio_payouts_lookup)
        #     data_row.update({"amount_in_gbp": data_row.get("amount_in_gbp") - abs(float(data_row.get("accrued_income")))})

        # return holding_id
    
        api_request_data = {
            "unique_identifier": data_row.get("unique_identifier"),
            "transaction_type": data_row.get("transaction_type"),
            "transaction_date": data_row.get("transaction_date"),
            "portfolio_id": portfolio_id,
            "symbol": data_row.get("symbol"),
            "market": data_row.get("market"),
            "quantity": data_row.get("quantity"),
            "price": data_row.get("price"), # in instrument currency
            "goes_ex_on": data_row.get("goes_ex_on"),
            # has to be in portfolio currency or instrument currency
            "brokerage": data_row.get("brokerage") if data_row.get("brokerage") else data_row.get("brokerage_in_gbp") if country_code == "GB" else data_row.get("brokerage_in_aud") if country_code == "AU" else "??",
            "brokerage_currency_code": data_row.get("brokerage_currency_code") if data_row.get("brokerage_currency_code") else "GBP" if country_code == "GB" else "AUD" if country_code == "AU" else "??",
            "exchange_rate": data_row.get("exchange_rate_gbp") if country_code == "GB" and data_row.get("exchange_rate_gbp") else data_row.get("exchange_rate_aud") if country_code=="AU" and data_row.get("exchange_rate_aud") else "1",
            # needs to be in portfolio currency
            "cost_base": (data_row.get("amount_in_gbp") * 1 if country_code == "GB" else 1/float(data_row.get("exchange_rate_aud"))) if data_row.get("transaction_type") == "OPENING_BALANCE" else "",
            "capital_return_value": str(abs(float(data_row.get("amount_in_gbp")))) if is_capital_call_or_return else "",
            "paid_on": data_row.get("transaction_date") if is_capital_call_or_return else "",
            "comments": data_row.get("description")
        }
        response = self._api_client.try_create_trade(api_request_data)
        errors,response_json  = self._get_errors(response)
        # workaround to "We do not have a price on 18 Sep 2019" error
        if (response.status_code != 200 and data_row.get("transaction_type") == "OPENING_BALANCE" and errors['market_price'][0] == "^We do not have a price on 18 Sep 2019"):
            print(f"{log_line_prefix}: {response.status_code} (handled) {response.url} Falling back to BUY transaction type, this will need modifying in the UI")
            api_request_data['transaction_type'] = "BUY"
            response = self._api_client.try_create_trade(api_request_data)
            errors,response_json = self._get_errors(response)
            self._print_response_status(log_line_prefix, api_request_data, response)
        if (errors and 'instrument_id' in errors and errors['instrument_id'][0] == "^Instrument does not exist"):
            print(f"{log_line_prefix}: {response.status_code} (handled) {response.url} ")
            if (data_row['market'].lower()!='other'):
                print(f"{log_line_prefix}: WARN Instrument data points to {data_row['market']} but needs to be 'other' for custom instrument")
            errors = self._create_missing_instrument(log_line_prefix, portfolio_id, data_row)
            if (errors == []):
                response = self._api_client.try_create_trade(api_request_data)
                errors,response_json = self._get_errors(response)
                self._print_response_status(log_line_prefix, api_request_data, response)
            else:
                print(f"{log_line_prefix}: ERROR Unable to create custom instrument {data_row.get('symbol')}")
                self._print_response_status(log_line_prefix, api_request_data, response)
        else:
            self._print_response_status(log_line_prefix, api_request_data, response)
        
        response_data = response_json.get('trade')
        holding_id = response_data.get('holding_id') if response_data else None
        if (not holding_id and len(errors) == 0):
            print(f"{log_line_prefix}: Couldn't find holding id but no error - {response_json}")
        cash_account_id = cash_accounts[data_row.get("cash_account") or ("INCOME" if is_capital_call_or_return else "CAPITAL")]
        self._process_cash(cash_account_id, log_line_prefix, data_row)

        if (data_row.get("accrued_income") and (data_row.get("transaction_type") == "SELL" or data_row.get("transaction_type") == "BUY")):
            accrued_income_row = data_row.copy()
            accrued_income_row.pop("accrued_income")
            accrued_income_row.update({"amount_in_gbp": abs(float(data_row.get("accrued_income")))})
            accrued_income_row.update({"unique_identifier": f"{data_row.get('unique_identifier')}-accrued_income"})
            if (data_row.get("transaction_type") == "SELL"):
                self._process_payout(portfolio_id, country_code, cash_account_id, log_line_prefix, accrued_income_row, holding_id, portfolio_payouts_lookup)
            elif (data_row.get("transaction_type") == "BUY"):
                if country_code == "GB":
                    print(f"{log_line_prefix}: TODO: adjust capital call on interest payment")
                accrued_income_row.update({"transaction_type": "CAPITAL_CALL"})
                self._process_trade(portfolio_id, country_code, cash_accounts, log_line_prefix, accrued_income_row, portfolio_payouts_lookup)
        
        return holding_id

    def _process_payout(self, portfolio_id, country_code, cash_account_id, log_line_prefix, data_row, existing_holding_id, portfolio_payouts_lookup):
        existing_payout = portfolio_payouts_lookup.get(self.get_portfolio_payouts_lookup_key(portfolio_id, existing_holding_id, data_row.get("transaction_date")))
        if (not existing_payout):
            api_request_data = {
                "portfolio_id": portfolio_id,
                "holding_id": existing_holding_id,
                "paid_on": data_row.get("transaction_date"),
                "amount": data_row.get("amount_in_gbp"),
                "goes_ex_on": data_row.get("goes_ex_on"),
                "currency_code": data_row.get("currency_code"),
                "exchange_rate": data_row.get("exchange_rate_gbp") if country_code == "GB" and data_row.get("exchange_rate_gbp") else data_row.get("exchange_rate_aud") if country_code=="AU" and data_row.get("exchange_rate_aud") else "1",
            }

            response = self._api_client.try_create_payout(api_request_data)
            self._print_response_status(log_line_prefix, api_request_data, response)
        else:
            print(f"{log_line_prefix}: Skipping payout as it already exists")
            # but we still want to try creating the cash record, as this has it's own
            # duplicate checking
        self._process_cash(cash_account_id, log_line_prefix, data_row)
    
    def _process_cash(self, cash_account_id, log_line_prefix, data_row):
        is_transfer = data_row.get("transaction_type") == "OPENING_BALANCE" or data_row.get("transaction_type") == "CANCEL"
        if (is_transfer):
            print(f"{log_line_prefix}: Skipping cash transaction as it is a transfer")
            return
        api_request_data = {
            "date_time": data_row.get("transaction_date"),
            "description": data_row.get("description"),
            "amount": data_row.get("amount_in_gbp"),
            "type_name": data_row.get("transaction_type"),
            "foreign_identifier": data_row.get("unique_identifier"),
        }
        response = self._api_client.try_create_cash_transaction(cash_account_id, api_request_data)
        self._print_response_status(log_line_prefix, api_request_data, response)

    def _get_errors(self, response):
        try:
            response_json = response.json()
            if not (response.status_code == 200):
                errors = response_json.get('errors')
                if not errors:
                    errors = [ response_json.get('error') ]
            else:
                errors = []
        except json.decoder.JSONDecodeError as e:
            response_json = { "error": f"Error decoding JSON response: {e}, {response.text}" }
            errors = []
        return errors, response_json
    def _print_response_status(self, log_line_prefix, api_request_data, response):
        errors, response_json = self._get_errors(response)
        if errors and len(errors) > 0:
            is_duplicate_tx = 'unique_identifier' in errors and errors['unique_identifier'][0] == "A trade with this unique_identifier already exists in the portfolio."
            is_duplicate_cash = 'foreign_identifier' in errors and errors['foreign_identifier'][0] == "has already been taken"
            if not is_duplicate_tx and not is_duplicate_cash:
                print(f"{log_line_prefix}: {response.status_code} {response_json} {api_request_data}")
            else:
                print(f"{log_line_prefix}: {response.status_code} Skipped (duplicate): {response_json} {api_request_data}")
            return errors
        else:
            print(f"{log_line_prefix}: {response.status_code} Success {response.url}")
            return []
