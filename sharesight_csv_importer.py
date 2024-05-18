import csv
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
    
    TRANSACTION_TYPE_TO_API_ENDPOINT = {
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

    def get_portfolio_holdings_lookup_key(self, portfolio_id, symbol, market):
        return f"{portfolio_id}-{market}-{symbol}"
    
    def import_file(self, file_path: TextIO, portfolio_name: str, country_code: str, use_seperate_income_account: bool, use_usd_eur_account: bool, delete_existing: bool):
        portfolio_id, cash_accounts = self._get_or_create_portfolio(portfolio_name, country_code, use_seperate_income_account, use_usd_eur_account, delete_existing)
        
        portfolio_holdings = self._api_client.get_portfolio_holdings(portfolio_id)['holdings']
        portfolio_holdings_lookup = {self.get_portfolio_holdings_lookup_key(portfolio_id, h['instrument']['code'], h['instrument']['market_code']): h['id'] for h in portfolio_holdings}

        with open(file_path, mode='r', encoding='utf-8-sig') as file:
            reader = csv.DictReader(file)
            print(f"Found columns in CSV: {reader.fieldnames}")
            for data_row in reader:
                log_line_prefix = f"Line {reader.line_num} ({data_row['transaction_type']})"
                api_endpoint_type = self.TRANSACTION_TYPE_TO_API_ENDPOINT.get(data_row.get('transaction_type'))
                holding_id_lookup_key = self.get_portfolio_holdings_lookup_key(portfolio_id, data_row.get("symbol"), data_row.get("market"))
                match api_endpoint_type:
                    case 'trade':
                        holding_id = self._process_trade(portfolio_id, country_code, cash_accounts, log_line_prefix, data_row)
                        if(holding_id):
                            portfolio_holdings_lookup[holding_id_lookup_key] = holding_id
                        else:
                            existing_holding_id = portfolio_holdings_lookup.get(holding_id_lookup_key)
                            if (existing_holding_id == None):
                                print(f"{log_line_prefix}: Missing holding id for {holding_id_lookup_key}")
                    case 'payout':
                        # cannot rely on using symbol/market directly, as this doesn't work for custom instruments
                        existing_holding_id = portfolio_holdings_lookup.get(holding_id_lookup_key)
                        if (existing_holding_id == None):
                            print(f'{log_line_prefix}: Unable to find holding id matching {data_row.get("symbol")}, {data_row.get("market")}, skipping payout')
                        elif (not delete_existing):
                            print(f"{log_line_prefix}: Skipping for now, as we cannot prevent duplicates when not a fresh portfolio")
                        else:
                            cash_account_id = cash_accounts[data_row.get("cash_account") or ("INCOME" if country_code == 'AU' else None)]
                            self._process_payout(portfolio_id, country_code, cash_account_id, log_line_prefix, data_row, existing_holding_id)
                    case 'merge':
                        existing_holding_id = portfolio_holdings_lookup.get(holding_id_lookup_key)
                        next_data_row = reader.__next__()
                        self._process_merge(portfolio_id, existing_holding_id, log_line_prefix, next_data_row)
                    case 'cash':
                        cash_account_id = cash_accounts[data_row.get("cash_account")]
                        self._process_cash(cash_account_id, log_line_prefix, data_row)
                    case _:
                        print(f"{log_line_prefix}: Unable to map {data_row.get('transaction_type')} to an API endpoint")
                        return None
        print(f"Syncing cash accounts")
        for cash_account in set(cash_accounts.values()):
            self._api_client.resync_cash_account(cash_account)

    def _get_portfolio_by_name(self, portfolio_name: str):
        portfolios = self._api_client.get_portfolios().get('portfolios', [])
        portfolio = next((item for item in portfolios if item["name"] == portfolio_name), None)
        if portfolio:
            portfolio_id = portfolio['id']
            cash_accounts = self._api_client.get_cash_accounts(portfolio_id).get('cash_accounts', [])
            print(f"{cash_accounts}")
            return portfolio_id, {
                "CAPITAL": portfolio['trade_sync_cash_account_id'], 
                "INCOME": portfolio['payout_sync_cash_account_id'],
                "GBP": next((item for item in cash_accounts if item["currency"] == "GBP"), {}).get('id'),
                "USD": next((item for item in cash_accounts if item["currency"] == "EUR"), {}).get('id'),
                "EUR": next((item for item in cash_accounts if item["currency"] == "EUR"), {}).get('id'),
            }
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
        capital_cash_account_id = self._api_client.create_cash_account(portfolio_id, {"name": f"{portfolio_name} Capital Account", "currency": "GBP"}).get('cash_account').get('id')
        print(f"Created cash account {capital_cash_account_id}")
        if (use_seperate_income_account):
            income_cash_account_id = self._api_client.create_cash_account(portfolio_id, {"name": f"{portfolio_name} Income Account", "currency": "GBP"}).get('cash_account').get('id')
            print(f"Created income cash account {income_cash_account_id}")
        else:
            income_cash_account_id = capital_cash_account_id
        # this was commented out, can't remember why?
        self._api_client.update_portfolio(portfolio_id, {"trade_sync_cash_account_id": capital_cash_account_id, "payout_sync_cash_account_id": income_cash_account_id })
        
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
        if (portfolio_id and delete_existing):
            print(f"Removing portfolio {portfolio_id}")
            self._api_client.delete_portfolio(portfolio_id)
            portfolio_id, cash_accounts = None, {}
        if (delete_existing):
            self._api_client.try_delete_custom_instruments(self.CUSTOM_INSTRUMENT_SUFFIX)
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
        if (errors and 'symbol' in errors and errors['symbol'][0] == "^Can't find instrument for this market and share code"):
            errors = self._create_missing_instrument(log_line_prefix, data_row)
            if (errors == []):
                response = self._api_client.try_create_holding_merge(portfolio_id, merge_data)
                self._print_response_status(log_line_prefix, merge_data, response)
        else:
            self._print_response_status(log_line_prefix, merge_data, response)

    def _create_missing_instrument(self, log_line_prefix, data_row):
        # create custom instrument
        custom_investment_data = {
            "code": data_row.get("symbol"),
            "name": data_row.get("symbol_name") + f" {self.CUSTOM_INSTRUMENT_SUFFIX}",
            "country_code": "LU" if data_row.get("brokerage_currency_code") == "EUR" else "GB" if not data_row.get("brokerage_currency_code") else data_row.get("brokerage_currency_code")[:2],
            "investment_type": "MANAGED_FUND" #  ORDINARY, WARRANT, SHAREFUND, PROPFUND, PREFERENCE, STAPLEDSEC, OPTIONS, RIGHTS, MANAGED_FUND, FIXED_INTEREST, PIE
        }
        print(f"{log_line_prefix}: Creating custom instrument {custom_investment_data}")
        response = self._api_client.try_create_custom_investment(custom_investment_data)
        errors = self._print_response_status(log_line_prefix, custom_investment_data, response)
        return errors

    def _process_trade(self, portfolio_id, country_code, cash_accounts, log_line_prefix, data_row):
        is_capital_call_or_return = data_row.get("transaction_type") == "CAPITAL_CALL" or data_row.get("transaction_type") == "CAPITAL_RETURN"
        api_request_data = {
            "unique_identifier": data_row.get("unique_identifier"),
            "transaction_type": data_row.get("transaction_type"),
            "transaction_date": data_row.get("transaction_date"),
            "portfolio_id": portfolio_id,
            "symbol": data_row.get("symbol"),
            "market": data_row.get("market"),
            "quantity": data_row.get("quantity"),
            "price": data_row.get("price"),
            "goes_ex_on": data_row.get("goes_ex_on"),
            "brokerage": data_row.get("brokerage"),
            "brokerage_currency_code": data_row.get("brokerage_currency_code"),
            "exchange_rate": data_row.get("exchange_rate_gbp") if country_code == "GB" and data_row.get("exchange_rate_gbp") else data_row.get("exchange_rate_aud") if country_code=="AU" and data_row.get("exchange_rate_aud") else "1",
            "cost_base": data_row.get("amount") if data_row.get("transaction_type") == "OPENING_BALANCE" else "",
            "capital_return_value": str(abs(float(data_row.get("amount")))) if is_capital_call_or_return else "",
            "paid_on": data_row.get("transaction_date") if is_capital_call_or_return else "",
            "comments": data_row.get("comments")
        }
        response = self._api_client.try_create_trade(api_request_data)
        errors,response_json  = self._get_errors(response)
        if (response.status_code != 200 and data_row.get("transaction_type") == "OPENING_BALANCE"):
            # errors = response_json.get('errors')
            # is_duplicate_tx = errors and 'unique_identifier' in errors and errors['unique_identifier'][0] == "A tr
            print(f"{log_line_prefix}: Falling back to BUY transaction type, this will need modifying in the UI")
            api_request_data['transaction_type'] = "BUY"
            response = self._api_client.try_create_trade(api_request_data)
            self._print_response_status(log_line_prefix, api_request_data, response)
        elif (errors and 'instrument_code' in errors and errors['instrument_code'][0] == "^Instrument does not exist"):
            errors = self._create_missing_instrument(log_line_prefix, data_row)
            if (errors == []):
                response = self._api_client.try_create_trade(api_request_data)
                self._print_response_status(log_line_prefix, api_request_data, response)
        else:
            self._print_response_status(log_line_prefix, api_request_data, response)

        response_data = response.json().get('trade')
        holding_id = response_data.get('holding_id') if response_data else None
        # create payout too as sharesight doesn't duplicate these into the cash account
        if is_capital_call_or_return or country_code == "AU" or len(cash_accounts) > 2:
            cash_account_id = cash_accounts[data_row.get("cash_account") or (cash_accounts["INCOME"] if is_capital_call_or_return else "CAPITAL")]
            self._process_cash(cash_account_id, log_line_prefix, data_row)
        return holding_id

    def _process_payout(self, portfolio_id, country_code, cash_account_id, log_line_prefix, data_row, existing_holding_id):
        api_request_data = {
            "portfolio_id": portfolio_id,
            "holding_id": existing_holding_id,
            "paid_on": data_row.get("transaction_date"),
            "amount": data_row.get("amount"),
            "goes_ex_on": data_row.get("goes_ex_on"),
            "currency_code": data_row.get("currency_code"),
            "exchange_rate": data_row.get("exchange_rate_gbp") if country_code == "GB" and data_row.get("exchange_rate_gbp") else data_row.get("exchange_rate_aud") if country_code=="AU" and data_row.get("exchange_rate_aud") else "1",
        }
        response = self._api_client.try_create_payout(api_request_data)
        self._print_response_status(log_line_prefix, api_request_data, response)
        # when currency of cash account doesn't match the portfolio, we need to record in the cash account too
        if cash_account_id != None:
            self._process_cash(cash_account_id, log_line_prefix, data_row)
    
    def _process_cash(self, cash_account_id, log_line_prefix, data_row):
        api_request_data = {
            "date_time": data_row.get("transaction_date"),
            "description": data_row.get("description"),
            "amount": data_row.get("amount"),
            "type_name": data_row.get("transaction_type"),
            "foreign_identifier": data_row.get("unique_identifier"),
        }
        response = self._api_client.try_create_cash_transaction(cash_account_id, api_request_data)
        self._print_response_status(log_line_prefix, api_request_data, response)

    def _get_errors(self, response):
        if not (response.status_code == 200):
            try:
                response_json = response.json()
                errors = response_json.get('errors')
                if not errors:
                    errors = [ response_json.get('error') ]
            except json.decoder.JSONDecodeError as e:
                response_json = { "error": f"Error decoding JSON response: {e}" }
                errors = []
            return errors, response_json
        return None, None
    def _print_response_status(self, log_line_prefix, api_request_data, response):
        errors, response_json = self._get_errors(response)
        if errors:
            is_duplicate_tx = errors and 'unique_identifier' in errors and errors['unique_identifier'][0] == "A trade with this unique_identifier already exists in the portfolio."
            is_duplicate_cash = errors and 'foreign_identifier' in errors and errors['foreign_identifier'][0] == "has already been taken"
            if not is_duplicate_tx and not is_duplicate_cash:
                print(f"{log_line_prefix}: {response.status_code} {response_json} {api_request_data}")
            else:
                print(f"{log_line_prefix}: {response.status_code} Skipped (duplicate): {response_json}")
            return errors
        else:
            print(f"{log_line_prefix}: {response.status_code} Success {response_json}")
            return []
