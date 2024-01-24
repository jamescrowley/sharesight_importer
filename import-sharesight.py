from os import getenv
import curlify
import requests
import csv
import argparse
from typing import TextIO

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

class SharesightApiClient:
    
    API_V2_BASE_URL = "https://api.sharesight.com/api/v2/"
    API_V3_BASE_URL = "https://api.sharesight.com/api/v3/"
    _output_curl = False
    _access_token = None

    def __init__(self, client_id: str, client_secret: str, output_curl: bool):
        self._output_curl = output_curl
        # access token is valid for 30 minutes which is sufficiently
        # long to avoid refreshing the token for our purposes
        self._access_token = self._get_access_token(client_id, client_secret)

    def _get_access_token(self, client_id, client_secret):
        redirect_uri = "urn:ietf:wg:oauth:2.0:oob"
        token_url = "https://api.sharesight.com/oauth2/token"
        payload = {
            'grant_type': 'client_credentials',
            'redirect_uri': redirect_uri,
            'client_id': client_id,
            'client_secret': client_secret
        }
        response = self._make_request('post', token_url, headers=None, json=payload).json()
        return response['access_token']
    
    def _make_request_without_status_check(self, method, url, headers=None, json=None):
        default_headers = {
            "Authorization": "Bearer " + self._access_token,
            "Content-Type": "application/json"
        } if self._access_token else {}
        response = requests.request(method, url, json=json, headers = headers or default_headers)
        if (self._output_curl):
            print(curlify.to_curl(response.request))
        return response

    def _make_request(self, method, url, headers=None, json=None):
        response = self._make_request_without_status_check(method, url, headers=headers, json=json)
        response.raise_for_status()
        return response

    def delete_portfolio(self, portfolio_id):
        return self._make_request('delete', 
            f'{self.API_V2_BASE_URL}portfolios/{portfolio_id}.json'
        )

    def update_portfolio(self, portfolio_id, data):
        return self._make_request('put', 
            f"{self.API_V2_BASE_URL}portfolios/{portfolio_id}.json", 
            json={'portfolio': data}
        ).json()

    def create_portfolio(self, data):
        return self._make_request('post', 
            f"{self.API_V2_BASE_URL}portfolios.json", 
            json={'portfolio': data}
        ).json()
    
    def get_portfolio_holdings(self, portfolio_id):
        return self._make_request('get',
            f"{self.API_V3_BASE_URL}portfolios/{portfolio_id}/holdings"
        ).json()

    def create_cash_account(self, portfolio_id, data):
        return self._make_request('post', 
            f"{self.API_V2_BASE_URL}portfolios/{portfolio_id}/cash_accounts.json", 
            json={'cash_account': data}
        ).json()

    def resync_cash_account(self, cash_account_id):
        # note - undocumented API
        return self._make_request('post', 
            f"{self.API_V2_BASE_URL}cash_accounts/{cash_account_id}/reset.json?start_date=%222010-01-01T00:00:00.000Z%22"
        )

    def get_portfolios(self):
        return self._make_request('get', 
            f"{self.API_V2_BASE_URL}portfolios.json"
        ).json()

    def get_payouts(self, portfolio_id, date):
        return self._make_request('get', 
            f"{self.API_V2_BASE_URL}portfolios/{portfolio_id}/payouts.json?start_date={date}&end_date={date}"
        ).json()

    def try_create_trade(self, trade_data):
        return self._make_request_without_status_check('post', 
            f'{self.API_V2_BASE_URL}trades.json', 
            json={"trade": trade_data}
        )

    def try_create_payout(self, payout_data):
        return self._make_request_without_status_check('post',
            f'{self.API_V2_BASE_URL}payouts.json',
            json={"payout": payout_data}
        )

    def try_create_cash_transaction(self, cash_account_id, cash_data):
        return self._make_request_without_status_check('post', 
            f'{self.API_V2_BASE_URL}cash_accounts/{cash_account_id}/cash_account_transactions.json', 
            json={"cash_account_transaction": cash_data}
        )

class SharesightCsvImport:
    
    TRANSACTION_TYPE_TO_API_ENDPOINT = {
        "DIVIDEND": "payout",
        "BUY": "trade",
        "SELL": "trade",
        "SPLIT": "trade",
        "BONUS": "trade",
        "CONSOLD": "trade",
        "CANCEL": "trade",
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
    def __init__(self, api_client: SharesightApiClient):
        self._api_client = api_client

    def get_portfolio_holdings_lookup_key(self, portfolio_id, symbol, market):
        return f"{portfolio_id}-{market}-{symbol}"
    
    def import_file(self, file_path: TextIO, portfolio_name: str, country_code: str, use_seperate_income_account: bool, delete_existing: bool):
        portfolio_id, capital_cash_account_id, income_cash_account_id = self._get_or_create_portfolio(portfolio_name, country_code, use_seperate_income_account, delete_existing)
        
        portfolio_holdings = self._api_client.get_portfolio_holdings(portfolio_id)['holdings']
        # print(portfolio_holdings)
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
                        created_holding_id = self._process_trade(portfolio_id, country_code, capital_cash_account_id, income_cash_account_id, log_line_prefix, data_row)
                        if(created_holding_id):
                            portfolio_holdings_lookup[holding_id_lookup_key] = created_holding_id
                        else:
                            print(f"Missing holding id for {holding_id_lookup_key}")
                    case 'payout':
                        # cannot rely on using symbol/market directly, as this doesn't work for custom instruments
                        existing_holding_id = portfolio_holdings_lookup.get(holding_id_lookup_key)
                        if (existing_holding_id == None):
                            print(f'{log_line_prefix}: Unable to find holding id matching {data_row.get("symbol")}, {data_row.get("market")}, skipping payout')
                        elif (not delete_existing):
                            print(f"{log_line_prefix}: Skipping for now, as we cannot prevent duplicates when not a fresh portfolio")
                        else:
                            self._process_payout(portfolio_id, country_code, income_cash_account_id, log_line_prefix, data_row, existing_holding_id)
                    case 'cash':
                        cash_account_id = income_cash_account_id if data_row.get("cash_account") == "INCOME" else capital_cash_account_id
                        self._process_cash(cash_account_id, log_line_prefix, data_row)
                    case _:
                        print(f"{log_line_prefix}: Unable to map {data_row.get('transaction_type')} to an API endpoint")
                        return None
        print(f"Syncing cash accounts")
        self._api_client.resync_cash_account(capital_cash_account_id)
        if (income_cash_account_id!=capital_cash_account_id):
            self._api_client.resync_cash_account(income_cash_account_id)

    def _get_portfolio_by_name(self, portfolio_name: str):
        portfolios = self._api_client.get_portfolios().get('portfolios', [])
        portfolio = next((item for item in portfolios if item["name"] == portfolio_name), None)
        if portfolio:
            portfolio_id = portfolio['id']
            capital_cash_account_id = portfolio['trade_sync_cash_account_id']
            income_cash_account_id = portfolio['payout_sync_cash_account_id']
            return portfolio_id, capital_cash_account_id, income_cash_account_id
        else:
            return None, None, None

    def _create_portfolio_and_cash_accounts(self, portfolio_name: str, country_code: str, use_seperate_income_account: bool):
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
       # self._api_client.update_portfolio(portfolio_id, {"trade_sync_cash_account_id": capital_cash_account_id, "payout_sync_cash_account_id": income_cash_account_id })
        return portfolio_id,capital_cash_account_id,income_cash_account_id

    def _get_or_create_portfolio(self, portfolio_name, country_code, use_seperate_income_account, delete_existing):
        portfolio_id, capital_cash_account_id, income_cash_account_id = self._get_portfolio_by_name(portfolio_name)
        if (portfolio_id and delete_existing):
            print(f"Removing portfolio {portfolio_id}")
            self._api_client.delete_portfolio(portfolio_id)
            portfolio_id, capital_cash_account_id, income_cash_account_id = None, None, None
        if (portfolio_id == None):
            portfolio_id, capital_cash_account_id, income_cash_account_id = self._create_portfolio_and_cash_accounts(portfolio_name, country_code, use_seperate_income_account)
        return portfolio_id,capital_cash_account_id,income_cash_account_id
    
    def _process_trade(self, portfolio_id, country_code, capital_cash_account_id, income_cash_account_id, log_line_prefix, data_row):
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
        self.print_response_status(log_line_prefix, api_request_data, response)
        if (response.status_code != 200 and data_row.get("transaction_type") == "OPENING_BALANCE"):
            # errors = response_json.get('errors')
            # is_duplicate_tx = errors and 'unique_identifier' in errors and errors['unique_identifier'][0] == "A tr
            print(f"{log_line_prefix}: Falling back to BUY transaction type, this will need modifying in the UI")
            api_request_data['transaction_type'] = "BUY"
            response = self._api_client.try_create_trade(api_request_data)
            self.print_response_status(log_line_prefix, api_request_data, response)
        response_data = response.json().get('trade')
        holding_id = response_data.get('holding_id') if response_data else None
        # create payout too as sharesight doesn't duplicate these into the cash account
        if is_capital_call_or_return or country_code == "AU":
            cash_account_id = income_cash_account_id if is_capital_call_or_return else capital_cash_account_id
            self._process_cash(cash_account_id, log_line_prefix, data_row)
        return holding_id

    def _process_payout(self, portfolio_id, country_code, income_cash_account_id, log_line_prefix, data_row, existing_holding_id):
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
        self.print_response_status(log_line_prefix, api_request_data, response)
        # when currency of cash account doesn't match the portfolio, we need to record in the cash account too
        if country_code == "AU":
            self._process_cash(income_cash_account_id, log_line_prefix, data_row)
    
    def _process_cash(self, cash_account_id, log_line_prefix, data_row):
        api_request_data = {
            "date_time": data_row.get("transaction_date"),
            "description": data_row.get("description"),
            "amount": data_row.get("amount"),
            "type_name": data_row.get("transaction_type"),
            "foreign_identifier": data_row.get("unique_identifier"),
        }
        response = self._api_client.try_create_cash_transaction(cash_account_id, api_request_data)
        self.print_response_status(log_line_prefix, api_request_data, response)

    def print_response_status(self, log_line_prefix, api_request_data, response):
        if not (response.status_code == 200):
            response_json = response.json()
            errors = response_json.get('errors')
            is_duplicate_tx = errors and 'unique_identifier' in errors and errors['unique_identifier'][0] == "A trade with this unique_identifier already exists in the portfolio."
            is_duplicate_cash = errors and 'foreign_identifier' in errors and errors['foreign_identifier'][0] == "has already been taken"
            if not is_duplicate_tx and not is_duplicate_cash:
                print(f"{log_line_prefix}: {response_json} {api_request_data}")
            else:
                print(f"{log_line_prefix}: Skipped (duplicate)")
        else:
            print(f"{log_line_prefix}: Success")

def main():
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('--client_id', default=getenv('SHARESIGHT_CLIENT_ID'), type=str, required=False, help=argparse.SUPPRESS)
    parser.add_argument('--client_secret', default=getenv('SHARESIGHT_CLIENT_SECRET'), type=str, required=False, help=argparse.SUPPRESS)
    parser.add_argument('-p', '--portfolio_name', type=str, required=True, help='The portfolio name')
    parser.add_argument('-f', '--file_name', type=str, required=True, help='The file name')
    parser.add_argument('-c', '--country_code', type=str, required=True, help='The file name')
    parser.add_argument('-r', '--delete_existing', type=bool, action=argparse.BooleanOptionalAction, help='Remove the portfolio')
    parser.add_argument('-t', '--use_seperate_income_account', type=bool, action=argparse.BooleanOptionalAction, help='Use a seperate cash account for income')
    parser.add_argument('-d', '--debug', type=bool, action=argparse.BooleanOptionalAction, help='Output curl requests')

    args = parser.parse_args()

    api_client = SharesightApiClient(args.client_id, args.client_secret, args.debug)
    csv_parser = SharesightCsvImport(api_client)
    csv_parser.import_file(args.file_name, args.portfolio_name, args.country_code, args.use_seperate_income_account, args.delete_existing)

main()
