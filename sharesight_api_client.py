import time
import curlify
import requests

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
        if (response.status_code == 504 or response.status_code == 502):
            # gateway timeout, wait and then retry
            print(f"Gateway timeout, waiting and retrying: {url}")
            time.sleep(5)
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
    
    def get_cash_accounts(self, portfolio_id):
        return self._make_request('get', 
            f"{self.API_V2_BASE_URL}portfolios/{portfolio_id}/cash_accounts.json"
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

    def get_payouts(self, portfolio_id):
        return self._make_request('get', 
            f"{self.API_V2_BASE_URL}portfolios/{portfolio_id}/payouts.json"
        ).json()
    
    def delete_all_trades(self, portfolio_id):
        trades = self._make_request('get', 
            f"{self.API_V2_BASE_URL}portfolios/{portfolio_id}/trades.json"
        ).json()
        for trade in trades.get('trades', []):
            self._make_request('delete', 
                f"{self.API_V2_BASE_URL}trades/{trade.get('id')}"
            )
    
    def delete_all_cash_account_transactions_in_portfolio(self, portfolio_id):
        cash_accounts = self.get_cash_accounts(portfolio_id)
        for cash_account in cash_accounts.get('cash_accounts', []):
            self.delete_all_cash_account_transactions(cash_account.get('id'))

    def delete_all_cash_account_transactions(self, cash_account_id):
        transactions = self._make_request('get', 
            f"{self.API_V2_BASE_URL}cash_accounts/{cash_account_id}/cash_account_transactions.json"
        ).json()
        for transaction in transactions.get('cash_account_transactions', []):
            self._make_request('delete', 
                f"{self.API_V2_BASE_URL}cash_account_transactions/{transaction.get('id')}"
            )
    
    def get_custom_investments(self, portfolio_id):
        return self._make_request('get', 
            f"{self.API_V3_BASE_URL}custom_investments?portfolio_id={portfolio_id}"
        ).json()
    
    def try_create_custom_investment(self, instrument_data):
        return self._make_request_without_status_check('post', 
            f'{self.API_V3_BASE_URL}custom_investments', 
            json=instrument_data
        )
    
    def try_create_holding_merge(self, portfolio_id, merge_data):
        return self._make_request_without_status_check('post', 
            f'{self.API_V2_BASE_URL}portfolios/{portfolio_id}/holding_merges.json', 
            json=merge_data
        )

    def try_delete_custom_instruments(self, portfolio_id, suffix):
        custom_investments = self._make_request('get', 
            f'{self.API_V3_BASE_URL}custom_investments?portfolio_id={portfolio_id}')
        for custom_investment in custom_investments.json().get('custom_investments', []):
            # if name ends with suffix then delete
            if custom_investment['name'].endswith(suffix):
                self._make_request('delete', 
                    f"{self.API_V3_BASE_URL}custom_investments/{custom_investment['id']}"
                )
    
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
