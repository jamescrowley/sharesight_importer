import csv
import datetime
from itertools import chain
import json
import sys
from typing import TextIO
from sharesight_api_client import SharesightApiClient

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
        "INTEREST_CHARGED": "cash",
        "FEE": "cash",
        "FEE_REIMBURSEMENT": "cash"
    }
    # cancel is not strictly a non-cash transaction, we are just using it for transfer
    # of holdings across portfolios
    NON_CASH_TX_TYPES = ["OPENING_BALANCE", "CANCEL", "MERGE_BUY", "MERGE_CANCEL", "CONSOLD", "BONUS", "SPLIT"]
    CUSTOM_INSTRUMENT_SUFFIX = "(AUTO)"

    def __init__(self, api_client: SharesightApiClient):
        self._api_client = api_client

    def get_portfolio_holdings_lookup_key(self, portfolio_id: str, symbol: str, market: str):
        return f"{portfolio_id}-{market}-{symbol}".lower()

    def get_portfolio_payouts_lookup_key(self, portfolio_id: str, holding_id: str, paid_on: str):
        return f"{portfolio_id}-{holding_id}-{paid_on}".lower()

    def _remove_portfolio_qualifier_from_symbol(self, symbol: str, portfolio_id: str):
        return symbol[:-len(f"-{portfolio_id}")] if symbol.endswith(f"-{portfolio_id}") else symbol

    def _get_symbol_key_with_portfolio_qualifier_for_custom_instruments(self, data_row, portfolio_id: str):
        return data_row.get("symbol") + f"-{portfolio_id}" if data_row.get('market','').lower()=='other' else data_row.get("symbol")

    def _get_unique_cash_accounts_in_file(self, file_path: TextIO):
        with open(file_path, mode='r', encoding='utf-8-sig') as file:
            reader = csv.DictReader(file)
            return set( (data_row.get("amount_currency"),data_row.get("cash_account") or "") for data_row in reader)

    def _get_unique_custom_instruments_in_file(self, file_path: TextIO, portfolio_id: str) -> list[dict]:
        with open(file_path, mode='r', encoding='utf-8-sig') as file:
            reader = csv.DictReader(file)
            return [
                {
                    "symbol": symbol,
                    "symbol_name": symbol_name,
                    "instrument_country_code": country_code,
                    # this is ignored currently, it just uses the country code's currency 
                    "instrument_currency": instrument_currency,
                    "symbol_type": symbol_type
                }
                for symbol, symbol_name, country_code, instrument_currency, symbol_type in {
                    (
                        self._get_symbol_key_with_portfolio_qualifier_for_custom_instruments(data_row, portfolio_id),
                        data_row.get("symbol_name"),
                        data_row.get("instrument_country_code"),
                        data_row.get("instrument_currency"),
                        data_row.get("symbol_type")
                    )
                    for data_row in reader
                    if data_row.get("market", "").lower() == "other"
                }
            ]

    def _generate_opening_balances_rows(self, portfolio_id: str, portfolio_currency_code, valuation_date: datetime.date):
        valuation = self._api_client.get_valuation_on(portfolio_id, valuation_date)
        exchange_rates = self._api_client.get_internal_exchange_rates(valuation_date).get("exchange_rates")
        print(f"Exchange rates: {exchange_rates}")
        gbp_to_aud = exchange_rates.get("GBP/AUD").get("rate")
        aud_to_gbp = exchange_rates.get("AUD/GBP").get("rate")
        for holding in valuation.get("holdings"):
            yield {
                "unique_identifier": f"GENERATED-{holding.get('symbol')}",
                "transaction_type": "OPENING_BALANCE",
                "transaction_date": valuation.get("balance_date"),
                "symbol": self._remove_portfolio_qualifier_from_symbol(holding.get("symbol"), portfolio_id),
                "market": holding.get("market"),
                "quantity": holding.get("quantity"),
                "amount": holding.get("value"),
                "amount_currency": portfolio_currency_code,
                "amount_in_gbp": holding.get("value") if portfolio_currency_code == "GBP" else holding.get("value") * aud_to_gbp,
                "amount_in_aud": holding.get("value") if portfolio_currency_code == "AUD" else holding.get("value") * gbp_to_aud,
                "description": "Opening Balance"
            }
        # cash account valuations are not reliable from the valuation api endpoint, as they
        # convert from the account currency to the portfolio currency, and back again
        # so instead, load all the transactions, and total the balance
        for cash_account in valuation.get("cash_accounts"):
            transactions = self._api_client.get_cash_account_transactions(cash_account.get('cash_account_id'), "2000-01-01", valuation.get("balance_date")).get("cash_account_transactions")
            total_amount = sum(float(t.get("amount")) for t in transactions)
            last_balance = transactions[0].get("balance")
            # confusingly value is in the portfolio_currency_code
            # not the account currency
            currency_pair = f"{portfolio_currency_code}/{cash_account.get('currency_code')}"
            portfolio_to_cash_account_exchange_rate = exchange_rates.get(currency_pair).get("rate") if portfolio_currency_code != cash_account.get('currency_code') else 1
            cash_account_name = self._get_cash_account_name_from_sharesight_cash_account(cash_account.get('name'), cash_account.get('currency_code'))
            calculated_total_amount = cash_account.get("value") * portfolio_to_cash_account_exchange_rate
            if (round(calculated_total_amount,2) != round(total_amount,2)):
                print(f"WARN Calculated total amount {calculated_total_amount} does not match total amount {total_amount} for {cash_account_name}. Sharesight valuation reports will show the calculated total, which does not match the balances shown in the cash account itself.")
            if (round(total_amount,2) != round(last_balance,2)):
                print(f"ERROR Total amount {total_amount} does not match last balance {last_balance} for {cash_account_name}. This should not happen!", file=sys.stderr)
            yield {
                "unique_identifier": f"GENERATED-{cash_account.get('currency_code')}-{cash_account_name}",
                "transaction_type": "DEPOSIT",
                "transaction_date": valuation.get("balance_date"),
                "amount": total_amount,
                "amount_currency": cash_account.get("currency_code"),
                "cash_account": cash_account_name,
                "description": "Opening Balance"
            }
    
    def import_file(self, file_path: TextIO, portfolio_name: str, country_code: str, delete_existing: bool, min_date: datetime.date, opening_balance_on: datetime.date | None, opening_balance_from: str | None, min_line: int, max_line: int, prices_file_path: TextIO):
        opening_balances = []
        if opening_balance_on and opening_balance_from:
            portfolio_id,_,portfolio_currency_code = self._get_portfolio_by_name(opening_balance_from)
            print(f"Generating opening balances on {opening_balance_on} from {opening_balance_from}")
            opening_balances = list(self._generate_opening_balances_rows(portfolio_id, portfolio_currency_code, opening_balance_on))
            # TODO: if custom instrument prices are not synced between the two portfolios, the opening balances will be incorrect
            # not sure how we check this yet
            print('    ' + '\n    '.join(f"{p}" for p in opening_balances))
            min_date = opening_balance_on
            
        self._process_transactions(file_path, portfolio_name, country_code, delete_existing, min_date, min_line, max_line, opening_balances, prices_file_path)
        
    def _process_transactions(self, file_path: TextIO, portfolio_name: str, country_code: str, delete_existing: bool, min_date: datetime.date, min_line: int, max_line: int, injected_opening_balances: list[dict], prices_file_path: TextIO):
        cash_accounts_in_file = self._get_unique_cash_accounts_in_file(file_path)
        portfolio_id, cash_accounts = self._get_or_create_portfolio(portfolio_name, country_code, cash_accounts_in_file, delete_existing)

        custom_instruments_in_file = self._get_unique_custom_instruments_in_file(file_path, portfolio_id)
        print(f"Found {len(custom_instruments_in_file)} custom instruments")
        print('    ' + '\n    '.join(f"{p}" for p in custom_instruments_in_file))
        print(f"Creating custom instruments")
        self._create_custom_instruments(portfolio_id, custom_instruments_in_file)

        if (prices_file_path):
            self._process_prices(prices_file_path, portfolio_id, country_code)
        
        # payouts don't have a unique id, so we have to fetch them and de-duplicate ourselves
        portfolio_payouts = self._api_client.get_payouts(portfolio_id).get('payouts')
        portfolio_payouts_lookup = {self.get_portfolio_payouts_lookup_key(portfolio_id, p['holding_id'], p['paid_on']): p['id'] for p in portfolio_payouts}
        portfolio_holdings = self._api_client.get_portfolio_holdings(portfolio_id)['holdings']
        portfolio_holdings_lookup = {self.get_portfolio_holdings_lookup_key(portfolio_id, h['instrument']['code'], h['instrument']['market_code']): h['id'] for h in portfolio_holdings}

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
                filtered_reader = filter(lambda row: datetime.datetime.strptime(row['transaction_date'], "%Y-%m-%d").date() >= min_date, filtered_reader)
            if (injected_opening_balances):
                filtered_reader = chain(injected_opening_balances, filtered_reader)
            
            for data_row in filtered_reader:
                log_line_prefix = f"Line {reader.line_num}\t{data_row['unique_identifier']}\t{data_row['transaction_type']}"
                api_endpoint_type = self.TRANSACTION_TYPE_TO_API_ENDPOINT.get(data_row.get('transaction_type'))
                data_row.update({"symbol": self._get_symbol_key_with_portfolio_qualifier_for_custom_instruments(data_row, portfolio_id)})
                holding_id_lookup_key = self.get_portfolio_holdings_lookup_key(portfolio_id, data_row.get("symbol"), data_row.get("market"))
                cash_account_id = cash_accounts[self._get_cash_account_lookup_key(data_row.get("amount_currency"), data_row.get("cash_account"))]

                match api_endpoint_type:
                    case 'trade':
                        holding_id = self._process_trade(portfolio_id, country_code, cash_account_id, log_line_prefix, data_row, portfolio_payouts_lookup)
                        if(holding_id):
                            print(f"{log_line_prefix}\tSaved holding id {holding_id} in {holding_id_lookup_key}")
                            portfolio_holdings_lookup[holding_id_lookup_key] = holding_id
                        else:
                            print(f"{log_line_prefix}\tLooking up holding id for {holding_id_lookup_key}")
                            existing_holding_id = portfolio_holdings_lookup.get(holding_id_lookup_key)
                            if (existing_holding_id == None):
                                print(f"{log_line_prefix}\tMissing holding id for {holding_id_lookup_key}")
                    case 'payout':
                        # cannot rely on using symbol/market directly, as this doesn't work for custom instruments
                        existing_holding_id = portfolio_holdings_lookup.get(holding_id_lookup_key)
                        if (existing_holding_id == None):
                            print(f'{log_line_prefix}\tERROR Unable to find holding id matching {holding_id_lookup_key} - {data_row.get("symbol")}, {data_row.get("market")}, skipping payout', file=sys.stderr)
                        else:
                            self._process_payout(portfolio_id, country_code, cash_account_id, log_line_prefix, data_row, existing_holding_id, portfolio_payouts_lookup)
                    case 'merge':
                        existing_holding_id = portfolio_holdings_lookup.get(holding_id_lookup_key)
                        print(f"{log_line_prefix}\texisting_holding_id: {existing_holding_id}")
                        next_data_row = reader.__next__()
                        next_data_row.update({"symbol": self._get_symbol_key_with_portfolio_qualifier_for_custom_instruments(next_data_row, portfolio_id)})
                        if (next_data_row.get("transaction_type") != "MERGE_BUY"):
                            print(f"{log_line_prefix}\tERROR Expected MERGE_BUY but got {next_data_row.get('transaction_type')}", file=sys.stderr)
                            return None
                        self._process_merge(portfolio_id, existing_holding_id, log_line_prefix, next_data_row)
                    case 'cash':
                        self._process_cash(cash_account_id, log_line_prefix, data_row)
                    case _:
                        print(f"{log_line_prefix}\tERROR Unable to map {data_row.get('transaction_type')} to an API endpoint", file=sys.stderr)
                        return None
        print(f"Syncing cash accounts")
        for cash_account in set(cash_accounts.values()):
            if cash_account:
                self._api_client.resync_cash_account(cash_account)

    def _process_prices(self, prices_file_path: TextIO, portfolio_id: str, country_code: str):
        print(f"Syncing custom instruments prices")
        portfolio_custom_investments = self._api_client.get_custom_investments(portfolio_id)['custom_investments']
        print(f"portfolio_custom_investments: {portfolio_custom_investments}")
        portfolio_custom_investments_lookup = {self._remove_portfolio_qualifier_from_symbol(c['code'], portfolio_id): c['id'] for c in portfolio_custom_investments}
        with open(prices_file_path, mode='r', encoding='utf-8-sig') as file:
            reader = csv.DictReader(file)
            for data_row in reader:
                print(f"Syncing custom instrument price for {data_row['symbol']}")
                custom_investment_id = portfolio_custom_investments_lookup.get(data_row['symbol'])
                if custom_investment_id:
                    api_request_data = {
                        "last_traded_price": data_row['price'],
                        "last_traded_on": data_row['date']
                    }
                    existing_prices = self._api_client.get_custom_investment_prices(custom_investment_id, data_row['date'], data_row['date'])['prices']
                    if existing_prices:
                        print(f"Found existing price for {data_row['symbol']} on {data_row['date']}: {existing_prices}")
                        response = self._api_client.put_custom_investment_price(existing_prices[0]['id'], api_request_data)
                    else:
                        response = self._api_client.create_custom_investment_price(custom_investment_id, api_request_data)
                    self._print_response_status(reader.line_num, api_request_data, response)

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

    def _create_portfolio_and_cash_accounts(self, portfolio_name: str, country_code: str, cash_accounts_in_file: list[(str,str)]):
        print("Creating portfolio")
        portfolio_data = {
                "name": portfolio_name,
                "country_code": country_code,
                "disable_automatic_transactions": True,
                "broker_email_api_enabled": False
            }
        print(f"Creating portfolio {portfolio_data}")
        portfolio_id = self._api_client.create_portfolio(portfolio_data).get('id')
        cash_accounts = {}

        for (cash_account_currency,cash_account_name) in cash_accounts_in_file:
            cash_account_full_name = f"{cash_account_name or 'Account'} ({cash_account_currency})"
            cash_account_id = self._api_client.create_cash_account(portfolio_id, {"name": cash_account_full_name, "currency": cash_account_currency}).get('cash_account').get('id')
            cash_accounts[self._get_cash_account_lookup_key(cash_account_currency, cash_account_name)] = cash_account_id
            print(f"Created cash account {cash_account_full_name} with id {cash_account_id}")
        
        return portfolio_id,cash_accounts

    def _get_or_create_portfolio(self, portfolio_name, country_code, cash_accounts_in_file, delete_existing):
        portfolio_id,cash_accounts,_ = self._get_portfolio_by_name(portfolio_name)
        if (delete_existing):
            if (portfolio_id):
                print(f"Removing existing portfolio {portfolio_id}")
                self._api_client.delete_portfolio(portfolio_id)
                # print(f"Removing existing trades and cash account transactions")
                # self._api_client.delete_all_cash_account_transactions_in_portfolio(portfolio_id)
                # self._api_client.delete_all_trades(portfolio_id)
                portfolio_id, cash_accounts = None, {}
        if (portfolio_id == None):
            portfolio_id, cash_accounts = self._create_portfolio_and_cash_accounts(portfolio_name, country_code, cash_accounts_in_file)
        return portfolio_id,cash_accounts
    
    def _process_merge(self, portfolio_id, existing_holding_id, log_line_prefix, data_row):
        merge_data = {
            "holding_id": existing_holding_id,
            "merge_date": data_row.get("goes_ex_on") if data_row.get("goes_ex_on") else data_row.get("transaction_date"),
            "quantity": float(data_row.get("quantity")),
            "symbol": data_row.get("symbol"),
            "market": data_row.get("market").upper(),
            "comments": "none"
        }
        response = self._api_client.try_create_holding_merge(portfolio_id, merge_data)
        self._print_response_status(log_line_prefix, merge_data, response)

    def _create_custom_instruments(self, portfolio_id, custom_instruments_in_file):
        existing_custom_instruments = self._api_client.get_custom_investments(portfolio_id).get('custom_investments', [])
        existing_custom_instruments_lookup = {c['code']: c['id'] for c in existing_custom_instruments}
        for data_row in custom_instruments_in_file:
            existing_custom_instrument_id = existing_custom_instruments_lookup.get(data_row.get("symbol"))
            if (existing_custom_instrument_id):
                print(f"Skipping existing custom instrument {data_row.get('symbol')}")
            else:
                self._create_custom_instrument("", portfolio_id, data_row)

    def _create_custom_instrument(self, log_line_prefix, portfolio_id, data_row):
        custom_investment_data = {
            "portfolio_id": portfolio_id,
            "code": data_row.get("symbol"),
            "name": data_row.get("symbol_name") + f" {self.CUSTOM_INSTRUMENT_SUFFIX}",
            "country_code": data_row.get("instrument_country_code"),
            "currency_code": data_row.get("instrument_currency"),
            "investment_type": data_row.get("symbol_type") if data_row.get("symbol_type") else "MANAGED_FUND" #  ORDINARY, WARRANT, SHAREFUND, PROPFUND, PREFERENCE, STAPLEDSEC, OPTIONS, RIGHTS, MANAGED_FUND, FIXED_INTEREST, PIE
        }
        print(f"Creating custom instrument {custom_investment_data['code']}")
        response = self._api_client.try_create_custom_investment(custom_investment_data)
        errors, response_json = self._get_errors(response)
        if (response_json.get("currency_code") != data_row.get("instrument_currency")):
            print(f"{log_line_prefix}\tWARN Sharesight has set {custom_investment_data['code']} currency code to {response_json.get('currency_code')} based on domicile, but instrument currency is set to {data_row.get('instrument_currency')}")
        self._print_response_status(log_line_prefix, custom_investment_data, response)
        return errors

    def _process_trade(self, portfolio_id, country_code, cash_account_id, log_line_prefix, data_row, portfolio_payouts_lookup):
        is_capital_call_or_return = data_row.get("transaction_type") == "CAPITAL_CALL" or data_row.get("transaction_type") == "CAPITAL_RETURN"
        if (float(data_row.get("quantity")) < 0):
            print(f"{log_line_prefix}\tWARN Shorts are not supported by Sharesight. Quantity is negative: {data_row.get('quantity')}")
        api_request_data = {
            "unique_identifier": data_row.get("unique_identifier"),
            "transaction_type": data_row.get("transaction_type"),
            "transaction_date": data_row.get("transaction_date"),
            "portfolio_id": portfolio_id,
            "symbol": data_row.get("symbol"),
            "market": data_row.get("market"),
            # NB: shorts are not supported
            "quantity": data_row.get("quantity"),
            "price": data_row.get("price"), # in instrument currency
            "goes_ex_on": data_row.get("goes_ex_on"),
            # has to be in portfolio currency or instrument currency
            "brokerage": data_row.get("brokerage") if data_row.get("brokerage") else data_row.get("brokerage_in_gbp") if country_code == "GB" else data_row.get("brokerage_in_aud") if country_code == "AU" else "??",
            "brokerage_currency": data_row.get("brokerage_currenc") if data_row.get("brokerage_currency") else "GBP" if country_code == "GB" else "AUD" if country_code == "AU" else "??",
            "exchange_rate": data_row.get("exchange_rate_gbp") if country_code == "GB" else data_row.get("exchange_rate_aud") if country_code=="AU" else "??",
            # needs to be in portfolio currency
            "cost_base": (data_row.get("amount_in_gbp") if country_code == "GB" else data_row.get("amount_in_aud") if country_code == "AU" else "??") if data_row.get("transaction_type") == "OPENING_BALANCE" else "",
            "capital_return_value": str(abs(float(data_row.get("amount_in_gbp") if country_code == "GB" else data_row.get("amount_in_aud") if country_code == "AU" else "??"))) if is_capital_call_or_return else "",
            "paid_on": data_row.get("transaction_date") if is_capital_call_or_return else "",
            "comments": data_row.get("description")
        }
        response = self._api_client.try_create_trade(api_request_data)
        errors,response_json  = self._get_errors(response)
        # workaround to "We do not have a price on 18 Sep 2019" error
        # if (response.status_code != 200 and data_row.get("transaction_type") == "OPENING_BALANCE" and errors and 'market_price' in errors and errors['market_price'][0] == "^We do not have a price on 18 Sep 2019"):
        #     print(f"{log_line_prefix}\t{response.status_code} (no price handled) {response.url} Falling back to BUY transaction type, this will need modifying in the UI")
        #     api_request_data['transaction_type'] = "BUY"
        #     response = self._api_client.try_create_trade(api_request_data)
        #     errors,response_json = self._get_errors(response)
        self._print_response_status(log_line_prefix, api_request_data, response)
        
        response_data = response_json.get('trade')
        holding_id = response_data.get('holding_id') if response_data else None
        if (not holding_id and len(errors) == 0):
            print(f"{log_line_prefix}\t{response.status_code} Couldn't find holding id but no error - {response_json}")
        self._process_cash(cash_account_id, log_line_prefix, data_row)

        if (data_row.get("accrued_income") and (data_row.get("transaction_type") == "SELL" or data_row.get("transaction_type") == "BUY")):
            accrued_income_row = data_row.copy()
            accrued_income_row.pop("accrued_income")
            accrued_income_row.update({"amount": abs(float(data_row.get("accrued_income")))})
            accrued_income_row.update({"unique_identifier": f"{data_row.get('unique_identifier')}-accrued_income"})
            if (data_row.get("transaction_type") == "SELL"):
                # sale price will exclude accrued income, so we add back as income
                self._process_payout(portfolio_id, country_code, cash_account_id, log_line_prefix, accrued_income_row, holding_id, portfolio_payouts_lookup)
            elif (data_row.get("transaction_type") == "BUY"):
                # the bond purchase is dirty, so some of the payment is interest and some is principal
                # so the original accrued income is a capital call
                accrued_income_row.update({"transaction_type": "CAPITAL_CALL"})
                self._process_trade(portfolio_id, country_code, cash_account_id, log_line_prefix, accrued_income_row, portfolio_payouts_lookup)
        return holding_id

    def _process_payout(self, portfolio_id, country_code, cash_account_id, log_line_prefix, data_row, existing_holding_id, portfolio_payouts_lookup):
        existing_payout = portfolio_payouts_lookup.get(self.get_portfolio_payouts_lookup_key(portfolio_id, existing_holding_id, data_row.get("transaction_date")))
        if (not existing_payout):
            # this should be 
            exchange_rate = data_row.get("exchange_rate_gbp") if country_code == "GB" else data_row.get("exchange_rate_aud") if country_code=="AU" else "??"
            amount_in_portfolio_base_currency = data_row.get("amount_in_gbp") if country_code == "GB" else data_row.get("amount_in_aud") if country_code == "AU" else "??"
            api_request_data = {
                "portfolio_id": portfolio_id,
                "holding_id": existing_holding_id,
                "paid_on": data_row.get("transaction_date"),
                "amount": data_row.get("amount"),
                "goes_ex_on": data_row.get("goes_ex_on"),
                "currency_code": data_row.get("amount_currency"),
                # specifying exchange rate instead of banked_amount seems to break
                # the income reports, so we're using banked_amount instead
                "banked_amount": amount_in_portfolio_base_currency
            }

            response = self._api_client.try_create_payout(api_request_data)
            self._print_response_status(log_line_prefix, api_request_data, response)
        else:
            print(f"{log_line_prefix}\tSkipping payout as it already exists")
            # but we still want to try creating the cash record, as this has it's own
            # duplicate checking
        self._process_cash(cash_account_id, log_line_prefix, data_row)
    
    def _process_cash(self, cash_account_id, log_line_prefix, data_row):
        is_non_cash_tx = data_row.get("transaction_type") in self.NON_CASH_TX_TYPES
        if (is_non_cash_tx):
            if (float(data_row.get("amount")) != 0):
                print(f"{log_line_prefix}\tWARN Non-cash transaction with amount: {data_row.get('amount')}")
            else:
                print(f"{log_line_prefix}\tINFO Non-cash transaction with amount 0")
            return
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
        try:
            response_json = response.json()
            if not (response.status_code == 200):
                errors = response_json.get('errors')
                if not errors:
                    errors = [ response_json.get('error') ]
                if not errors:
                    errors = [ "Received unexpected response with status code " + str(response.status_code) + ": " + response.text ]
            else:
                errors = []
        except json.decoder.JSONDecodeError as e:
            response_json = { "error": f"Error decoding JSON response: {e}, {response.text}" }
            errors = []
        return errors, response_json

    def _print_response_status(self, log_line_prefix, api_request_data, response):
        errors, response_json = self._get_errors(response)
        response_url = response.url.replace("https://api.sharesight.com", "")
        if errors and len(errors) > 0:
            is_duplicate_tx = 'unique_identifier' in errors and errors['unique_identifier'][0] == "A trade with this unique_identifier already exists in the portfolio."
            is_duplicate_cash = 'foreign_identifier' in errors and errors['foreign_identifier'][0] == "has already been taken"
            if not is_duplicate_tx and not is_duplicate_cash:
                print(f"{log_line_prefix}\t{response.status_code} {response_json} {api_request_data} {response_url}", file=sys.stderr)
            else:
                print(f"{log_line_prefix}\t{response.status_code} Skipped (duplicate): {response_json} {api_request_data} {response_url}")
            return errors
        else:
            print(f"{log_line_prefix}\t{response.status_code} Success {response_url}")
            return []
