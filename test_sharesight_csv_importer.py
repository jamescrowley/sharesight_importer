import unittest
from unittest.mock import MagicMock, call, patch
import datetime
import tempfile
from pathlib import Path
from sharesight_csv_importer import SharesightCsvImporter
from sharesight_api_client import ApiResult
from sharesight_import_options import ImportOptions, OpeningBalanceOptions

# Test Data Constants
PORTFOLIO_NAME = "Test Portfolio"
COUNTRY_CODE = "GB"
PORTFOLIO_ID = 123
CASH_ACC_USD_ID = 456
CASH_ACC_GBP_ID = 457
HOLDING_ID_AAPL = 789
HOLDING_ID_MSFT = 790
HOLDING_ID_CUSTOM = 791
CUSTOM_INST_ID = 999
PRICE_ID = 1001


def api_result(data=None, status_code=200, duplicate=False, endpoint="mock://create"):
    return ApiResult(
        status_code=status_code,
        data=data or {},
        errors=() if status_code == 200 else ("validation error",),
        duplicate=duplicate,
        endpoint=endpoint,
    )

class TestSharesightCsvImporter(unittest.TestCase):

    def setUp(self):
        """Set up a mock API client for each test."""
        self.mock_api_client = MagicMock(name="MockApiClient")
        # Default successful responses - override in specific tests if needed
        self.mock_api_client.create_portfolio.return_value = {'id': PORTFOLIO_ID}
        self.mock_api_client.create_cash_account.return_value = {'cash_account': {'id': CASH_ACC_USD_ID}} # Default to USD for simplicity
        self.mock_api_client.get_portfolios.return_value = {'portfolios': []} # Default: portfolio doesn't exist
        self.mock_api_client.get_cash_accounts.return_value = {'cash_accounts': []} # Default: no cash accounts
        self.mock_api_client.get_portfolio_holdings.return_value = {'holdings': []} # Default: no holdings
        self.mock_api_client.get_payouts.return_value = {'payouts': []} # Default: no payouts
        self.mock_api_client.get_custom_investments.return_value = {'custom_investments': []} # Default: no custom instruments
        self.mock_api_client.get_custom_investment_prices.return_value = {'prices': []} # Default: no existing prices

        # Mock successful creation responses (can be refined)
        self.mock_api_client.try_create_trade.return_value = api_result({'trade': {}})
        self.mock_api_client.try_create_payout.return_value = api_result({'payout': {'id': 555}})
        self.mock_api_client.try_create_cash_transaction.return_value = api_result()
        self.mock_api_client.try_create_custom_investment.return_value = MagicMock(
            status_code=200,
            url="mock://create_custom_investment",
            json=lambda: {'id': CUSTOM_INST_ID, 'currency_code': 'USD'} # Assume USD for 'OTHER'
        )
        self.mock_api_client.create_custom_investment.return_value = {
            'id': CUSTOM_INST_ID,
            'currency_code': 'USD'
        }
        self.mock_api_client.create_custom_investment_price.return_value = MagicMock(
             status_code=200,
             url="mock://create_price",
             json=lambda: {'id': PRICE_ID}
        )
        self.mock_api_client.put_custom_investment_price.return_value = MagicMock(
             status_code=200,
             url="mock://put_price",
             json=lambda: {'id': PRICE_ID}
        )
        self.mock_api_client.resync_cash_account.return_value = MagicMock(
            status_code=200,
            url="mock://resync",
            json=lambda: {}
        )
        self.mock_api_client.delete_all_cash_account_transactions_in_portfolio.return_value = MagicMock(status_code=200)
        self.mock_api_client.delete_all_trades.return_value = MagicMock(status_code=200)
        self.mock_api_client.get_internal_exchange_rates.return_value = {
            'exchange_rates': {
                'GBP/AUD': {'rate': 1.8},
                'AUD/GBP': {'rate': 0.55},
                'GBP/USD': {'rate': 1.2},
                'USD/GBP': {'rate': 0.83},
                # Add others if needed by tests
            }
        }
        self.mock_api_client.get_holding.return_value = {
            'holding': {'instrument': {'currency_code': 'USD'}}
        }


        self.importer = SharesightCsvImporter(self.mock_api_client)

    def _run_import(self, csv_data, portfolio_name=PORTFOLIO_NAME, country_code=COUNTRY_CODE, delete_existing=False, min_date=None, exclude_exdate_transactions_before_min_date=None, opening_balance_on=None, opening_balance_from=None, min_line=None, max_line=None, prices_csv_data=None, exchange_rates_csv_data=None):
        """Helper to run the import process with mock file."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            transactions_path = temp_path / "transactions.csv"
            transactions_path.write_text(csv_data, encoding="utf-8")
            prices_path = temp_path / "prices.csv" if prices_csv_data else None
            exchange_rates_path = temp_path / "exchange_rates.csv" if exchange_rates_csv_data else None
            if prices_path:
                prices_path.write_text(prices_csv_data, encoding="utf-8")
            if exchange_rates_path:
                exchange_rates_path.write_text(exchange_rates_csv_data, encoding="utf-8")

            opening_balance = (
                OpeningBalanceOptions(
                    valuation_date=opening_balance_on,
                    source_portfolio_name=opening_balance_from,
                    exchange_rates_file_path=exchange_rates_path,
                )
                if opening_balance_on else None
            )
            options = ImportOptions(
                delete_existing=delete_existing,
                min_date=min_date,
                exclude_exdate_transactions_before_min_date=exclude_exdate_transactions_before_min_date,
                min_line=min_line,
                max_line=max_line,
                prices_file_path=prices_path,
                opening_balance=opening_balance,
            )
            self.importer.import_file(
                transactions_path, portfolio_name, country_code, options
            )

    def test_import_new_portfolio_simple_buy(self):
        """Test importing a single BUY into a non-existent portfolio."""
        csv_data = """unique_identifier,transaction_type,transaction_date,symbol,market,quantity,price_in_instrument_currency,amount,amount_currency,cash_account,description,brokerage_in_instrument_currency,instrument_currency,exchange_rate_gbp,amount_in_instrument_currency
tx1,BUY,2023-01-15,AAPL,NASDAQ,10,150.0,1505.0,USD,My USD Account,Test Buy,5,USD,0.8,1505.0
"""
        # Setup mocks for this specific scenario
        self.mock_api_client.get_portfolios.return_value = {'portfolios': []} # Portfolio doesn't exist
        self.mock_api_client.get_cash_accounts.return_value = {'cash_accounts': []} # No cash accounts yet
        self.mock_api_client.create_cash_account.return_value = {'cash_account': {'id': CASH_ACC_USD_ID}}

        self._run_import(csv_data)

        # Assertions
        self.mock_api_client.get_portfolios.assert_called_once()
        self.mock_api_client.create_portfolio.assert_called_once_with({
            "name": PORTFOLIO_NAME,
            "country_code": COUNTRY_CODE,
            "disable_automatic_transactions": True,
            "broker_email_api_enabled": False
        })
        self.mock_api_client.create_cash_account.assert_called_once_with(PORTFOLIO_ID, {"name": "My USD Account (USD)", "currency": "USD"})
        self.mock_api_client.try_create_trade.assert_called_once()
        trade_call_args = self.mock_api_client.try_create_trade.call_args[0][0]
        self.assertEqual(trade_call_args['transaction_type'], 'BUY')
        self.assertEqual(trade_call_args['symbol'], 'AAPL')
        self.assertEqual(trade_call_args['quantity'], '10')
        self.assertEqual(trade_call_args['price'], '150.0')
        self.assertEqual(trade_call_args['portfolio_id'], PORTFOLIO_ID)
        self.assertEqual(trade_call_args['brokerage'], '5') # Uses brokerage column
        self.assertEqual(trade_call_args['brokerage_currency_code'], 'USD')
        self.assertEqual(trade_call_args['exchange_rate'], '0.8') # Uses GBP rate for GB portfolio

        self.mock_api_client.try_create_cash_transaction.assert_called_once()
        cash_call_args = self.mock_api_client.try_create_cash_transaction.call_args[0][1]
        self.assertEqual(cash_call_args['amount'], 1505.0)
        self.assertEqual(cash_call_args['foreign_identifier'], 'tx1')
        self.assertEqual(cash_call_args['type_name'], 'BUY')

        self.mock_api_client.resync_cash_account.assert_called_once_with(CASH_ACC_USD_ID)

    def test_import_existing_portfolio_dividend(self):
        """Test importing a DIVIDEND into an existing portfolio with a known holding."""
        csv_data = """unique_identifier,transaction_type,transaction_date,symbol,market,quantity,amount,amount_currency,cash_account,description,exchange_rate_gbp,amount_in_gbp
tx2,DIVIDEND,2023-02-20,MSFT,NASDAQ,,100.0,USD,My USD Account,MSFT Div,0.8,80
"""
        # Setup mocks
        self.mock_api_client.get_portfolios.return_value = {
            'portfolios': [{'id': PORTFOLIO_ID, 'name': PORTFOLIO_NAME, 'currency_code': COUNTRY_CODE}]
        }
        self.mock_api_client.get_cash_accounts.return_value = {
            'cash_accounts': [{'id': CASH_ACC_USD_ID, 'name': 'My USD Account (USD)', 'currency': 'USD'}]
        }
        self.mock_api_client.get_portfolio_holdings.return_value = {
            'holdings': [{'id': HOLDING_ID_MSFT, 'instrument': {'code': 'MSFT', 'market_code': 'NASDAQ'}}]
        }
        self.mock_api_client.get_payouts.return_value = {'payouts': []} # No existing payout for this date

        self._run_import(csv_data)

        self.mock_api_client.create_portfolio.assert_not_called()
        self.mock_api_client.create_cash_account.assert_not_called()
        self.mock_api_client.get_portfolio_holdings.assert_called_once_with(PORTFOLIO_ID)
        self.mock_api_client.get_payouts.assert_called_once_with(PORTFOLIO_ID)
        self.mock_api_client.try_create_trade.assert_not_called()

        self.mock_api_client.try_create_payout.assert_called_once()
        payout_call_args = self.mock_api_client.try_create_payout.call_args[0][0]
        self.assertEqual(payout_call_args['holding_id'], HOLDING_ID_MSFT)
        self.assertEqual(payout_call_args['paid_on'], '2023-02-20')
        self.assertEqual(payout_call_args['amount'], '100.0')
        self.assertEqual(payout_call_args['currency_code'], 'USD')
        self.assertEqual(payout_call_args['banked_amount'], '80') # Uses amount_in_gbp

        self.mock_api_client.try_create_cash_transaction.assert_called_once()
        cash_call_args = self.mock_api_client.try_create_cash_transaction.call_args[0][1]
        self.assertEqual(cash_call_args['amount'], 100.0)
        self.assertEqual(cash_call_args['foreign_identifier'], 'tx2')
        self.assertEqual(cash_call_args['type_name'], 'DIVIDEND')

        self.mock_api_client.resync_cash_account.assert_called_once_with(CASH_ACC_USD_ID)


    def test_import_custom_instrument_buy(self):
        """Test importing a BUY for a custom instrument (market=OTHER)."""
        csv_data = """unique_identifier,transaction_type,transaction_date,symbol,market,quantity,price_in_instrument_currency,amount,amount_currency,cash_account,description,brokerage_in_instrument_currency,instrument_currency,exchange_rate_gbp,amount_in_instrument_currency,symbol_name,instrument_country_code,symbol_type
tx3,BUY,2023-03-10,MYFUND,OTHER,50,10.0,501.0,USD,My USD Account,Custom Fund,1,USD,0.8,501.0,My Custom Fund,US,MANAGED_FUND
"""
        # Setup mocks
        self.mock_api_client.get_portfolios.return_value = {
            'portfolios': [{'id': PORTFOLIO_ID, 'name': PORTFOLIO_NAME, 'currency_code': COUNTRY_CODE}]
        }
        self.mock_api_client.get_cash_accounts.return_value = {
            'cash_accounts': [{'id': CASH_ACC_USD_ID, 'name': 'My USD Account (USD)', 'currency': 'USD'}]
        }
        self.mock_api_client.get_custom_investments.return_value = {'custom_investments': []} # Does not exist yet
        self._run_import(csv_data)

        self.mock_api_client.get_custom_investments.assert_called_once_with(PORTFOLIO_ID)
        self.mock_api_client.create_custom_investment.assert_called_once()
        custom_inv_args = self.mock_api_client.create_custom_investment.call_args[0][0]
        expected_symbol_key = f"MYFUND-{PORTFOLIO_ID}"
        self.assertEqual(custom_inv_args['portfolio_id'], PORTFOLIO_ID)
        self.assertEqual(custom_inv_args['code'], expected_symbol_key)
        self.assertEqual(custom_inv_args['name'], f"My Custom Fund {SharesightCsvImporter.CUSTOM_INSTRUMENT_SUFFIX}")
        self.assertEqual(custom_inv_args['country_code'], 'US')
        self.assertEqual(custom_inv_args['currency_code'], 'USD')
        self.assertEqual(custom_inv_args['investment_type'], 'MANAGED_FUND')


        self.mock_api_client.try_create_trade.assert_called_once()
        trade_call_args = self.mock_api_client.try_create_trade.call_args[0][0]
        self.assertEqual(trade_call_args['symbol'], expected_symbol_key) # Uses portfolio-qualified symbol
        self.assertEqual(trade_call_args['market'], 'OTHER')
        self.assertEqual(trade_call_args['quantity'], '50')

        self.mock_api_client.try_create_cash_transaction.assert_called_once()
        cash_call_args = self.mock_api_client.try_create_cash_transaction.call_args[0][1]
        self.assertEqual(cash_call_args['amount'], 501.0)
        self.assertEqual(cash_call_args['foreign_identifier'], 'tx3')

        self.mock_api_client.resync_cash_account.assert_called_once_with(CASH_ACC_USD_ID)

    def test_opening_balance_generation(self):
        """Test generating opening balances from another portfolio."""
        opening_portfolio_name = "Source Portfolio"
        opening_portfolio_id = 987
        opening_date = datetime.date(2023, 1, 1)

        # Mock responses for the *source* portfolio
        self.mock_api_client.get_portfolios.side_effect = [
            {'portfolios': [{'id': opening_portfolio_id, 'name': opening_portfolio_name, 'currency_code': 'GBP'}]}, # First call for source
            {'portfolios': []} # Second call for target (doesn't exist yet)
        ]
        self.mock_api_client.get_cash_accounts.side_effect = [
             {'cash_accounts': [{'id': 111, 'name': 'Source GBP Acc (GBP)', 'currency': 'GBP'}]}, # Source cash accounts
             {'cash_accounts': []} # Target cash accounts (initially)
        ]

        mock_valuation = {
            'balance_date': opening_date.strftime("%Y-%m-%d"),
            'holdings': [
                {'id': 201, 'symbol': 'VUSA', 'market': 'LSE', 'quantity': 100.0, 'value': 5000.0},
                {'id': 202, 'symbol': f'MYFUND-{opening_portfolio_id}', 'market': 'OTHER', 'quantity': 50.0, 'value': 500.0}
            ],
            'cash_accounts': [
                {'cash_account_id': 111, 'name': 'Source GBP Acc (GBP)', 'currency_code': 'GBP', 'value': 1000.0}
            ]
        }
        self.mock_api_client.get_valuation_on.return_value = mock_valuation
        # Mock cash transactions for balance calculation in source portfolio
        self.mock_api_client.get_cash_account_transactions.return_value = {
            'cash_account_transactions': [
                 # Sharesight returns newest first
                 {'amount': '1000.00', 'balance': 1000.00, 'date_time': '2022-12-31T10:00:00Z'},
                 {'amount': '0.00', 'balance': 0.00, 'date_time': '2022-01-01T10:00:00Z'}
            ]
        }
         # Mock responses for the *target* portfolio creation
        self.mock_api_client.create_portfolio.return_value = {'id': PORTFOLIO_ID}
        self.mock_api_client.create_cash_account.return_value = {'cash_account': {'id': CASH_ACC_GBP_ID}} # Expect GBP account to be created
        self.mock_api_client.get_payouts.return_value = {'payouts': []}
        self.mock_api_client.get_portfolio_holdings.return_value = {'holdings': []}
        self.mock_api_client.get_custom_investments.return_value = {'custom_investments': []}

        # No CSV data needed, only generating opening balances
        csv_data = """unique_identifier,transaction_type,transaction_date,symbol,market,quantity,price_in_instrument_currency,amount,amount_currency,cash_account,description,amount_in_gbp
cash-account-template,DEPOSIT,2000-01-01,,,,,0,GBP,Source GBP Acc,Filtered cash-account template,
"""
        exchange_rates_csv_data = """date,GBP/GBP,AUD/GBP
2023-01-01,1,0.55
"""

        self.mock_api_client.get_holding.return_value = {
            'holding': {'instrument': {'currency_code': 'GBP'}}
        }
        self._run_import(
            csv_data,
            opening_balance_on=opening_date,
            opening_balance_from=opening_portfolio_name,
            exchange_rates_csv_data=exchange_rates_csv_data
        )

        # Assertions
        self.mock_api_client.get_portfolios.assert_has_calls([
            call(), # Called once to find source portfolio
            call()  # Called again to check target portfolio existence
        ])
        self.mock_api_client.get_valuation_on.assert_called_once_with(opening_portfolio_id, "2022-12-31")
        self.mock_api_client.get_cash_account_transactions.assert_called_once_with(111, "2000-01-01", "2022-12-31")
        self.mock_api_client.create_portfolio.assert_called_once() # Target portfolio created
        # Should create GBP cash account in target based on generated opening balance
        self.mock_api_client.create_cash_account.assert_called_once_with(PORTFOLIO_ID, {"name": "Source GBP Acc (GBP)", "currency": "GBP"})

        # Check that trades/cash corresponding to opening balances were created in the target
        self.assertEqual(self.mock_api_client.try_create_trade.call_count, 2) # VUSA + MYFUND
        self.assertEqual(self.mock_api_client.try_create_cash_transaction.call_count, 1) # GBP Account

        trade_calls = self.mock_api_client.try_create_trade.call_args_list
        trade_args_list = [c[0][0] for c in trade_calls] # Extract the 'api_request_data' dict

        # Check VUSA Opening Balance Trade
        vusa_trade = next(t for t in trade_args_list if t['symbol'] == 'VUSA')
        self.assertEqual(vusa_trade['transaction_type'], 'BUY')
        self.assertEqual(vusa_trade['transaction_date'], opening_date.strftime("%Y-%m-%d"))
        self.assertEqual(vusa_trade['quantity'], 100.0)
        self.assertEqual(vusa_trade['price'], 50.0)
        self.assertEqual(vusa_trade['portfolio_id'], PORTFOLIO_ID)

        # Custom symbols are cleaned from the source ID and qualified for the target portfolio.
        myfund_trade = next(t for t in trade_args_list if t['symbol'] == f'MYFUND-{PORTFOLIO_ID}')
        self.assertEqual(myfund_trade['transaction_type'], 'BUY')
        self.assertEqual(myfund_trade['market'], 'OTHER') # Market remains OTHER
        self.assertEqual(myfund_trade['transaction_date'], opening_date.strftime("%Y-%m-%d"))
        self.assertEqual(myfund_trade['quantity'], 50.0)
        self.assertEqual(myfund_trade['price'], 10.0)
        self.assertEqual(myfund_trade['portfolio_id'], PORTFOLIO_ID)

        # Check Cash Opening Balance (DEPOSIT)
        cash_call_args = self.mock_api_client.try_create_cash_transaction.call_args[0][1]
        self.assertEqual(cash_call_args['type_name'], 'DEPOSIT')
        self.assertEqual(cash_call_args['date_time'], opening_date.strftime("%Y-%m-%d"))
        self.assertEqual(cash_call_args['amount'], 1000.0) # Calculated total_amount
        self.assertIn('Opening Balance', cash_call_args['description'])
        # Cash transaction called with target cash account ID
        self.assertEqual(self.mock_api_client.try_create_cash_transaction.call_args[0][0], CASH_ACC_GBP_ID)


    def test_date_filtering(self):
        """Test the min_date filter."""
        csv_data = """unique_identifier,transaction_type,transaction_date,symbol,market,quantity,price_in_instrument_currency,amount,amount_currency,cash_account,description,brokerage_in_instrument_currency,instrument_currency,exchange_rate_gbp,amount_in_instrument_currency
tx_old,BUY,2022-12-31,AAPL,NASDAQ,5,140.0,701.0,USD,My USD Account,Old Buy,1,USD,0.8,701.0
tx_new,BUY,2023-01-15,AAPL,NASDAQ,10,150.0,1502.0,USD,My USD Account,New Buy,2,USD,0.8,1502.0
"""
        min_date = datetime.date(2023, 1, 1)

        # Assume portfolio exists
        self.mock_api_client.get_portfolios.return_value = {
             'portfolios': [{'id': PORTFOLIO_ID, 'name': PORTFOLIO_NAME, 'currency_code': COUNTRY_CODE}]
        }
        self.mock_api_client.get_cash_accounts.return_value = {
            'cash_accounts': [{'id': CASH_ACC_USD_ID, 'name': 'My USD Account (USD)', 'currency': 'USD'}]
        }
        self._run_import(csv_data, min_date=min_date)

        # Only the new transaction should be processed
        self.mock_api_client.try_create_trade.assert_called_once()
        trade_call_args = self.mock_api_client.try_create_trade.call_args[0][0]
        self.assertEqual(trade_call_args['unique_identifier'], 'tx_new')
        self.assertEqual(trade_call_args['quantity'], '10')

        self.mock_api_client.try_create_cash_transaction.assert_called_once()
        cash_call_args = self.mock_api_client.try_create_cash_transaction.call_args[0][1]
        self.assertEqual(cash_call_args['foreign_identifier'], 'tx_new')
        self.assertEqual(cash_call_args['amount'], 1502.0)

    def test_line_filtering(self):
        """Test the min_line and max_line filters."""
        csv_data = """unique_identifier,transaction_type,transaction_date,symbol,market,quantity,price_in_instrument_currency,amount,amount_currency,cash_account,description,brokerage_in_instrument_currency,instrument_currency,exchange_rate_gbp,amount_in_instrument_currency
tx1,BUY,2023-01-15,AAPL,NASDAQ,10,150.0,1501,USD,My USD Account,Buy 1,1,USD,0.8,1501
tx2,BUY,2023-01-16,MSFT,NASDAQ,20,250.0,5001,USD,My USD Account,Buy 2,1,USD,0.8,5001
tx3,SELL,2023-01-17,AAPL,NASDAQ,5,155.0,774,USD,My USD Account,Sell 1,1,USD,0.8,774
"""
        # Assume portfolio exists
        self.mock_api_client.get_portfolios.return_value = {
             'portfolios': [{'id': PORTFOLIO_ID, 'name': PORTFOLIO_NAME, 'currency_code': COUNTRY_CODE}]
        }
        self.mock_api_client.get_cash_accounts.return_value = {
            'cash_accounts': [{'id': CASH_ACC_USD_ID, 'name': 'My USD Account (USD)', 'currency': 'USD'}]
        }
        # Run import for lines 2 and 3 (data rows, header is line 1)
        # CSV reader line_num is 1-based and includes header. So data lines are 2, 3, 4.
        # We want to process tx2 (line 3)
        self._run_import(csv_data, min_line=3, max_line=3)

        # Assertions - Should only process tx2
        self.mock_api_client.try_create_trade.assert_called_once()
        trade_call_args = self.mock_api_client.try_create_trade.call_args[0][0]
        self.assertEqual(trade_call_args['unique_identifier'], 'tx2')
        self.assertEqual(trade_call_args['symbol'], 'MSFT')
        self.assertEqual(trade_call_args['quantity'], '20')

        self.mock_api_client.try_create_cash_transaction.assert_called_once()
        cash_call_args = self.mock_api_client.try_create_cash_transaction.call_args[0][1]
        self.assertEqual(cash_call_args['foreign_identifier'], 'tx2')
        self.assertEqual(cash_call_args['amount'], 5001.0)


    def test_delete_existing(self):
        """Test the delete_existing=True flag."""
        csv_data = """unique_identifier,transaction_type,transaction_date,symbol,market,quantity,price_in_instrument_currency,amount,amount_currency,cash_account,description,brokerage_in_instrument_currency,instrument_currency,exchange_rate_gbp,amount_in_instrument_currency
tx_del,BUY,2023-01-15,AAPL,NASDAQ,10,150.0,1505.0,USD,My USD Account,Test Buy,5,USD,0.8,1505.0
"""
        # Setup mocks - Portfolio *does* exist this time
        self.mock_api_client.get_portfolios.return_value = {
             'portfolios': [{'id': PORTFOLIO_ID, 'name': PORTFOLIO_NAME, 'currency_code': COUNTRY_CODE}]
        }
        # Assume some cash accounts exist but we want to recreate based on file
        self.mock_api_client.get_cash_accounts.return_value = {
            'cash_accounts': [{'id': 999, 'name': 'Old Account (USD)', 'currency': 'USD'}]
        }
        self.mock_api_client.create_cash_account.return_value = {'cash_account': {'id': CASH_ACC_USD_ID}}

        self._run_import(csv_data, delete_existing=True)

        self.mock_api_client.get_portfolios.assert_called_once()
        # Should NOT delete portfolio, but should delete trades/cash txs
        self.mock_api_client.delete_portfolio.assert_not_called()
        self.mock_api_client.delete_all_cash_account_transactions_in_portfolio.assert_called_once_with(PORTFOLIO_ID)
        self.mock_api_client.delete_all_holdings.assert_called_once_with(PORTFOLIO_ID)

        # Should recreate cash accounts based on file content
        self.mock_api_client.create_cash_account.assert_called_once_with(PORTFOLIO_ID, {"name": "My USD Account (USD)", "currency": "USD"})

        # Should still process the transaction
        self.mock_api_client.try_create_trade.assert_called_once()
        trade_call_args = self.mock_api_client.try_create_trade.call_args[0][0]
        self.assertEqual(trade_call_args['unique_identifier'], 'tx_del')
        self.mock_api_client.try_create_cash_transaction.assert_called_once()
        cash_call_args = self.mock_api_client.try_create_cash_transaction.call_args[0][1]
        self.assertEqual(cash_call_args['foreign_identifier'], 'tx_del')

        self.mock_api_client.resync_cash_account.assert_called_once_with(CASH_ACC_USD_ID) # Resync the *new* account

    def test_price_file_import_create_and_update(self):
        """Test importing prices for a custom instrument, creating one, updating another."""
        prices_csv_data = """symbol,date,price
MYFUND,2023-03-11,10.50
MYFUND,2023-03-12,10.60
OTHERFUND,2023-03-12,25.00
"""
        # Setup Mocks - Assume portfolio and custom instruments exist
        self.mock_api_client.get_portfolios.return_value = {
             'portfolios': [{'id': PORTFOLIO_ID, 'name': PORTFOLIO_NAME, 'currency_code': COUNTRY_CODE}]
        }
        self.mock_api_client.get_cash_accounts.return_value = {'cash_accounts': []} # No cash needed for price import
        # One instrument exists (MYFUND), one doesn't (OTHERFUND) - note unqualified symbol lookup
        self.mock_api_client.get_custom_investments.return_value = {
            'custom_investments': [
                {'id': CUSTOM_INST_ID, 'code': f'MYFUND-{PORTFOLIO_ID}', 'name': 'My Custom Fund (AUTO)'},
                {'id': CUSTOM_INST_ID + 1, 'code': f'OTHERFUND-{PORTFOLIO_ID}', 'name': 'Other Custom Fund (AUTO)'}
            ]
        }
        # Simulate one price already existing for MYFUND on 2023-03-12
        def get_price_side_effect(instrument_id, start_date, end_date):
            if instrument_id == CUSTOM_INST_ID and start_date == '2023-03-12':
                return {'prices': [{'id': PRICE_ID, 'last_traded_price': '10.55', 'last_traded_on': '2023-03-12'}]}
            else:
                return {'prices': []}
        self.mock_api_client.get_custom_investment_prices.side_effect = get_price_side_effect

        # Dummy main CSV data - needed to trigger portfolio setup, but content doesn't matter here
        csv_data = """unique_identifier,transaction_type,transaction_date,symbol,market,quantity,price,amount,amount_currency,cash_account,description
"""

        self._run_import(csv_data, prices_csv_data=prices_csv_data)

        # Assertions
        self.assertEqual(self.mock_api_client.get_custom_investments.call_count, 2)

        # Check calls to get existing prices
        self.mock_api_client.get_custom_investment_prices.assert_has_calls([
            call(CUSTOM_INST_ID, '2023-03-11', '2023-03-11'), # MYFUND date 1
            call(CUSTOM_INST_ID, '2023-03-12', '2023-03-12'), # MYFUND date 2 (will find existing)
            call(CUSTOM_INST_ID + 1, '2023-03-12', '2023-03-12') # OTHERFUND date 1
        ], any_order=True)

        # Check creation/update calls
        self.mock_api_client.create_custom_investment_price.assert_has_calls([
             # MYFUND on 2023-03-11 (new)
             call(CUSTOM_INST_ID, {'last_traded_price': '10.50', 'last_traded_on': '2023-03-11'}),
             # OTHERFUND on 2023-03-12 (new)
             call(CUSTOM_INST_ID + 1, {'last_traded_price': '25.00', 'last_traded_on': '2023-03-12'})
        ], any_order=True)
        self.assertEqual(self.mock_api_client.create_custom_investment_price.call_count, 2)

        # Check update call for MYFUND on 2023-03-12
        self.mock_api_client.put_custom_investment_price.assert_called_once_with(
            PRICE_ID, {'last_traded_price': '10.60', 'last_traded_on': '2023-03-12'}
        )

    def test_duplicate_handling_trade(self):
        """Test skipping a trade if API returns duplicate unique_identifier error."""
        csv_data = """unique_identifier,transaction_type,transaction_date,symbol,market,quantity,price,amount,amount_currency,cash_account,description,brokerage,brokerage_currency,exchange_rate_gbp
tx_dup,BUY,2023-01-15,AAPL,NASDAQ,10,150.0,1505.0,USD,My USD Account,Test Buy,5,USD,0.8
"""
        # Setup mocks - Portfolio exists
        self.mock_api_client.get_portfolios.return_value = {
             'portfolios': [{'id': PORTFOLIO_ID, 'name': PORTFOLIO_NAME, 'currency_code': COUNTRY_CODE}]
        }
        self.mock_api_client.get_cash_accounts.return_value = {
            'cash_accounts': [{'id': CASH_ACC_USD_ID, 'name': 'My USD Account (USD)', 'currency': 'USD'}]
        }
        # Simulate duplicate error for trade
        self.mock_api_client.try_create_trade.return_value = api_result(
            {'errors': {'unique_identifier': ["A trade with this unique_identifier already exists in the portfolio."]}},
            status_code=422,
            duplicate=True,
        )
        # Simulate duplicate error for cash as well (might happen if trade failed but cash didn't rollback)
        self.mock_api_client.try_create_cash_transaction.return_value = api_result(
            {'errors': {'foreign_identifier': ["has already been taken"]}},
            status_code=422,
            duplicate=True,
        )


        # Use patch to capture print output
        with patch('builtins.print') as mock_print:
            self._run_import(csv_data)

            # Assertions
            self.mock_api_client.try_create_trade.assert_called_once() # Attempted once
            # Cash transaction should still be attempted even if trade fails/is duplicate
            self.mock_api_client.try_create_cash_transaction.assert_called_once()

            # Check log output for skipped message
            # Note: This checks if *any* print call contains the text. Might need refinement.
            self.assertTrue(any("Skipped (duplicate)" in str(call_args) for call_args in mock_print.call_args_list))

            # Resync should still be called
            self.mock_api_client.resync_cash_account.assert_called_once_with(CASH_ACC_USD_ID)

    def test_duplicate_handling_payout(self):
        """Test skipping a payout if it already exists based on lookup."""
        csv_data = """unique_identifier,transaction_type,transaction_date,symbol,market,quantity,amount,amount_currency,cash_account,description,exchange_rate_gbp,amount_in_gbp
tx_div_dup,DIVIDEND,2023-02-20,MSFT,NASDAQ,,100.0,USD,My USD Account,MSFT Div Dup,0.8,80
"""
        payout_date = '2023-02-20'
        # Setup mocks
        self.mock_api_client.get_portfolios.return_value = {
            'portfolios': [{'id': PORTFOLIO_ID, 'name': PORTFOLIO_NAME, 'currency_code': COUNTRY_CODE}]
        }
        self.mock_api_client.get_cash_accounts.return_value = {
            'cash_accounts': [{'id': CASH_ACC_USD_ID, 'name': 'My USD Account (USD)', 'currency': 'USD'}]
        }
        self.mock_api_client.get_portfolio_holdings.return_value = {
            'holdings': [{'id': HOLDING_ID_MSFT, 'instrument': {'code': 'MSFT', 'market_code': 'NASDAQ'}}]
        }
        # Simulate finding an *existing* payout for this holding/date
        self.mock_api_client.get_payouts.return_value = {
            'payouts': [{'id': 666, 'holding_id': HOLDING_ID_MSFT, 'paid_on': payout_date}]
        }
         # Simulate duplicate cash as well
        self.mock_api_client.try_create_cash_transaction.return_value = api_result(
            {'errors': {'foreign_identifier': ["has already been taken"]}},
            status_code=422,
            duplicate=True,
        )


        with patch('builtins.print') as mock_print:
            self._run_import(csv_data)

            # Assertions
            self.mock_api_client.get_payouts.assert_called_once_with(PORTFOLIO_ID)
            # Payout creation should NOT be called because it was found in the lookup
            self.mock_api_client.try_create_payout.assert_not_called()

            # Cash transaction should still be attempted (and potentially skipped via API error)
            self.mock_api_client.try_create_cash_transaction.assert_called_once()

            # Check log output for skipped payout message
            self.assertTrue(any("Skipping payout as it already appears to exist" in str(call_args) for call_args in mock_print.call_args_list))
             # Check log output for skipped cash message
            self.assertTrue(any("Skipped (duplicate)" in str(call_args) for call_args in mock_print.call_args_list))


            self.mock_api_client.resync_cash_account.assert_called_once_with(CASH_ACC_USD_ID)


    def test_accrued_income_buy(self):
        """Test processing accrued income on a BUY transaction (creates CAPITAL_CALL)."""
        csv_data = """unique_identifier,transaction_type,transaction_date,symbol,market,quantity,price_in_instrument_currency,amount,amount_currency,cash_account,description,brokerage_in_instrument_currency,instrument_currency,exchange_rate_gbp,amount_in_instrument_currency,accrued_income,accrued_income_in_instrument_currency,accrued_income_in_gbp,accrued_income_in_aud,amount_in_gbp
tx_bond_buy,BUY,2023-04-01,BNDX,NASDAQ,100,50.0,5015.0,USD,My USD Account,Bond Buy Dirty,5,USD,0.8,5015.0,10.0,10.0,8.0,16.0,4012.0
"""
        # Mock setup - portfolio/cash exists
        self.mock_api_client.get_portfolios.return_value = {
             'portfolios': [{'id': PORTFOLIO_ID, 'name': PORTFOLIO_NAME, 'currency_code': COUNTRY_CODE}]
        }
        self.mock_api_client.get_cash_accounts.return_value = {
            'cash_accounts': [{'id': CASH_ACC_USD_ID, 'name': 'My USD Account (USD)', 'currency': 'USD'}]
        }
        # Mock trade returns holding ID
        self.mock_api_client.try_create_trade.return_value = api_result(
            {'trade': {'holding_id': HOLDING_ID_AAPL, 'transaction_type': 'SPLIT'}}
        )

        self._run_import(csv_data)

        # Assertions: Should call try_create_trade TWICE
        # 1. For the original BUY
        # 2. For the generated CAPITAL_CALL
        self.assertEqual(self.mock_api_client.try_create_trade.call_count, 2)

        calls = self.mock_api_client.try_create_trade.call_args_list
        buy_call_args = calls[0][0][0] # First call, first arg (api_request_data)
        capital_call_args = calls[1][0][0] # Second call, first arg

        # Check BUY call
        self.assertEqual(buy_call_args['transaction_type'], 'BUY')
        self.assertEqual(buy_call_args['unique_identifier'], 'tx_bond_buy')
        self.assertEqual(buy_call_args['quantity'], '100')
        self.assertEqual(buy_call_args['price'], '50.0') # Original price

        # Check CAPITAL_CALL call
        self.assertEqual(capital_call_args['transaction_type'], 'CAPITAL_CALL')
        # Unique ID should be modified
        self.assertEqual(capital_call_args['unique_identifier'], 'tx_bond_buy-accrued_income')
        self.assertNotIn('accrued_income', capital_call_args) # Should not be passed in call
        self.assertEqual(capital_call_args['quantity'], '100') # Same quantity
        self.assertEqual(capital_call_args['symbol'], 'BNDX')
        # Capital call value should be based on accrued_income amount converted to portfolio currency
        self.assertEqual(capital_call_args['capital_return_value'], 10.0)
        self.assertEqual(capital_call_args['paid_on'], '2023-04-01')


        self.assertEqual(self.mock_api_client.try_create_cash_transaction.call_count, 2)
        cash_calls = self.mock_api_client.try_create_cash_transaction.call_args_list
        buy_cash_args = next(c[0][1] for c in cash_calls if c[0][1]['foreign_identifier'] == 'tx_bond_buy')
        accrued_cash_args = next(c[0][1] for c in cash_calls if c[0][1]['foreign_identifier'] == 'tx_bond_buy-accrued_income')
        self.assertEqual(buy_cash_args['amount'], 5005.0)
        self.assertEqual(accrued_cash_args['amount'], 10.0)

        self.mock_api_client.resync_cash_account.assert_called_once_with(CASH_ACC_USD_ID)


    def test_accrued_income_sell(self):
        """Test processing accrued income on a SELL transaction (creates DIVIDEND/INTEREST payout)."""
        csv_data = """unique_identifier,transaction_type,transaction_date,symbol,market,quantity,price_in_instrument_currency,amount,amount_currency,cash_account,description,brokerage_in_instrument_currency,instrument_currency,exchange_rate_gbp,amount_in_instrument_currency,accrued_income,accrued_income_in_instrument_currency,accrued_income_in_gbp,accrued_income_in_aud,amount_in_gbp
tx_bond_sell,SELL,2023-05-01,BNDX,NASDAQ,50,51.0,2545.0,USD,My USD Account,Bond Sell Dirty,5,USD,0.8,2545.0,7.0,7.0,5.6,11.2,2036.0
"""
         # Mock setup - portfolio/cash/holding exists
        self.mock_api_client.get_portfolios.return_value = {
             'portfolios': [{'id': PORTFOLIO_ID, 'name': PORTFOLIO_NAME, 'currency_code': COUNTRY_CODE}]
        }
        self.mock_api_client.get_cash_accounts.return_value = {
            'cash_accounts': [{'id': CASH_ACC_USD_ID, 'name': 'My USD Account (USD)', 'currency': 'USD'}]
        }
        self.mock_api_client.get_portfolio_holdings.return_value = {
            'holdings': [{'id': HOLDING_ID_AAPL, 'instrument': {'code': 'BNDX', 'market_code': 'NASDAQ'}}] # Using AAPL ID just for test
        }
        # Mock trade returns holding ID
        self.mock_api_client.try_create_trade.return_value = api_result(
            {'trade': {'holding_id': HOLDING_ID_AAPL, 'transaction_type': 'SPLIT'}}
        )
        self.mock_api_client.get_payouts.return_value = {'payouts': []} # No existing payout


        self._run_import(csv_data)

        # Assertions: Should call try_create_trade ONCE for the SELL
        self.assertEqual(self.mock_api_client.try_create_trade.call_count, 1)
        sell_call_args = self.mock_api_client.try_create_trade.call_args[0][0]
        self.assertEqual(sell_call_args['transaction_type'], 'SELL')
        self.assertEqual(sell_call_args['unique_identifier'], 'tx_bond_sell')
        self.assertEqual(sell_call_args['quantity'], '50')

        # Should call try_create_payout ONCE for the accrued income
        self.assertEqual(self.mock_api_client.try_create_payout.call_count, 1)
        payout_call_args = self.mock_api_client.try_create_payout.call_args[0][0]
        self.assertEqual(payout_call_args['holding_id'], HOLDING_ID_AAPL)
        self.assertEqual(payout_call_args['paid_on'], '2023-05-01') # Uses transaction date
        self.assertEqual(payout_call_args['amount'], '7.0')
        self.assertEqual(payout_call_args['currency_code'], 'USD')
        # Banked amount uses converted accrued_income value
        self.assertEqual(payout_call_args['banked_amount'], '5.6') # 7.0 USD * 0.8 GBP/USD = 5.6 GBP

        # Should call try_create_cash_transaction TWICE
        # 1. For the original SELL amount
        # 2. For the generated Payout amount
        self.assertEqual(self.mock_api_client.try_create_cash_transaction.call_count, 2)
        cash_calls = self.mock_api_client.try_create_cash_transaction.call_args_list
        sell_cash_args = next(c[0][1] for c in cash_calls if c[0][1]['foreign_identifier'] == 'tx_bond_sell')
        payout_cash_args = next(c[0][1] for c in cash_calls if c[0][1]['foreign_identifier'] == 'tx_bond_sell-accrued_income')

        self.assertEqual(sell_cash_args['amount'], 2538.0)
        self.assertEqual(sell_cash_args['type_name'], 'SELL')

        self.assertEqual(payout_cash_args['amount'], 7.0) # Accrued income amount
        # The type_name for cash derived from payout uses the original row's type ('SELL'),
        # which might be slightly confusing but is how _process_payout calls _process_cash.
        self.assertEqual(payout_cash_args['type_name'], 'SELL')

        self.mock_api_client.resync_cash_account.assert_called_once_with(CASH_ACC_USD_ID)


if __name__ == '__main__':
    unittest.main() 
