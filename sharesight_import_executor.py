import sys

from sharesight_import_plan import PlannedCash, PlannedMerge, PlannedPayout, PlannedTrade
from sharesight_payloads import (
    build_cash_payload,
    build_merge_payload,
    build_payout_payload,
    build_trade_payload,
)
from sharesight_trade_validation import validate_trade


def qualify_custom_instrument_symbol(data_row, portfolio_id):
    symbol = data_row.get("symbol")
    return f"{symbol}-{portfolio_id}" if data_row.get("market", "").lower() == "other" else symbol


def holding_lookup_key(portfolio_id, symbol, market):
    return f"{portfolio_id}-{market}-{symbol}".lower()


class ImportExecutor:
    def __init__(self, api_client, portfolio_id, country_code, cash_accounts):
        self._api_client = api_client
        self._portfolio_id = portfolio_id
        self._country_code = country_code
        self._cash_accounts = cash_accounts

        payouts = api_client.get_payouts(portfolio_id).get("payouts")
        self._payouts = {
            self._payout_lookup_key(payout["holding_id"], payout["paid_on"]): payout["id"]
            for payout in payouts
        }
        holdings = api_client.get_portfolio_holdings(portfolio_id)["holdings"]
        self._holdings = {
            holding_lookup_key(
                portfolio_id,
                holding["instrument"]["code"],
                holding["instrument"]["market_code"],
            ): holding["id"]
            for holding in holdings
        }

    def execute(self, plan):
        for operation in plan:
            if isinstance(operation, PlannedMerge):
                if not self._execute_merge(operation):
                    return False
            elif not self._execute_operation(operation):
                return False

        print("Syncing cash accounts")
        for cash_account_id in set(self._cash_accounts.values()):
            if cash_account_id:
                self._api_client.resync_cash_account(cash_account_id)
        return True

    def _execute_merge(self, operation):
        cancel_data = self._qualified_data(operation.cancel.data)
        buy_data = self._qualified_data(operation.buy.data)
        log_prefix = (
            f"Lines {operation.cancel.line_number}-{operation.buy.line_number}\t"
            f"{cancel_data['unique_identifier']}\tMERGE"
        )
        cancel_key = holding_lookup_key(
            self._portfolio_id, cancel_data.get("symbol"), cancel_data.get("market")
        )
        holding_id = self._holdings.get(cancel_key)
        if holding_id is None:
            print(
                f"{log_prefix}\tERROR Unable to find holding id for cancellation matching "
                f"{cancel_key} - {cancel_data.get('symbol')}, {cancel_data.get('market')}. "
                f"Known holdings {self._holdings}",
                file=sys.stderr,
            )
            return False

        payload = build_merge_payload(holding_id, buy_data)
        response = self._api_client.try_create_holding_merge(self._portfolio_id, payload)
        self._print_result(log_prefix, payload, response)
        return True

    def _execute_operation(self, operation):
        data = self._qualified_data(operation.data)
        log_prefix = (
            f"Line {operation.line_number}\t{data['unique_identifier']}\t{data['transaction_type']}"
        )
        holding_key = holding_lookup_key(
            self._portfolio_id, data.get("symbol"), data.get("market")
        )

        if isinstance(operation, PlannedTrade):
            holding_id = self._create_trade(log_prefix, data)
            if holding_id:
                print(f"{log_prefix}\tSaved holding id {holding_id} in {holding_key}")
                self._holdings[holding_key] = holding_id
            else:
                print(f"{log_prefix}\tLooking up holding id for {holding_key}")
                if self._holdings.get(holding_key) is None:
                    print(f"{log_prefix}\tMissing holding id for {holding_key}")
            return True

        if isinstance(operation, PlannedPayout):
            holding_id = self._holdings.get(holding_key)
            if holding_id is None:
                print(
                    f'{log_prefix}\tERROR Unable to find holding id matching "{holding_key}" - '
                    f"{data.get('symbol')}, {data.get('market')}. Known holdings {self._holdings}",
                    file=sys.stderr,
                )
                return False
            self._create_payout(log_prefix, data, holding_id)
            return True

        if isinstance(operation, PlannedCash):
            cash_account_key = f"{data.get('amount_currency')}-{data.get('cash_account') or 'Account'}"
            self._create_cash(self._cash_accounts.get(cash_account_key), log_prefix, data)
            return True

        raise AssertionError(f"Unhandled planned operation: {type(operation).__name__}")

    def _qualified_data(self, data):
        qualified = dict(data)
        qualified["symbol"] = qualify_custom_instrument_symbol(qualified, self._portfolio_id)
        return qualified

    def _create_trade(self, log_prefix, data):
        if float(data.get("quantity")) < 0:
            print(
                f"{log_prefix}\tWARN Shorts are not supported by Sharesight. "
                f"Quantity is negative: {data.get('quantity')}"
            )
        payload = build_trade_payload(self._portfolio_id, self._country_code, data)
        response = self._api_client.try_create_trade(payload)
        self._print_result(log_prefix, payload, response)
        response_data = response.data.get("trade")
        holding_id = response_data.get("holding_id") if response_data else None
        if not holding_id:
            reason = "but no error" if not response.errors else "due to error"
            print(
                f"{log_prefix}\t{response.status_code} Couldn't find holding id {reason} - "
                f"{response.data} - skipping instrument currency check and validation"
            )
            return None

        holding = self._api_client.get_holding(holding_id)
        holding_currency = holding["holding"]["instrument"]["currency_code"]
        for message in validate_trade(data, response_data, holding_currency):
            print(f"{log_prefix}\t{message}")
        return holding_id

    def _create_payout(self, log_prefix, data, holding_id):
        payout_key = self._payout_lookup_key(holding_id, data.get("transaction_date"))
        if payout_key in self._payouts:
            print(f"{log_prefix}\tWARN: Skipping payout as it already appears to exist")
            return
        payload = build_payout_payload(
            self._portfolio_id, holding_id, self._country_code, data
        )
        response = self._api_client.try_create_payout(payload)
        self._print_result(log_prefix, payload, response)

    def _create_cash(self, cash_account_id, log_prefix, data):
        if cash_account_id is None:
            raise ValueError(
                f"Unable to find cash account {data.get('amount_currency')} {data.get('cash_account')}"
            )
        payload = build_cash_payload(data)
        response = self._api_client.try_create_cash_transaction(cash_account_id, payload)
        self._print_result(log_prefix, payload, response)

    def _payout_lookup_key(self, holding_id, paid_on):
        return f"{self._portfolio_id}-{holding_id}-{paid_on}".lower()

    @staticmethod
    def _print_result(log_prefix, payload, result):
        if result.successful:
            print(f"{log_prefix}\t{result.status_code} Success {result.endpoint}")
        elif result.duplicate:
            print(
                f"{log_prefix}\t{result.status_code} Skipped (duplicate): "
                f"{result.data} {payload} {result.endpoint}"
            )
        else:
            print(
                f"{log_prefix}\t{result.status_code} {result.data} {payload} {result.endpoint}",
                file=sys.stderr,
            )
