import unittest

from sharesight_csv_input import TransactionRow
from sharesight_import_plan import (
    PlannedCash,
    PlannedPayout,
    PlannedTrade,
    SKIP_CASH_TRANSACTION_FLAG,
    build_import_plan,
)


def row(transaction_type, **overrides):
    data = {
        "unique_identifier": "transaction-1",
        "transaction_type": transaction_type,
        "transaction_date": "2024-01-10",
        "amount": "10",
        "amount_in_instrument_currency": "10",
        "amount_in_aud": "19",
        "amount_in_gbp": "10",
        "accrued_income": "0",
    }
    data.update(overrides)
    return TransactionRow(2, data)


class ImportPlanTests(unittest.TestCase):
    def test_buy_expands_to_trade_then_cash(self):
        plan = build_import_plan([row("BUY")])

        self.assertEqual([type(operation) for operation in plan], [PlannedTrade, PlannedCash])
        self.assertIs(plan[0].data, plan[1].data)

    def test_non_cash_trade_has_no_cash_operation(self):
        plan = build_import_plan([row("ADJUST_COST_BASE")])

        self.assertEqual([type(operation) for operation in plan], [PlannedTrade])

    def test_internal_skip_flag_suppresses_cash_operation(self):
        plan = build_import_plan(
            [row("BUY", **{SKIP_CASH_TRANSACTION_FLAG: True})],
        )

        self.assertEqual([type(operation) for operation in plan], [PlannedTrade])

    def test_sell_with_accrued_income_expands_in_execution_order(self):
        plan = build_import_plan([
            row(
                "SELL",
                accrued_income="2",
                accrued_income_in_instrument_currency="2",
                accrued_income_in_aud="3.8",
                accrued_income_in_gbp="2",
            )
        ])

        self.assertEqual(
            [type(operation) for operation in plan],
            [PlannedTrade, PlannedCash, PlannedPayout, PlannedCash],
        )
        self.assertEqual(plan[2].data["unique_identifier"], "transaction-1-accrued_income")
        self.assertEqual(plan[2].data["goes_ex_on"], "2024-01-09")

    def test_retained_income_expands_to_two_non_cash_operations(self):
        plan = build_import_plan([row("RETAINED_NET_INCOME")])

        self.assertEqual([type(operation) for operation in plan], [PlannedPayout, PlannedTrade])
        self.assertEqual(plan[1].data["transaction_type"], "CAPITAL_CALL")
        self.assertEqual(plan[1].data["unique_identifier"], "transaction-1_CALL")

    def test_zero_capital_operation_is_omitted(self):
        plan = build_import_plan([row("CAPITAL_RETURN", amount="0")])

        self.assertEqual(plan, [])


if __name__ == "__main__":
    unittest.main()
