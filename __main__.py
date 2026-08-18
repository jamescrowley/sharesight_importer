import datetime
from os import getenv
import argparse
from sharesight_api_client import SharesightApiClient
from sharesight_csv_importer import SharesightCsvImporter
from sharesight_import_options import ImportOptions, OpeningBalanceOptions

def main():
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('--client_id', default=getenv('SHARESIGHT_CLIENT_ID'), type=str, required=False, help=argparse.SUPPRESS)
    parser.add_argument('--client_secret', default=getenv('SHARESIGHT_CLIENT_SECRET'), type=str, required=False, help=argparse.SUPPRESS)
    parser.add_argument('-p', '--portfolio_name', type=str, required=True, help='The portfolio name')
    parser.add_argument('-f', '--file_name', type=str, required=True, help='The file name')
    parser.add_argument('-pf', '--prices_file_name', type=str, required=False, help='The prices file name')
    parser.add_argument('-c', '--country_code', type=str, required=True, help='The country code (GB/AU)')
    parser.add_argument('-r', '--delete_existing', type=bool, action=argparse.BooleanOptionalAction, help='Remove the portfolio')
    parser.add_argument('-d', '--min_date', type=lambda s: datetime.datetime.strptime(s, '%Y-%m-%d').date(), help='Min date to import')
    parser.add_argument('-n', '--min_line', type=int, help='Line number to start at')
    parser.add_argument('-e', '--exclude_exdate_transactions_before_min_date', type=bool, action=argparse.BooleanOptionalAction, help='Exclude exdate transactions before min date')
    parser.add_argument('-x', '--max_line', type=int, help='Line number to finish at')
    parser.add_argument('-v', '--verbose', type=bool, action=argparse.BooleanOptionalAction, help='Output curl requests')
    
    opening_balance_group = parser.add_argument_group('opening balance options')
    opening_balance_group.add_argument('-ob', '--opening_balance_on', type=lambda s: datetime.datetime.strptime(s, '%Y-%m-%d').date(), help='Generate opening balances on this date')
    opening_balance_group.add_argument('-obf', '--opening_balance_from', type=str, required=False, help='The portfolio to calculate opening balances from')
    opening_balance_group.add_argument('-ef', '--exchange_rates_file_name', type=str, required=False, help='The exchange rates file name')

    args = parser.parse_args()
    
    ob_args = [args.opening_balance_on, args.opening_balance_from, args.exchange_rates_file_name]
    if any(ob_args) and not all(ob_args):
        parser.error("opening balance options must be used together")
    print(f"{args}")
    api_client = SharesightApiClient(args.client_id, args.client_secret, args.verbose)
    csv_importer = SharesightCsvImporter(api_client)
    opening_balance = (
        OpeningBalanceOptions(
            valuation_date=args.opening_balance_on,
            source_portfolio_name=args.opening_balance_from,
            exchange_rates_file_path=args.exchange_rates_file_name,
        )
        if args.opening_balance_on else None
    )
    options = ImportOptions(
        delete_existing=args.delete_existing,
        min_date=args.min_date,
        exclude_exdate_transactions_before_min_date=(
            args.exclude_exdate_transactions_before_min_date
        ),
        min_line=args.min_line,
        max_line=args.max_line,
        prices_file_path=args.prices_file_name,
        opening_balance=opening_balance,
    )
    csv_importer.import_file(
        args.file_name, args.portfolio_name, args.country_code, options
    )
main()
