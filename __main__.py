import datetime
from os import getenv
import argparse
from sharesight_api_client import SharesightApiClient
from sharesight_csv_importer import SharesightCsvImporter

def main():
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('--client_id', default=getenv('SHARESIGHT_CLIENT_ID'), type=str, required=False, help=argparse.SUPPRESS)
    parser.add_argument('--client_secret', default=getenv('SHARESIGHT_CLIENT_SECRET'), type=str, required=False, help=argparse.SUPPRESS)
    parser.add_argument('-p', '--portfolio_name', type=str, required=True, help='The portfolio name')
    parser.add_argument('-f', '--file_name', type=str, required=True, help='The file name')
    parser.add_argument('-c', '--country_code', type=str, required=True, help='The country code')
    parser.add_argument('-r', '--delete_existing', type=bool, action=argparse.BooleanOptionalAction, help='Remove the portfolio')
    parser.add_argument('-i', '--use_seperate_income_account', type=bool, action=argparse.BooleanOptionalAction, help='Use a seperate cash account for income')
    parser.add_argument('-u', '--use_usd_eur_account', type=bool, action=argparse.BooleanOptionalAction, help='Use GBP, EUR and USD accounts')
    parser.add_argument('-d', '--min_date', type=lambda s: datetime.datetime.strptime(s, '%Y-%m-%d').date(), help='Min date to import')
    parser.add_argument('-n', '--min_line', type=int, help='Line number to start at')
    parser.add_argument('-x', '--max_line', type=int, help='Line number to finish at')
    parser.add_argument('-v', '--verbose', type=bool, action=argparse.BooleanOptionalAction, help='Output curl requests')

    args = parser.parse_args()
    print(f"{args}")
    api_client = SharesightApiClient(args.client_id, args.client_secret, args.verbose)
    csv_importer = SharesightCsvImporter(api_client)
    csv_importer.import_file(args.file_name, args.portfolio_name, args.country_code, args.use_seperate_income_account, args.use_usd_eur_account, args.delete_existing, args.min_date, args.min_line, args.max_line) 
main()
