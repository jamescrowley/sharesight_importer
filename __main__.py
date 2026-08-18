import argparse
import datetime
from os import getenv

from sharesight_api_client import SharesightApiClient
from sharesight_csv_importer import SharesightCsvImporter
from sharesight_import_options import ImportOptions
from sharesight_opening_balances import OpeningBalanceExporter


def _date(value):
    return datetime.datetime.strptime(value, "%Y-%m-%d").date()


def build_parser():
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--client-id", default=getenv("SHARESIGHT_CLIENT_ID"), help=argparse.SUPPRESS)
    common.add_argument("--client-secret", default=getenv("SHARESIGHT_CLIENT_SECRET"), help=argparse.SUPPRESS)
    common.add_argument("-v", "--verbose", action=argparse.BooleanOptionalAction)

    parser = argparse.ArgumentParser(description="Import transactions into Sharesight")
    commands = parser.add_subparsers(dest="command", required=True)

    import_parser = commands.add_parser("import", parents=[common])
    import_parser.add_argument("-p", "--portfolio-name", required=True)
    import_parser.add_argument("-f", "--file-name", required=True)
    import_parser.add_argument("-pf", "--prices-file-name")
    import_parser.add_argument("-obf", "--opening-balances-file-name")
    import_parser.add_argument("-c", "--country-code", choices=("AU", "GB"), required=True)
    import_parser.add_argument("-r", "--delete-existing", action=argparse.BooleanOptionalAction)
    import_parser.add_argument("-d", "--min-date", type=_date)
    import_parser.add_argument("-n", "--min-line", type=int)
    import_parser.add_argument(
        "-e", "--exclude-exdate-transactions-before-min-date",
        action=argparse.BooleanOptionalAction,
    )
    import_parser.add_argument("-x", "--max-line", type=int)

    export_parser = commands.add_parser("export-opening-balances", parents=[common])
    export_parser.add_argument("--source-portfolio-name", required=True)
    export_parser.add_argument("--valuation-date", type=_date, required=True)
    export_parser.add_argument("--exchange-rates-file-name", required=True)
    export_parser.add_argument("--output-file", required=True)
    export_parser.add_argument("--overwrite", action="store_true")
    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)
    api_client = SharesightApiClient(args.client_id, args.client_secret, args.verbose)
    if args.command == "export-opening-balances":
        return OpeningBalanceExporter(api_client).export(
            args.source_portfolio_name,
            args.valuation_date,
            args.exchange_rates_file_name,
            args.output_file,
            args.overwrite,
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
        opening_balances_file_path=args.opening_balances_file_name,
    )
    return SharesightCsvImporter(api_client).import_file(
        args.file_name, args.portfolio_name, args.country_code, options
    )


if __name__ == "__main__":
    main()
