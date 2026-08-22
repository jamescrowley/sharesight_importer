import argparse
import datetime
import os
import sys

from sharesight_api_client import SharesightApiClient
from sharesight_csv_importer import SharesightCsvImporter
from sharesight_import_options import ImportOptions
from sharesight_residency_reset import ResidencyResetExporter
from sharesight_schema import report_unknown_columns, validate_import_files


def _date(value):
    try:
        return datetime.datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as error:
        raise argparse.ArgumentTypeError("expected YYYY-MM-DD") from error


def _currency(value):
    value = value.upper()
    if len(value) != 3 or not value.isalpha():
        raise argparse.ArgumentTypeError("expected a three-letter ISO 4217 code")
    return value


def _country(value):
    value = value.upper()
    if len(value) != 2 or not value.isalpha():
        raise argparse.ArgumentTypeError("expected a two-letter country code")
    return value


def _suffix(value):
    if not value.strip():
        raise argparse.ArgumentTypeError("managed instrument suffix cannot be blank")
    return value


def build_parser():
    parser = argparse.ArgumentParser(
        prog="sharesight-importer",
        description="Validate and import canonical transaction CSVs into Sharesight",
    )
    commands = parser.add_subparsers(dest="command", required=True)

    validate = commands.add_parser(
        "validate", help="validate CSV inputs without credentials or network"
    )
    _add_input_arguments(validate)

    importer = commands.add_parser("import", help="import a canonical CSV")
    _add_input_arguments(importer)
    importer.add_argument("--portfolio-name", required=True)
    importer.add_argument(
        "--country-code", type=_country, help="required only with --create-portfolio"
    )
    importer.add_argument("--create-portfolio", action="store_true")
    importer.add_argument("--delete-existing", action="store_true")
    importer.add_argument("--dry-run", action="store_true")
    importer.add_argument("--yes", action="store_true", help="bypass destructive confirmation")
    importer.add_argument("--no-resync-cash-accounts", action="store_true")
    importer.add_argument("--ignore-retained-income", action="store_true")
    importer.add_argument("--min-date", type=_date)
    importer.add_argument("--min-line", type=int)
    importer.add_argument("--max-line", type=int)
    importer.add_argument("--exclude-exdate-transactions-before-min-date", action="store_true")
    importer.add_argument("--managed-instrument-name-suffix", default="(AUTO)", type=_suffix)
    importer.add_argument("-v", "--verbose", action="store_true")

    reset = commands.add_parser(
        "export-residency-reset", help="export an Australian residency-reset SELL/BUY CSV"
    )
    reset.add_argument("--source-portfolio-name", required=True)
    reset.add_argument("--portfolio-currency", required=True, type=_currency, choices=("AUD",))
    reset.add_argument("--residency-date", type=_date, required=True)
    reset.add_argument("--exchange-rates-file-name", required=True)
    reset.add_argument("--output-file", required=True)
    reset.add_argument("--overwrite", action="store_true")
    reset.add_argument("--managed-instrument-name-suffix", default="(AUTO)", type=_suffix)
    reset.add_argument("-v", "--verbose", action="store_true")
    return parser


def _add_input_arguments(parser):
    parser.add_argument("--file-name", required=True)
    parser.add_argument("--portfolio-currency", required=True, type=_currency)
    parser.add_argument("--prices-file-name")
    parser.add_argument("--residency-reset-file-name")


def main(argv=None):
    try:
        args = build_parser().parse_args(argv)
        if args.command == "validate":
            transactions = validate_import_files(
                args.file_name,
                args.portfolio_currency,
                args.prices_file_name,
                args.residency_reset_file_name,
            )
            report_unknown_columns(args.file_name)
            print(f"Validation succeeded: {len(transactions)} transaction group(s)")
            return 0

        if args.command == "import" and args.create_portfolio and not args.country_code:
            raise ValueError("--country-code is required with --create-portfolio")
        if args.command == "import" and args.country_code and not args.create_portfolio:
            raise ValueError("--country-code is only valid with --create-portfolio")
        client_id, client_secret = _credentials()
        api_client = SharesightApiClient(client_id, client_secret, args.verbose)
        if args.command == "export-residency-reset":
            ResidencyResetExporter(api_client).export(
                args.source_portfolio_name,
                args.residency_date,
                args.exchange_rates_file_name,
                args.output_file,
                args.overwrite,
                args.portfolio_currency,
                args.managed_instrument_name_suffix,
            )
            return 0

        options = ImportOptions(
            delete_existing=args.delete_existing,
            min_date=args.min_date,
            exclude_exdate_transactions_before_min_date=args.exclude_exdate_transactions_before_min_date,
            min_line=args.min_line,
            max_line=args.max_line,
            prices_file_path=args.prices_file_name,
            residency_reset_file_path=args.residency_reset_file_name,
            ignore_retained_income=args.ignore_retained_income,
            create_portfolio=args.create_portfolio,
            dry_run=args.dry_run,
            yes=args.yes,
            resync_cash_accounts=not args.no_resync_cash_accounts,
            managed_instrument_name_suffix=args.managed_instrument_name_suffix,
        )
        result = SharesightCsvImporter(api_client).import_file(
            args.file_name,
            args.portfolio_name,
            args.portfolio_currency,
            options=options,
            country_code=args.country_code,
        )
        return 0 if result is not False else 1
    except (ValueError, RuntimeError, FileNotFoundError, FileExistsError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 2
    except Exception as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1


def _credentials():
    client_id = os.getenv("SHARESIGHT_CLIENT_ID")
    client_secret = os.getenv("SHARESIGHT_CLIENT_SECRET")
    missing = [
        name
        for name, value in (
            ("SHARESIGHT_CLIENT_ID", client_id),
            ("SHARESIGHT_CLIENT_SECRET", client_secret),
        )
        if not value
    ]
    if missing:
        raise ValueError(f"Missing required environment variable(s): {', '.join(missing)}")
    return client_id, client_secret
