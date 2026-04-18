from __future__ import annotations

import argparse
from pathlib import Path

from data_processing.config import DEFAULT_MATRIC_NUMBER, build_query_config
from data_processing.query import query
from data_storage.csv_loader import load_resale_csv
from data_storage.result_writer import write_scan_result_csv


DEFAULT_INPUT_CSV = "ResalePricesSingapore.csv"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate HDB resale scan results for a matriculation number."
    )
    parser.add_argument(
        "csv_path",
        nargs="?",
        default=DEFAULT_INPUT_CSV,
        help=f"Input CSV path. Defaults to {DEFAULT_INPUT_CSV}.",
    )
    parser.add_argument(
        "matric_number",
        nargs="?",
        default=DEFAULT_MATRIC_NUMBER,
        help=f"Matriculation number. Defaults to {DEFAULT_MATRIC_NUMBER}.",
    )
    parser.add_argument(
        "-o",
        "--output",
        default=None,
        help="Output CSV path. Defaults to ScanResult_<MatricNum>.csv.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = build_query_config(args.matric_number)
    output_path = Path(args.output) if args.output else Path(config.output_filename)

    store = load_resale_csv(args.csv_path)
    results = query(store, config)
    write_scan_result_csv(output_path, store, results)


if __name__ == "__main__":
    main()
