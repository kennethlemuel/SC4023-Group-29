from __future__ import annotations

import csv
from pathlib import Path

from data_storage.column_store import ColumnStore, ColumnStoreError
from data_storage.month_parser import MonthParseError, ParsedMonth, parse_month_value


EXPECTED_HEADERS = [
    "month",
    "town",
    "flat_type",
    "block",
    "street_name",
    "storey_range",
    "floor_area_sqm",
    "flat_model",
    "lease_commence_date",
    "resale_price",
]


class CSVLoaderError(ValueError):
    # only raised when the input CSV file is missing, empty, or deformed of any kind. 
    pass


def load_resale_csv(csv_path: str | Path) -> ColumnStore:
    # function that loads resale csv into col oriented store
    path = Path(csv_path)
    if not path.exists():
        raise CSVLoaderError(f"CSV file does not exist: {path}")
    if not path.is_file():
        raise CSVLoaderError(f"CSV path is not a file: {path}")

    store = ColumnStore()

    try:
        with path.open(newline="", encoding="utf-8-sig") as csv_file:
            reader = csv.DictReader(csv_file)
            _validate_headers(reader.fieldnames)

            for csv_row_number, raw_row in enumerate(reader, start=2):
                parsed_row = _parse_csv_row(raw_row, csv_row_number)
                try:
                    store.append_row(**parsed_row)
                except ColumnStoreError as exc:
                    raise CSVLoaderError(
                        f"Invalid values in CSV row {csv_row_number}: {exc}"
                    ) from exc
    except OSError as exc:
        raise CSVLoaderError(f"Failed to read CSV file: {path}") from exc

    store.validate_alignment()
    return store


def _validate_headers(fieldnames: list[str] | None) -> None:
    if fieldnames is None:
        raise CSVLoaderError("CSV file is empty or missing the header row.")
    if fieldnames != EXPECTED_HEADERS:
        raise CSVLoaderError(
            "CSV headers do not match the expected columns. "
            f"Expected {EXPECTED_HEADERS}, got {fieldnames}."
        )


def _parse_csv_row(raw_row: dict[str, str | None], csv_row_number: int) -> dict[str, object]:
    month_raw = _require_csv_text(raw_row, "month", csv_row_number)
    parsed_month = _parse_month(month_raw, csv_row_number)

    return {
        "month_raw": month_raw,
        "year": parsed_month.year,
        "month_number": parsed_month.month_number,
        "town_raw": _require_csv_text(raw_row, "town", csv_row_number),
        "flat_type": _require_csv_text(raw_row, "flat_type", csv_row_number),
        "block": _require_csv_text(raw_row, "block", csv_row_number),
        "street_name": _require_csv_text(raw_row, "street_name", csv_row_number),
        "storey_range": _require_csv_text(raw_row, "storey_range", csv_row_number),
        "floor_area_sqm": _parse_float_field(
            raw_row, "floor_area_sqm", csv_row_number
        ),
        "flat_model": _require_csv_text(raw_row, "flat_model", csv_row_number),
        "lease_commence_raw": _require_csv_text(
            raw_row, "lease_commence_date", csv_row_number
        ),
        "lease_commence_year": _parse_int_field(
            raw_row, "lease_commence_date", csv_row_number
        ),
        "resale_price": _parse_float_field(raw_row, "resale_price", csv_row_number),
    }


def _parse_month(month_raw: str, csv_row_number: int) -> ParsedMonth:
    try:
        return parse_month_value(month_raw)
    except MonthParseError as exc:
        raise CSVLoaderError(
            f"Invalid month value in CSV row {csv_row_number}: {exc}"
        ) from exc


def _require_csv_text(
    raw_row: dict[str, str | None], field_name: str, csv_row_number: int
) -> str:
    value = raw_row.get(field_name)
    if value is None:
        raise CSVLoaderError(
            f"Missing field {field_name!r} in CSV row {csv_row_number}."
        )

    clean_value = value.strip()
    if not clean_value:
        raise CSVLoaderError(
            f"Field {field_name!r} is empty in CSV row {csv_row_number}."
        )
    return clean_value


def _parse_float_field(
    raw_row: dict[str, str | None], field_name: str, csv_row_number: int
) -> float:
    text_value = _require_csv_text(raw_row, field_name, csv_row_number)
    try:
        return float(text_value)
    except ValueError as exc:
        raise CSVLoaderError(
            f"Field {field_name!r} is not a valid number in CSV row "
            f"{csv_row_number}: {text_value!r}."
        ) from exc


def _parse_int_field(
    raw_row: dict[str, str | None], field_name: str, csv_row_number: int
) -> int:
    text_value = _require_csv_text(raw_row, field_name, csv_row_number)
    try:
        return int(text_value)
    except ValueError as exc:
        raise CSVLoaderError(
            f"Field {field_name!r} is not a valid integer in CSV row "
            f"{csv_row_number}: {text_value!r}."
        ) from exc
