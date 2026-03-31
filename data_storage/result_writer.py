from __future__ import annotations
import csv
from pathlib import Path
from data_storage.column_store import ColumnStore, ColumnStoreError
from data_storage.models import OUTPUT_HEADERS, OutputRow, QueryResult, sort_query_results


class ResultWriterError(ValueError):
    # raised when the query result cant be converted or written properly.
    pass

def build_output_rows(
    store: ColumnStore,
    results: list[QueryResult],
) -> list[OutputRow]:
    # converts the query results into the final output rows. results will be sorted by increasing x and then increasing y
    sorted_results = sort_query_results(results)
    output_rows: list[OutputRow] = []

    for result in sorted_results:
        if result.has_match:
            output_rows.append(_build_matched_output_row(store, result))
        else:
            output_rows.append(_build_no_result_output_row(result))

    return output_rows


def write_scan_result_csv(
    output_path: str | Path,
    store: ColumnStore,
    results: list[QueryResult],
) -> Path:
    # writes the final scan result csv file
    path = Path(output_path)
    output_rows = build_output_rows(store, results)

    try:
        with path.open("w", newline="", encoding="utf-8") as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(OUTPUT_HEADERS)
            for output_row in output_rows:
                writer.writerow(output_row.to_csv_row())
    except OSError as exc:
        raise ResultWriterError(f"Failed to write output CSV: {path}") from exc

    return path


def _build_matched_output_row(store: ColumnStore, result: QueryResult) -> OutputRow:
    matched_row_id = result.matched_row_id
    if matched_row_id is None:
        raise ResultWriterError(
            "Matched output row requested for a QueryResult with no row."
        )

    try:
        row = store.get_row_view(matched_row_id)
    except ColumnStoreError as exc:
        raise ResultWriterError(
            f"QueryResult points to invalid row_id {matched_row_id}."
        ) from exc

    return OutputRow(
        pair=result.pair_label,
        year=str(row.year),
        month=f"{row.month_number:02d}",
        town=row.town_raw,
        block=row.block,
        floor_area=_format_floor_area(row.floor_area_sqm),
        flat_model=row.flat_model,
        lease_commence_date=str(row.lease_commence_year),
        price_per_square_meter=str(round(row.price_per_sqm)),
    )


def _build_no_result_output_row(result: QueryResult) -> OutputRow:
    return OutputRow(
        pair=result.pair_label,
        year="No result",
        month="No result",
        town="No result",
        block="No result",
        floor_area="No result",
        flat_model="No result",
        lease_commence_date="No result",
        price_per_square_meter="No result",
    )


def _format_floor_area(value: float) -> str:
    # keeps integer looking values clean while perserving the real decimals, for e.g : 126.0 becomes 126 and 60.3 becomes 60.3
    if value.is_integer():
        return str(int(value))
    return format(value, "g")
