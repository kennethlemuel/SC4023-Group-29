from __future__ import annotations

from data_storage.column_store import ColumnStore
from data_storage.models import QueryResult, sort_query_results

from data_processing.config import QueryConfig, build_query_config
from data_processing.month_range import build_month_range


def filter_sort_town_rows(store: ColumnStore, config: QueryConfig) -> list[int]:
    # Returns row ids in matched towns, sorted by floor area descending.
    towns = store.town_norm
    areas = store.floor_area_sqm

    matched_town_row_ids = [
        i for i, town in enumerate(towns)
        if town in config.matched_towns
    ]

    matched_town_row_ids.sort(key=lambda i: areas[i], reverse=True)
    return matched_town_row_ids

def filter_month_range(
    store: ColumnStore,
    row_ids: list[int],
    start_key: int,
    end_key: int,
) -> list[int]:
    # Filters only rows in the target month range.
    month_keys = store.month_key

    return [
        i for i in row_ids
        if start_key <= month_keys[i] <= end_key
    ]

def find_area_end(
    store: ColumnStore,
    row_ids: list[int],
    current_end: int,
    min_area: float,
) -> int:
    # Finds the end index where floor area is still at least min_area.
    areas = store.floor_area_sqm
    
    end = current_end
    while end > 0 and areas[row_ids[end - 1]] < min_area:
        end -= 1
    return end


def find_min_price_row(
    store: ColumnStore,
    row_ids: list[int],
    valid_end: int,
    config: QueryConfig,
) -> int | None:
    # Finds the row id with the minimum price per sqm among the filtered rows.
    if valid_end == 0:
        return None

    prices = store.price_per_sqm

    min_price_row_id: int | None = None
    min_price: float = float("inf")

    for row_id in row_ids[:valid_end]:
        price = prices[row_id]
        if price < min_price:
            min_price = price
            min_price_row_id = row_id

    if (
        min_price_row_id is not None
        and min_price > config.price_per_sqm_threshold
    ):
        return None

    return min_price_row_id


def query(store: ColumnStore, config: QueryConfig | None = None) -> list[QueryResult]:
    # Implements the main query logic.
    if config is None:
        config = build_query_config()

    town_row_ids = filter_sort_town_rows(store, config)

    results: list[QueryResult] = []

    for x in range(config.x_min, config.x_max + 1):
        start_key, end_key = build_month_range(config, x)
        town_month_row_ids = filter_month_range(store, town_row_ids, start_key, end_key)

        valid_end = len(town_month_row_ids)

        for y in range(config.y_min, config.y_max + 1):
            valid_end = find_area_end(store, town_month_row_ids, valid_end, float(y))

            matched_row_id = find_min_price_row(
                store,
                town_month_row_ids,
                valid_end,
                config,
            )
            results.append(QueryResult(x=x, y=y, matched_row_id=matched_row_id))

    return sort_query_results(results)
