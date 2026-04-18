from __future__ import annotations

from data_storage.month_parser import month_key

from data_processing.config import QueryConfig


def _end_month(config: QueryConfig, x: int) -> tuple[int, int]:
    # Calculates the inclusive end month for a query spanning x months.
    end_month = config.start_month + (x - 1)
    end_year = config.target_year + (end_month - 1) // 12
    end_month = ((end_month - 1) % 12) + 1

    return end_year, end_month


def build_month_range(config: QueryConfig, x: int) -> tuple[int, int]:
    # Builds a comparable month_key range from the start month to the x-th month.
    start_month_key = month_key(config.target_year, config.start_month)
    end_year, end_month = _end_month(config, x)
    end_month_key = month_key(end_year, end_month)

    return start_month_key, end_month_key
