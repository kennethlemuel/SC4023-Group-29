from __future__ import annotations

from data_storage.month_parser import month_key

from data_processing.config import START_MONTH, TARGET_YEAR


def _end_month(x: int) -> tuple[int, int]:
    #this function calculates the end month and year given x months from the start month and year.
    end_month = START_MONTH + (x - 1)
    end_year = TARGET_YEAR + (end_month - 1) // 12
    end_month = ((end_month - 1) % 12) + 1

    return end_year, end_month

def build_month_range(x: int) -> tuple[int, int]:
    #this function builds month_key range from the start to the x-th month.
    start_month_key = month_key(TARGET_YEAR, START_MONTH)
    end_year, end_month = _end_month(x)
    end_month_key = month_key(end_year, end_month)

    return start_month_key, end_month_key
