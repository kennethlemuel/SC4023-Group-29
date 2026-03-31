from __future__ import annotations
import re
from dataclasses import dataclass

_MONTH_NAME_TO_NUMBER = {
    "JAN": 1,
    "FEB": 2,
    "MAR": 3,
    "APR": 4,
    "MAY": 5,
    "JUN": 6,
    "JUL": 7,
    "AUG": 8,
    "SEP": 9,
    "OCT": 10,
    "NOV": 11,
    "DEC": 12,
}

_MON_YY_PATTERN = re.compile(r"^([A-Za-z]{3})-(\d{2})$")
_YEAR_MONTH_PATTERN = re.compile(r"^(\d{4})-(\d{1,2})$")

class MonthParseError(ValueError):
    # raised when a month value cant be parsed safely.
    pass

@dataclass(frozen=True)
class ParsedMonth:
    # stores the parsed year and month number from one raw month value.

    raw_value: str
    year: int
    month_number: int

    @property
    def month_key(self) -> int:
        # function returns an integer key like 201501, for comparisons and range filtering.
        return self.year * 100 + self.month_number

    @property
    def month_two_digits(self) -> str:
        # returns month in an output friendly MM format.
        return f"{self.month_number:02d}"


def parse_month_value(raw_value: str) -> ParsedMonth:
    # parses one month cell from the resale csv.
    # support formats examples: - Mon-YY (e.g Jan-15) - YYYY-MM (e.g 2015-01)
    value = _normalize_month_text(raw_value)
    if not value:
        raise MonthParseError("Month value is empty.")

    parsed = _parse_mon_yy(value)
    if parsed is not None:
        return parsed

    parsed = _parse_year_month(value)
    if parsed is not None:
        return parsed

    raise MonthParseError(
        f"Unsupported month format: {raw_value!r}. "
        "Expected 'Mon-YY' or 'YYYY-MM'."
    )


def month_key(year: int, month_number: int) -> int:
    # builds a comparable integer key from the year and month, e.g: month_key(2015,1) becomes 201501
    _validate_year(year)
    _validate_month_number(month_number)
    return year * 100 + month_number


def _parse_mon_yy(value: str) -> ParsedMonth | None:
    match = _MON_YY_PATTERN.fullmatch(value)
    if match is None:
        return None

    month_name = match.group(1).upper()
    year_suffix = int(match.group(2))

    if month_name not in _MONTH_NAME_TO_NUMBER:
        raise MonthParseError(f"Unknown month name: {match.group(1)!r}.")

    month_number = _MONTH_NAME_TO_NUMBER[month_name]
    year = _expand_two_digit_year(year_suffix)

    return ParsedMonth(raw_value=value, year=year, month_number=month_number)


def _parse_year_month(value: str) -> ParsedMonth | None:
    match = _YEAR_MONTH_PATTERN.fullmatch(value)
    if match is None:
        return None

    year = int(match.group(1))
    month_number = int(match.group(2))
    _validate_year(year)
    _validate_month_number(month_number)

    return ParsedMonth(raw_value=value, year=year, month_number=month_number)


def _expand_two_digit_year(year_suffix: int) -> int:
    # converts two digit year to four digit year 
    if 0 <= year_suffix <= 69:
        return 2000 + year_suffix
    return 1900 + year_suffix


def _normalize_month_text(raw_value: str) -> str:
    # converts inpu into a stripped string for normalization.
    if raw_value is None:
        raise MonthParseError("Month value is missing (None).")
    if not isinstance(raw_value, str):
        raise MonthParseError(
            f"Month value must be a string, instead got {type(raw_value).__name__}."
        )
    return raw_value.strip()


def _validate_year(year: int) -> None:
    # rejects invalid years while still being flexible about dataset range, this can be changed up to us
    if year < 1000 or year > 9999:
        raise MonthParseError(f"Year must be four digits, got {year}.")


def _validate_month_number(month_number: int) -> None:
    if not 1 <= month_number <= 12:
        raise MonthParseError(f"Month number must be between 1 and 12, instead got {month_number}.")
