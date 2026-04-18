from __future__ import annotations

from dataclasses import dataclass

DIGIT_TO_TOWN: dict[str, str] = {
    "0": "BEDOK",
    "1": "BUKIT PANJANG",
    "2": "CLEMENTI",
    "3": "CHOA CHU KANG",
    "4": "HOUGANG",
    "5": "JURONG WEST",
    "6": "PASIR RIS",
    "7": "TAMPINES",
    "8": "WOODLANDS",
    "9": "YISHUN",
}

DEFAULT_MATRIC_NUMBER = "U2222559E"

X_MIN: int = 1
X_MAX: int = 8
Y_MIN: int = 80
Y_MAX: int = 150
PRICE_PER_SQM_THRESHOLD: float = 4725.0


class QueryConfigError(ValueError):
    pass


@dataclass(frozen=True)
class QueryConfig:
    matric_number: str
    target_year: int
    start_month: int
    matched_towns: frozenset[str]
    x_min: int = X_MIN
    x_max: int = X_MAX
    y_min: int = Y_MIN
    y_max: int = Y_MAX
    price_per_sqm_threshold: float = PRICE_PER_SQM_THRESHOLD

    @property
    def output_filename(self) -> str:
        return f"ScanResult_{self.matric_number}.csv"


def build_query_config(matric_number: str = DEFAULT_MATRIC_NUMBER) -> QueryConfig:
    clean_matric = _normalize_matric_number(matric_number)
    digits = [char for char in clean_matric if char.isdigit()]
    if len(digits) < 2:
        raise QueryConfigError(
            "Matriculation number must contain at least two digits."
        )

    target_year = _target_year_from_digit(digits[-1])
    start_month = _start_month_from_digit(digits[-2])
    matched_towns = frozenset(DIGIT_TO_TOWN[digit] for digit in set(digits))

    return QueryConfig(
        matric_number=clean_matric,
        target_year=target_year,
        start_month=start_month,
        matched_towns=matched_towns,
    )


def _normalize_matric_number(matric_number: str) -> str:
    if matric_number is None:
        raise QueryConfigError("Matriculation number is required.")
    if not isinstance(matric_number, str):
        raise QueryConfigError(
            "Matriculation number must be text, "
            f"got {type(matric_number).__name__}."
        )

    clean_matric = matric_number.strip().upper()
    if not clean_matric:
        raise QueryConfigError("Matriculation number cannot be empty.")
    return clean_matric


def _start_month_from_digit(digit: str) -> int:
    if digit == "0":
        return 10
    month = int(digit)
    if not 1 <= month <= 9:
        raise QueryConfigError(f"Invalid start month digit: {digit!r}.")
    return month


def _target_year_from_digit(digit: str) -> int:
    year_digit = int(digit)
    if year_digit <= 4:
        return 2020 + year_digit
    return 2010 + year_digit
