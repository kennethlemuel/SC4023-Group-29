from __future__ import annotations
from dataclasses import dataclass

OUTPUT_HEADERS = [
    "(x, y)",
    "Year",
    "Month",
    "Town",
    "Block",
    "Floor_Area",
    "Flat_Model",
    "Lease_Commence_Date",
    "Price_Per_Square_Meter",
]

class ModelValidationError(ValueError):
    # only raised when a result or output row causes an error
    pass


@dataclass(frozen=True)
class QueryResult:
    # represents one query result for an x,y pair, matched_row_id will be None when no matched rows
    x: int
    y: int
    matched_row_id: int | None

    def __post_init__(self) -> None:
        object.__setattr__(self, "x", _validate_x(self.x))
        object.__setattr__(self, "y", _validate_y(self.y))
        object.__setattr__(
            self, "matched_row_id", _validate_matched_row_id(self.matched_row_id)
        )

    @property
    def has_match(self) -> bool:
        return self.matched_row_id is not None

    @property
    def pair_label(self) -> str:
        return f"({self.x}, {self.y})"

    @classmethod
    def no_result(cls, x: int, y: int) -> "QueryResult":
        # for handling no-result case.
        return cls(x=x, y=y, matched_row_id=None)


@dataclass(frozen=True)
class OutputRow:
    # stores one final output row  in the format used for csv writing.
    pair: str
    year: str
    month: str
    town: str
    block: str
    floor_area: str
    flat_model: str
    lease_commence_date: str
    price_per_square_meter: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "pair", _require_output_text("pair", self.pair))
        object.__setattr__(self, "year", _require_output_text("year", self.year))
        object.__setattr__(self, "month", _require_output_text("month", self.month))
        object.__setattr__(self, "town", _require_output_text("town", self.town))
        object.__setattr__(self, "block", _require_output_text("block", self.block))
        object.__setattr__(
            self, "floor_area", _require_output_text("floor_area", self.floor_area)
        )
        object.__setattr__(
            self, "flat_model", _require_output_text("flat_model", self.flat_model)
        )
        object.__setattr__(
            self,
            "lease_commence_date",
            _require_output_text("lease_commence_date", self.lease_commence_date),
        )
        object.__setattr__(
            self,
            "price_per_square_meter",
            _require_output_text(
                "price_per_square_meter", self.price_per_square_meter
            ),
        )

    def to_csv_row(self) -> list[str]:
        # function returns row in required csv col order.
        return [
            self.pair,
            self.year,
            self.month,
            self.town,
            self.block,
            self.floor_area,
            self.flat_model,
            self.lease_commence_date,
            self.price_per_square_meter,
        ]


def sort_query_results(results: list[QueryResult]) -> list[QueryResult]:
    # function that returns the results, it will be sorted by increasing x and then increasing y.
    return sorted(results, key=lambda result: (result.x, result.y))


def _validate_x(value: int) -> int:
    if not isinstance(value, int):
        raise ModelValidationError(f"x must be an integer, instead got {type(value).__name__}.")
    if not 1 <= value <= 8:
        raise ModelValidationError(f"x must be between 1 and 8, instead got {value}.")
    return value


def _validate_y(value: int) -> int:
    if not isinstance(value, int):
        raise ModelValidationError(f"y must be an integer, insteadgot {type(value).__name__}.")
    if not 80 <= value <= 150:
        raise ModelValidationError(f"y must be between 80 and 150, instead got {value}.")
    return value


def _validate_matched_row_id(value: int | None) -> int | None:
    if value is None:
        return None
    if not isinstance(value, int):
        raise ModelValidationError(
            "matched_row_id must be an int or None, "
            f"got {type(value).__name__}."
        )
    if value < 0:
        raise ModelValidationError(
            f"matched_row_id must be non-negative, instead got {value}."
        )
    return value


def _require_output_text(field_name: str, value: str) -> str:
    if value is None:
        raise ModelValidationError(f"{field_name} cannot be None.")
    if not isinstance(value, str):
        raise ModelValidationError(
            f"{field_name} must be a string, instead got {type(value).__name__}."
        )
    clean_value = value.strip()
    if not clean_value:
        raise ModelValidationError(f"{field_name} cannot be empty.")
    return clean_value
