from __future__ import annotations

from dataclasses import dataclass, field

from data_storage.month_parser import month_key


class ColumnStoreError(ValueError):
    # this is only raised when invalid data is appended or alignment is broken.
    pass

@dataclass(frozen=True)
class RowView:
    # a lightweight read-only view of one row.

    row_id: int
    month_raw: str
    year: int
    month_number: int
    month_key: int
    town_raw: str
    town_norm: str
    flat_type: str
    block: str
    street_name: str
    storey_range: str
    floor_area_sqm: float
    flat_model: str
    lease_commence_raw: str
    lease_commence_year: int
    resale_price: float
    price_per_sqm: float


@dataclass
class ColumnStore:
    # the ture column-store rep of the dataset. 
    # the raw columns here are kept for better tracability and future output. and the helper cols are stored seperately for query.

    month_raw: list[str] = field(default_factory=list)
    town_raw: list[str] = field(default_factory=list)
    flat_type: list[str] = field(default_factory=list)
    block: list[str] = field(default_factory=list)
    street_name: list[str] = field(default_factory=list)
    storey_range: list[str] = field(default_factory=list)
    flat_model: list[str] = field(default_factory=list)
    lease_commence_raw: list[str] = field(default_factory=list)

    town_norm: list[str] = field(default_factory=list)
    year: list[int] = field(default_factory=list)
    month_number: list[int] = field(default_factory=list)
    month_key: list[int] = field(default_factory=list)
    floor_area_sqm: list[float] = field(default_factory=list)
    lease_commence_year: list[int] = field(default_factory=list)
    resale_price: list[float] = field(default_factory=list)
    price_per_sqm: list[float] = field(default_factory=list)

    @property
    def row_count(self) -> int:
        # function that returns no of stored rows.
        return len(self.month_raw)

    def __len__(self) -> int:
        return self.row_count

    def iter_row_ids(self) -> range:
        # exposes row ids for scanning
        return range(self.row_count)

    def append_row(
        self,
        *,
        month_raw: str,
        year: int,
        month_number: int,
        town_raw: str,
        flat_type: str,
        block: str,
        street_name: str,
        storey_range: str,
        floor_area_sqm: float,
        flat_model: str,
        lease_commence_raw: str,
        lease_commence_year: int,
        resale_price: float,
    ) -> int:
        # this appends one fully parsed transaction to the store n returns new row_id assigned to appended row.
        self.validate_alignment()

        clean_month_raw = _require_text("month_raw", month_raw)
        clean_town_raw = _require_text("town_raw", town_raw)
        clean_flat_type = _require_text("flat_type", flat_type)
        clean_block = _require_text("block", block)
        clean_street_name = _require_text("street_name", street_name)
        clean_storey_range = _require_text("storey_range", storey_range)
        clean_flat_model = _require_text("flat_model", flat_model)
        clean_lease_raw = _require_text("lease_commence_raw", lease_commence_raw)

        clean_year = _coerce_year(year)
        clean_month_number = _coerce_month_number(month_number)
        clean_month_key = month_key(clean_year, clean_month_number)
        clean_town_norm = _normalize_town(clean_town_raw)

        clean_floor_area = _coerce_positive_float("floor_area_sqm", floor_area_sqm)
        clean_lease_year = _coerce_year(lease_commence_year)
        clean_resale_price = _coerce_positive_float("resale_price", resale_price)
        clean_price_per_sqm = clean_resale_price / clean_floor_area

        self.month_raw.append(clean_month_raw)
        self.town_raw.append(clean_town_raw)
        self.flat_type.append(clean_flat_type)
        self.block.append(clean_block)
        self.street_name.append(clean_street_name)
        self.storey_range.append(clean_storey_range)
        self.flat_model.append(clean_flat_model)
        self.lease_commence_raw.append(clean_lease_raw)

        self.town_norm.append(clean_town_norm)
        self.year.append(clean_year)
        self.month_number.append(clean_month_number)
        self.month_key.append(clean_month_key)
        self.floor_area_sqm.append(clean_floor_area)
        self.lease_commence_year.append(clean_lease_year)
        self.resale_price.append(clean_resale_price)
        self.price_per_sqm.append(clean_price_per_sqm)

        self.validate_alignment()
        return self.row_count - 1

    def get_row_view(self, row_id: int) -> RowView:
        # function to return one row as small structured obj
        self._validate_row_id(row_id)
        return RowView(
            row_id=row_id,
            month_raw=self.month_raw[row_id],
            year=self.year[row_id],
            month_number=self.month_number[row_id],
            month_key=self.month_key[row_id],
            town_raw=self.town_raw[row_id],
            town_norm=self.town_norm[row_id],
            flat_type=self.flat_type[row_id],
            block=self.block[row_id],
            street_name=self.street_name[row_id],
            storey_range=self.storey_range[row_id],
            floor_area_sqm=self.floor_area_sqm[row_id],
            flat_model=self.flat_model[row_id],
            lease_commence_raw=self.lease_commence_raw[row_id],
            lease_commence_year=self.lease_commence_year[row_id],
            resale_price=self.resale_price[row_id],
            price_per_sqm=self.price_per_sqm[row_id],
        )

    def validate_alignment(self) -> None:
        # function for confirmation of every col having the same number of rows, impt. for col store design.
        lengths = {
            "month_raw": len(self.month_raw),
            "town_raw": len(self.town_raw),
            "flat_type": len(self.flat_type),
            "block": len(self.block),
            "street_name": len(self.street_name),
            "storey_range": len(self.storey_range),
            "flat_model": len(self.flat_model),
            "lease_commence_raw": len(self.lease_commence_raw),
            "town_norm": len(self.town_norm),
            "year": len(self.year),
            "month_number": len(self.month_number),
            "month_key": len(self.month_key),
            "floor_area_sqm": len(self.floor_area_sqm),
            "lease_commence_year": len(self.lease_commence_year),
            "resale_price": len(self.resale_price),
            "price_per_sqm": len(self.price_per_sqm),
        }

        unique_lengths = set(lengths.values())
        if len(unique_lengths) != 1:
            raise ColumnStoreError(f"Column alignment broken: {lengths}")

    def _validate_row_id(self, row_id: int) -> None:
        if not isinstance(row_id, int):
            raise ColumnStoreError(
                f"row_id must be an integer, instead got {type(row_id).__name__}."
            )
        if row_id < 0 or row_id >= self.row_count:
            raise ColumnStoreError(
                f"row_id {row_id} is out of range for {self.row_count} rows."
            )


def _require_text(field_name: str, value: str) -> str:
    if value is None:
        raise ColumnStoreError(f"{field_name} is missing (None).")
    if not isinstance(value, str):
        raise ColumnStoreError(
            f"{field_name} must be a string, instead got {type(value).__name__}."
        )
    clean_value = value.strip()
    if not clean_value:
        raise ColumnStoreError(f"{field_name} cannot be left empty.")
    return clean_value


def _normalize_town(town_raw: str) -> str:
    return town_raw.strip().upper()


def _coerce_year(value: int) -> int:
    try:
        year_value = int(value)
    except (TypeError, ValueError) as exc:
        raise ColumnStoreError(f"Year value turned out to be invalid: {value!r}.") from exc

    if year_value < 1000 or year_value > 9999:
        raise ColumnStoreError(f"Year must be four digits, instead got {year_value}.")
    return year_value


def _coerce_month_number(value: int) -> int:
    try:
        month_value = int(value)
    except (TypeError, ValueError) as exc:
        raise ColumnStoreError(f"Month number is invalid: {value!r}.") from exc

    if not 1 <= month_value <= 12:
        raise ColumnStoreError(
            f"The month number must be between 1 and 12, instead got {month_value}."
        )
    return month_value


def _coerce_positive_float(field_name: str, value: float) -> float:
    try:
        numeric_value = float(value)
    except (TypeError, ValueError) as exc:
        raise ColumnStoreError(f"{field_name} is invalid: {value!r}.") from exc

    if numeric_value <= 0:
        raise ColumnStoreError(
            f"{field_name} must be positive, instead got {numeric_value}."
        )
    return numeric_value
