# SC4023 Group 29 HDB Resale Query

This program loads `ResalePricesSingapore.csv` into a column-oriented in-memory store,
computes the required HDB resale queries for a matriculation number, and writes
`ScanResult_<MatricNum>.csv`.

## Requirements

- Python 3.10 or newer
- No third-party Python packages are required

## Run

Default run for `U2222559E`:

```bash
python main.py
```

Explicit input file and matriculation number:

```bash
python main.py ResalePricesSingapore.csv U2222559E
```

## Query Derivation

The matriculation number determines:

- target year: year from `2015` to `2024` whose last digit matches the last
  matriculation digit
- start month: second-last digit, where `0` means October
- matched towns: all towns mapped from digits appearing in the matriculation number

For `U2222559E`, the query uses target year `2019`, start month `05`, and towns
`CLEMENTI`, `JURONG WEST`, and `YISHUN`.

## Source Layout

- `main.py`: command-line entry point
- `data_storage/`: CSV loading, column-store representation, output writing
- `data_processing/`: matric-driven query configuration and scan logic
- `tests/`: lightweight validation tests
