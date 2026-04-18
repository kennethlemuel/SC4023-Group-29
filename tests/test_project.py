from __future__ import annotations

import csv
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from data_processing.config import build_query_config
from data_processing.month_range import build_month_range
from data_storage.month_parser import parse_month_value


ROOT = Path(__file__).resolve().parents[1]


class ProjectTest(unittest.TestCase):
    def test_matric_number_builds_expected_config(self) -> None:
        config = build_query_config("U2222559E")

        self.assertEqual(config.matric_number, "U2222559E")
        self.assertEqual(config.target_year, 2019)
        self.assertEqual(config.start_month, 5)
        self.assertEqual(
            config.matched_towns,
            frozenset({"CLEMENTI", "JURONG WEST", "YISHUN"}),
        )

    def test_matric_number_target_year_wraps_to_2020s(self) -> None:
        self.assertEqual(build_query_config("A0000000B").target_year, 2020)
        self.assertEqual(build_query_config("A1234564B").target_year, 2024)
        self.assertEqual(build_query_config("A1234565B").target_year, 2015)

    def test_month_parsing_supports_dataset_formats(self) -> None:
        jan_2015 = parse_month_value("Jan-15")
        may_2019 = parse_month_value("2019-05")

        self.assertEqual(
            (jan_2015.year, jan_2015.month_number, jan_2015.month_key),
            (2015, 1, 201501),
        )
        self.assertEqual(
            (may_2019.year, may_2019.month_number, may_2019.month_key),
            (2019, 5, 201905),
        )

    def test_month_range_for_u2222559e(self) -> None:
        config = build_query_config("U2222559E")

        self.assertEqual(build_month_range(config, 1), (201905, 201905))
        self.assertEqual(build_month_range(config, 8), (201905, 201912))

    def test_default_program_output_shape_and_key_rows(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "ScanResult_U2222559E.csv"

            subprocess.run(
                [
                    sys.executable,
                    str(ROOT / "main.py"),
                    str(ROOT / "ResalePricesSingapore.csv"),
                    "U2222559E",
                    "-o",
                    str(output_path),
                ],
                cwd=ROOT,
                check=True,
            )

            with output_path.open(newline="", encoding="utf-8") as csv_file:
                rows = list(csv.reader(csv_file))

        self.assertEqual(
            rows[0],
            [
                "(x, y)",
                "Year",
                "Month",
                "Town",
                "Block",
                "Floor_Area",
                "Flat_Model",
                "Lease_Commence_Date",
                "Price_Per_Square_Meter",
            ],
        )
        self.assertEqual(len(rows), 569)
        self.assertEqual(
            rows[1],
            [
                "(1, 80)",
                "2019",
                "05",
                "JURONG WEST",
                "820",
                "113",
                "Model A",
                "1993",
                "2655",
            ],
        )
        self.assertEqual(
            rows[-1],
            [
                "(8, 150)",
                "2019",
                "07",
                "JURONG WEST",
                "407",
                "150",
                "Maisonette",
                "1985",
                "3253",
            ],
        )


if __name__ == "__main__":
    unittest.main()
