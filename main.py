from data_storage.csv_loader import load_resale_csv
from data_storage.result_writer import write_scan_result_csv

from data_processing.query import query


def main() -> None:
	store = load_resale_csv("ResalePricesSingapore.csv")
	results = query(store)
	write_scan_result_csv("ScanResult_U2222559E.csv", store, results)


if __name__ == "__main__":
	main()