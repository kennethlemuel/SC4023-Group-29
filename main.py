import sys
import os
import csv
import re
from data_storage.column_store import ColumnStore


from data_storage.month_parser import parse_month_value, MonthParseError
from data_storage.models import QueryResult, OutputRow, sort_query_results
from constants import assigned_towns, towns, flat_type, storey_range, flat_model
from mapping import *

# from query import QueryHelper

# def perform_analysis(analysis_store: ColumnStore):
#     """Perform full analysis on a given store"""
#     analysis_store.flush_write_buffers()
#     print("\n---------COMPRESSED STORE---------")
#     analysis_store.print_storage_stats()

#     print(
#         f"\n\nRunning queries for {TOWN_NAME} from months {int(MATRIC[-3])} to {int(MATRIC[-3])+1} in {YEAR}"
#     )


# DATAFILE = sys.argv[1]
# if not os.path.isfile(DATAFILE):
#     print(f"{DATAFILE} not found!")
#     sys.exit(1)

# MATRIC = sys.argv[2]
# pattern = re.compile(r'[A-Z][0-9]{7}[A-Z]')
# if not re.match(pattern=pattern, string=MATRIC):
#     print("Invalid Matric format. Should be A1234567B")
#     sys.exit(1)

##################################
# Map values
##################################
# townMapper = TownMapper(towns)

# TOWN_NAME = assigned_towns[int(MATRIC[-4])]
# TOWN = townMapper.map_value(TOWN_NAME)

# monthMapper = MonthMapper()
# floatMapper = FloatMapper()
# shortMapper = ShortMapper()

# YEAR = int(MATRIC[-2]) + 2010
# if YEAR < 2014:
#     YEAR += 10

# month_str = "10" if f"0{MATRIC[-3]}" == "00" else f"0{MATRIC[-3]}"
# MONTH = monthMapper.map_value(f"{YEAR}-{month_str}")

# print("Loading data")    

########################################################
# Define mappings for basic and compressed stores
########################################################
# basic_mappings = [
#     CharMapper(7),
#     CharMapper(15),
#     CharMapper(16),
#     CharMapper(5),
#     CharMapper(20),
#     CharMapper(12),
#     floatMapper,
#     CharMapper(22),
#     shortMapper,
#     floatMapper,
# ]

# compressed_mappings = [
#     monthMapper,
#     townMapper,
#     FlatTypeMapper(flat_type),
#     BlockMapper(),
#     CharMapper(20),
#     StoreyRangeMapper(storey_range),
#     floatMapper,
#     FlatModelMapper(flat_model),
#     shortMapper,
#     floatMapper,
# ]

# Based on the CSV structure defined in the project description 
compressed_mappings = [
    MonthMapper(),           # Month: YYYY-MM [cite: 58]
    TownMapper(towns),       # Town [cite: 59]
    BlockMapper(),           # Block [cite: 60]
    CharMapper(30),          # Street_Name [cite: 61]
    FlatTypeMapper(flat_type), # Flat_Type [cite: 62]
    FlatModelMapper(flat_model), # Flat_Model [cite: 64]
    StoreyRangeMapper(storey_range), # Storey_Range [cite: 65]
    FloatMapper(),           # Floor_Area [cite: 66]
    ShortMapper(),           # Lease_Commence_Date (Year) [cite: 67]
    FloatMapper(),           # Resale_Price [cite: 68]
]

##################################################
# loading the CSV file
##################################################

def load_data(file_path, store_instance):
    if not os.path.exists(file_path):
        print(f"Error: {file_path} not found.")
        return

    print(f"Reading data from {file_path}...")
    with open(file_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        header = next(reader) # Skip title row [cite: 57]
        
        count = 0
        for parts in reader:
            if not parts or len(parts) < 10:
                continue

            try:
                parsed_date = parse_month_value(parts[0])
                store_instance.append_row(
                    month_raw=parts[0],
                    year=parsed_date.year,
                    month_number=parsed_date.month_number,
                    town_raw=parts[1],
                    block=parts[2],
                    street_name=parts[3],
                    flat_type=parts[4],
                    flat_model=parts[5],
                    storey_range=parts[6],
                    floor_area_sqm=float(parts[7]),
                    lease_commence_raw=parts[8],
                    lease_commence_year=int(parts[8]),
                    resale_price=float(parts[9])
                )
                count += 1
                if count % 10000 == 0:
                    print(f"Loaded {count} records...")
            except Exception as e:
                pass # Silent skip for bad rows to keep alignment clean [cite: 155]

    print(f"Successfully loaded {count} records into the ColumnStore.")

###############################################
# run Queries
#############################################
def run_project_queries(main_store, matric_num):
    # a) Target Year: Last digit matches last digit of YYYY [cite: 17]
    # Note: 2025 is for query, not as target year [cite: 18]
    last_digit = int(matric_num[-2]) 
    target_year = 2010 + last_digit
    if target_year < 2015: target_year += 10 

    # b) Commencing Month: Second last digit ("0" is Oct) [cite: 20]
    commence_month = int(matric_num[-3])
    if commence_month == 0: commence_month = 10

    # c) Town List: Based on all digits in matric [cite: 21, 24]
    matric_digits = set(re.findall(r'\d', matric_num))
    target_towns = [assigned_towns[int(d)] for d in matric_digits]

    print(f"Targeting: Year {target_year}, Start Month {commence_month}, Towns {target_towns}")

    results = []
    # d) Area requirement (y >= 80 to 150) and x (1 to 8 months) [cite: 14, 22]
    for x in range(1, 9):
        for y in range(80, 151):
            min_price_sqm = float('inf')
            best_record = None

            # Column-store scan [cite: 133, 158]
            for i in main_store.iter_row_ids():
                # Filter conditions: Town, Year, Month Range, and Area [cite: 26, 51]
                if main_store.town_norm[i] not in target_towns: continue
                if main_store.year[i] != target_year: continue
                
                # Month range: From commence_month to commence_month + x - 1
                if not (commence_month <= main_store.month_number[i] < commence_month + x):
                    continue
                
                if main_store.floor_area_sqm[i] < y: continue

                # Tracking Minimum Price per Square Meter [cite: 14, 52]
                current_price_sqm = main_store.price_per_sqm[i]
                if current_price_sqm < min_price_sqm:
                    min_price_sqm = current_price_sqm
                    best_record = main_store.get_row_view(i)

            # Valid pair check: Min Price per Sqm <= 4725 [cite: 14, 53]
            if best_record and min_price_sqm <= 4725:
                results.append(((x, y), best_record))
    
    return results

##################################################
# TESTING
##################################################

# Define columns that must not be empty
# critical = [0, 1, 6, 9]


# with open(DATAFILE, 'r') as f:
#     print("\n---------BASIC STORE---------")
#     columns = f.readline()[:-1].split(",")
#     basic_store = ColumnStore(
#         # columns=columns, mappings=basic_mappings, critical=critical, basic=True
#         columns=columns, mappings=basic_mappings, basic=True
#     )
#     while True:
#         line = f.readline()[:-1]
#         if not line:
#             break
#         try:
#             basic_store.add_entry(line.split(","))
#         except Exception as s:
#             print(f"Line {line}:", type(s).__name__, " -> ", str(s), "Skipping...")
#     basic_store.flush_write_buffers()
#     basic_store.print_storage_stats()
#     basic_store.clear_disk()

# with open(DATAFILE, "r") as f:
#     columns = f.readline()[:-1].split(",")
#     store = ColumnStore(
#         # columns=columns, mappings=compressed_mappings, critical=critical
#         columns=columns, mappings=compressed_mappings
#     )
#     sorted_rows = []
#     while True:
#         line = f.readline()[:-1]
#         if not line:
#             break
#         sorted_rows.append(line.split(","))
#         try:
#             store.add_entry(line.split(","))
#         except Exception as s:
#             print(f"Line {line}:", type(s).__name__, " -> ", str(s), "Skipping...")


##################################################
# SIMPLE TESTING
##################################################
print("--- Starting Simple Test ---")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        # print("Usage: python main.py ResalePricesSingapore.csv <matric_number>")
        print("Usage: python main.py ResalePricesSingapore.csv U2222559E")
        sys.exit(1)

    DATAFILE, MATRIC = sys.argv[1], sys.argv[2]
    main_store = ColumnStore()

    # Load Data
    load_data(DATAFILE, main_store)

    # Run Analysis
    query_results = run_project_queries(main_store, MATRIC)

    # Write Output File: ScanResult_<MatricNum>.csv
    output_file = f"ScanResult_{MATRIC}.csv"
    with open(output_file, 'w') as f:
        # Title row
        f.write("(x, y), Year, Month, Town, Block, Floor_Area, Flat_Model, Lease_Commence_Date, Price_Per_Square_Meter\n")
        
        if not query_results:
            f.write("No result\n") # [cite: 78]
        else:
            for (x, y), rec in query_results:
                # Format: (x, y) follow increasing order of x then y [cite: 79, 80]
                line = f"({x}, {y}), {rec.year}, {rec.month_number:02d}, {rec.town_raw}, {rec.block}, {rec.floor_area_sqm}, {rec.flat_model}, {rec.lease_commence_year}, {round(rec.price_per_sqm)}\n"
                f.write(line)

    print(f"Results saved to {output_file}")