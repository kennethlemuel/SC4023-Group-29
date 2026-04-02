import sys
import os
import csv
import re
from data_storage.column_store import ColumnStore


from data_storage.month_parser import parse_month_value, MonthParseError
from data_storage.models import QueryResult, OutputRow, sort_query_results
from constants import assigned_towns, towns, flat_type, storey_range, flat_model
from mapping import *

# Based on the CSV structure defined in the project description 
compressed_mappings = [
    MonthMapper(),           # Month: YYYY-MM 
    TownMapper(towns),       # Town 
    BlockMapper(),           # Block 
    CharMapper(30),          # Street_Name 
    FlatTypeMapper(flat_type), # Flat_Type 
    FlatModelMapper(flat_model), # Flat_Model 
    StoreyRangeMapper(storey_range), # Storey_Range 
    FloatMapper(),           # Floor_Area 
    ShortMapper(),           # Lease_Commence_Date (Year) 
    FloatMapper(),           # Resale_Price 
]

##################################################
# loading the CSV file
##################################################

def load_data(file_path, store_instance):
    if not os.path.exists(file_path):
        print(f"Error: {file_path} not found.")
        return

    print(f"Reading data from {file_path}...")
    # Use utf-8-sig to handle potential Byte Order Marks in Excel-generated CSVs
    with open(file_path, 'r', encoding='utf-8-sig') as f:
        # Use csv.reader to handle commas inside street names correctly
        reader = csv.reader(f)
        header = next(reader) # Skip the header row
        
        count = 0
        for parts in reader:
            if not parts or len(parts) < 10:
                continue

            try:
                # Use the provided month_parser
                parsed_date = parse_month_value(parts[0]) 
                
                # Use named arguments to match your ColumnStore.append_row
                store_instance.append_row(
                    month_raw=parts[0],           # index 0 
                    year=parsed_date.year,
                    month_number=parsed_date.month_number,
                    town_raw=parts[1],            # index 1 
                    block=parts[2],               # index 2 
                    street_name=parts[3],         # index 3 
                    flat_type=parts[4],           # index 4 
                    flat_model=parts[5],          # index 5 
                    storey_range=parts[6],        # index 6 
                    floor_area_sqm=float(parts[7]), # index 7 
                    lease_commence_raw=parts[8],  # index 8 
                    lease_commence_year=int(parts[8]),
                    resale_price=float(parts[9])  # index 9 
                )
                count += 1
                if count % 10000 == 0:
                    print(f"Loaded {count} records...")

            except Exception as e:
                # IMPORTANT: Print the error for the first failure to debug
                if count == 0:
                    print(f"Debug: First row failed with error: {e}")
                    print(f"Row content was: {parts}")
                continue

    print(f"Successfully loaded {count} records into the ColumnStore.")

###############################################
# run Queries
#############################################
def run_project_queries(main_store, matric_num):
    # a) Target Year: Last digit matches last digit of YYYY
    # Note: 2025 is for query, not as target year
    last_digit = int(matric_num[-2]) 
    target_year = 2010 + last_digit
    if target_year < 2015: target_year += 10 

    # b) Commencing Month: Second last digit ("0" is Oct)
    commence_month = int(matric_num[-3])
    if commence_month == 0: commence_month = 10

    # c) Town List: Based on all digits in matric
    matric_digits = set(re.findall(r'\d', matric_num))
    target_towns = [assigned_towns[int(d)] for d in matric_digits]

    print(f"Targeting: Year {target_year}, Start Month {commence_month}, Towns {target_towns}")

    results = []
    # d) Area requirement (y >= 80 to 150) and x (1 to 8 months)
    for x in range(1, 9):
        for y in range(80, 151):
            min_price_sqm = float('inf')
            best_record = None

            # Column-store scan 
            for i in main_store.iter_row_ids():
                # Filter conditions: Town, Year, Month Range, and Area
                if main_store.town_norm[i] not in target_towns: continue
                if main_store.year[i] != target_year: continue
                
                # Month range: From commence_month to commence_month + x - 1
                if not (commence_month <= main_store.month_number[i] < commence_month + x):
                    continue
                
                if main_store.floor_area_sqm[i] < y: continue

                # Tracking Minimum Price per Square Meter
                current_price_sqm = main_store.price_per_sqm[i]
                if current_price_sqm < min_price_sqm:
                    min_price_sqm = current_price_sqm
                    best_record = main_store.get_row_view(i)

            # Valid pair check: Min Price per Sqm <= 4725
            if best_record and min_price_sqm <= 4725:
                results.append(((x, y), best_record))
    
    return results



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
        # 1. Header row is ALWAYS required
        f.write("(x, y), Year, Month, Town, Block, Floor_Area, Flat_Model, Lease_Commence_Date, Price_Per_Square_Meter\n")
        
        # 2. Check if query_results is empty
        if not query_results:
            f.write("No result\n")
        else:
            for (x, y), rec in query_results:
                # Round Price_Per_Square_Meter as required
                line = f"({x}, {y}), {rec.year}, {rec.month_number:02d}, {rec.town_raw}, {rec.block}, {rec.floor_area_sqm}, {rec.flat_model}, {rec.lease_commence_year}, {round(rec.price_per_sqm)}\n"
                f.write(line)

    print(f"Results saved to {output_file}")