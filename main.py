import sys
import os
import csv
import re
from data_storage.column_store import ColumnStore
from data_storage.month_parser import parse_month_value, MonthParseError
from data_storage.models import *
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

# lists for column store
col_month_raw = []     
col_year = []          
col_month_num = []     
col_town_raw = []          
col_block = []         
col_street_name = []        
col_flat_type = []     
col_flat_model = []
col_storey_range = []  
col_floor_area = []    
col_lease_raw = []   
col_lease_year = []    
col_resale_price = []  

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
        # reader = csv.reader(f)
        reader = csv.DictReader(f)
        # header = next(reader) # Skip the header row
        
        count = 0
        for entries in reader:
            if not entries or len(entries) < 10:
                continue

            try:
                parsed_date = parse_month_value(entries['month'])

                # store_instance.append_row(
                #     col_month_raw.append(entries['month']),
                #     col_year.append(parsed_date.year),
                #     col_month_num.append(parsed_date.month_number),
                #     col_town_raw.append(entries['town']),
                #     col_flat_type.append(entries['flat_type']),
                #     col_block.append(entries['block']),
                #     col_street_name.append(entries['street_name']),
                #     col_storey_range.append(entries['storey_range']),
                #     col_floor_area.append(float(entries['floor_area_sqm'])),
                #     col_flat_model.append(entries['flat_model']),
                #     col_lease_raw.append((entries['lease_commence_date'])),
                #     col_lease_year.append(int(entries['lease_commence_date'])),
                #     col_resale_price.append(float(entries['resale_price'])),
                # )
                store_instance.append_row(
                    month_raw=entries['month'],
                    year=parsed_date.year,
                    month_number=parsed_date.month_number,
                    town_raw=entries['town'],
                    flat_type=entries['flat_type'],
                    block=entries['block'],
                    street_name=entries['street_name'],
                    storey_range=entries['storey_range'],
                    floor_area_sqm=float(entries['floor_area_sqm']),
                    flat_model=entries['flat_model'],
                    lease_commence_raw=entries['lease_commence_date'],
                    lease_commence_year=int(entries['lease_commence_date']),
                    resale_price=float(entries['resale_price'])
                )
                count += 1
                if count % 10000 == 0:
                    print(f"Loaded {count} records...")

            except Exception as e:
                # IMPORTANT: Print the error for the first failure to debug
                if count == 0:
                    print(f"Debug: First row failed with error: {e}")
                    print(f"Row content was: {entries}")
                continue

    print(f"Successfully loaded {count} records into the ColumnStore.")



def is_in_date_range(rec_year, rec_month, start_year, start_month, x):
    # Convert dates to a total month count: (Year * 12) + Month
    start_total = (start_year * 12) + start_month
    end_total = start_total + x - 1
    record_total = (rec_year * 12) + rec_month
    
    return start_total <= record_total <= end_total    

###############################################
# Run Queries
#############################################
def run_project_queries(store, matric_num):
    # Retrieve parameters from your mapping.py logic
    target_year, start_month, target_towns = get_query_params(matric_num) 
    
    final_results = []

    # Outer loops for (x, y) pairs 
    for x in range(1, 9):
        for y in range(80, 151):
            min_ppsm = float('inf')
            best_idx = -1

            # COLUMN-STORE SCAN: Iterate through indices 
            for i in store.iter_row_ids():
                
                # 1. Check Town column first [cite: 158]
                if store.town_norm[i] not in target_towns:
                    continue
                
                # 2. Check Area column [cite: 158]
                if store.year[i] != target_year: 
                    continue
                
                # 3. Check Date range using month_parser.py logic [cite: 17, 37]
                if not is_in_date_range(store.year[i], store.month_number[i], 
                                     target_year, start_month, x):
                    continue

                # 4. Calculation using price/area columns [cite: 14, 52, 87]
                ppsm = store.resale_price[i] / store.floor_area_sqm[i]
                if ppsm < min_ppsm:
                    min_ppsm = ppsm
                    best_idx = i

            # Minimum Price check [cite: 14, 53]
            if best_idx != -1 and min_ppsm <= 4725:
                # Use models.py to create a Result object
                final_results.append(create_result_obj(x, y, best_idx, min_ppsm))
    
    return final_results



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
    main_column_store = ColumnStore()

    # Load Data
    load_data(DATAFILE, main_column_store)

    # Run Analysis
    query_results = run_project_queries(main_column_store, MATRIC)

    output_file = f"ScanResult_{MATRIC}.csv"
    with open(output_file, 'w', encoding='utf-8') as f:
        # Title row must be exactly as shown
        f.write("(x, y), Year, Month, Town, Block, Floor_Area, Flat_Model, Lease_Commence_Date, Price_Per_Square_Meter\n")
        
        if not query_results:
            f.write("No result\n") # Required if no qualified data is found
        else:
            # 1. Sort by increasing x, then increasing y
            sorted_results = sort_query_results(query_results)
        
            for res in sorted_results:
                # res is now a QueryResult object, so we use res.matched_row_id
                rec = main_column_store.get_row_view(res.matched_row_id)
                
                area = int(rec.floor_area_sqm) if rec.floor_area_sqm.is_integer() else rec.floor_area_sqm
                
                line = (
                    f"{res.pair_label},"              # Uses the "(x, y)" property from models.py
                    f"{rec.year},"
                    f"{rec.month_number:02d},"
                    f"{rec.town_raw},"
                    f"{rec.block},"
                    f"{area},"
                    f"{rec.flat_model},"
                    f"{rec.lease_commence_year},"
                    f"{round(rec.price_per_sqm)}\n"
                )
                f.write(line)

    print(f"Final output generated: {output_file}")