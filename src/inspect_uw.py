import pandas as pd
import os
import sys

# Redirect stdout to a file with UTF-8 encoding
sys.stdout = open('inspection_uw_output.txt', 'w', encoding='utf-8')

file_path = r'data\ReportUW-07152026150110.xlsx'

try:
    print(f"Loading {file_path}...")
    xls = pd.ExcelFile(file_path)
    print("Sheet names:", xls.sheet_names)
    
    for sheet_name in xls.sheet_names:
        print(f"\n--- Sheet: {sheet_name} ---")
        try:
            df = pd.read_excel(xls, sheet_name=sheet_name, nrows=5)
            print("Columns:", list(df.columns))
            print(df.head().to_string())
            
            # Print data types interpretation
            print("\nInferred Types:")
            print(df.dtypes)
        except Exception as e:
            print(f"Error reading sheet {sheet_name}: {e}")
        print("-" * 20)

except Exception as e:
    print(f"An error occurred: {e}")
