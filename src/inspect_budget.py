import pandas as pd
import os

file_path = 'budget_2026_v1.xlsx'

try:
    print(f"Loading {file_path}...")
    xls = pd.ExcelFile(file_path)
    print("Sheet names:", xls.sheet_names)
    
    for sheet_name in xls.sheet_names:
        print(f"\n--- Sheet: {sheet_name} ---")
        df = pd.read_excel(xls, sheet_name=sheet_name, nrows=5)
        print(df.to_string())
        print("-" * 20)

except ImportError as e:
    print(f"Error: {e}. Please install missing libraries (pandas, openpyxl).")
except Exception as e:
    print(f"An error occurred: {e}")
