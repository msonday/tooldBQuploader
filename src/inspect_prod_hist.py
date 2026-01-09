import pandas as pd
import sys

# Redirect stdout to a file with UTF-8 encoding
sys.stdout = open('inspection_prod_hist_output.txt', 'w', encoding='utf-8')

file_path = r'data\prod_hist\ProduksiJanDes2024.xlsx'

try:
    print(f"Loading {file_path}...")
    xls = pd.ExcelFile(file_path)
    print("Sheet names:", xls.sheet_names)
    
    # Check first sheet
    sheet_name = xls.sheet_names[0]
    print(f"\n--- Sheet: {sheet_name} ---")
    df = pd.read_excel(xls, sheet_name=sheet_name, nrows=5)
    print("Columns:", list(df.columns))
    print(df.head().to_string())
    print("\nInferred Types:")
    print(df.dtypes)

except Exception as e:
    print(f"An error occurred: {e}")
