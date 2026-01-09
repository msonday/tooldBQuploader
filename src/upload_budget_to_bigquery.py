import pandas as pd
from google.cloud import bigquery
from google.api_core.exceptions import Conflict
import os
import re

# Configuration
KEY_FILE = 'big-bliss-302909-06cd6e425088.json'
EXCEL_FILE = r'data\budget_080126.xlsx'
DATASET_ID = 'temporary'
LOCATION = 'asia-southeast2' # Jakarta

# Setup Credentials
if not os.path.exists(KEY_FILE):
    print(f"Error: Key file '{KEY_FILE}' not found in {os.getcwd()}")
    exit(1)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = KEY_FILE

def sanitize_table_name(name):
    """
    Sanitize the sheet name to be a valid BigQuery table name.
    - Only alphanumeric and underscores.
    - Max length 1024.
    """
    # Replace non-alphanumeric characters with underscores
    clean_name = re.sub(r'[^a-zA-Z0-9]', '_', name)
    # Ensure it starts with a letter or underscore
    if not clean_name:
        return "table"
        
    if not clean_name[0].isalpha() and clean_name[0] != '_':
        clean_name = '_' + clean_name
        
    return clean_name

def upload_budget():
    try:
        print(f"Initializing BigQuery client with key: {KEY_FILE}")
        client = bigquery.Client()
        
        # Create Dataset
        dataset_ref = client.dataset(DATASET_ID)
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = LOCATION
        
        try:
            client.create_dataset(dataset, timeout=30)
            print(f"Created dataset {client.project}.{DATASET_ID}")
        except Conflict:
            print(f"Dataset {client.project}.{DATASET_ID} already exists")

        # Read Excel
        print(f"Reading Excel file: {EXCEL_FILE}")
        xls = pd.ExcelFile(EXCEL_FILE)
        
        target_sheets = ["Input MKT", "Input Teknik"]
        for sheet_name in xls.sheet_names:
            if sheet_name not in target_sheets:
                continue
                
            print(f"Processing sheet: {sheet_name}")
            try:
                df = pd.read_excel(xls, sheet_name=sheet_name)
                
                # Attempt to convert object columns to numeric where possible
                # This fixes issues where numeric columns are read as objects due to a few bad values or formatting
                for col in df.columns:
                    # Clean up common non-numeric formatting like ' - ' for 0
                    if df[col].dtype == 'object':
                        # Try to handle the specific case where accounting explicit format results in '-' for 0
                        df[col] = df[col].replace({'-': 0})
                        # Attempt to convert to numeric, coercing errors to NaN (which will be handled later)
                        # We only keep the numeric version if it doesn't destroy the data (i.e. if it wasn't all just text)
                        temp_col = pd.to_numeric(df[col], errors='coerce')
                        
                        # Check: If the column had data, and conversion resulted in SOME numbers (or was already empty), use it.
                        # If it was purely text, temp_col would be all NaN (except where original was NaN).
                        # Simple heuristic: If the original column had non-null values, and the numeric conversion has non-null values
                        # we assume it's numeric.
                        if temp_col.notna().sum() > 0 or df[col].notna().sum() == 0:
                             df[col] = temp_col

                # Handle NaNs: 0 for numbers, empty string for others
                for col in df.columns:
                    if pd.api.types.is_numeric_dtype(df[col]):
                        df[col] = df[col].fillna(0)
                    else:
                        df[col] = df[col].fillna("")
                        # Ensure string type for non-numeric to avoid mixed type issues
                        df[col] = df[col].astype(str)

                # Sanitize column names
                # Replace non-alphanumeric (except underscore) with underscore
                # BigQuery fields must contain only letters, numbers, and underscores, start with letter or underscore, length <= 300
                new_columns = []
                for col in df.columns:
                    col_str = str(col)
                    # specific replacements for readability
                    col_str = col_str.replace('%', '_pct')
                    col_str = col_str.replace('&', '_and_')
                    col_str = col_str.replace('+', '_plus_')
                    
                    # Replace non-alphanumeric characters with a single underscore
                    clean_col = re.sub(r'[^a-zA-Z0-9]+', '_', col_str)
                    
                    # Strip leading/trailing underscores
                    clean_col = clean_col.strip('_')
                    
                    if not clean_col:
                        clean_col = "col_"
                    if not clean_col[0].isalpha() and clean_col[0] != '_':
                        clean_col = '_' + clean_col
                    new_columns.append(clean_col)
                df.columns = new_columns
                
                table_name = sanitize_table_name(sheet_name)
                table_id = f"{client.project}.{DATASET_ID}.{table_name}"
                
                print(f"  -> Uploading to {table_id}...")
                
                # DELETE table if it exists to handle cases where it might be an EXTERNAL table
                # which cannot be overwritten by load_table_from_dataframe with WRITE_TRUNCATE
                client.delete_table(table_id, not_found_ok=True)
                
                job_config = bigquery.LoadJobConfig(
                    write_disposition="WRITE_TRUNCATE", # Defines the action when the table exists.
                    autodetect=True,
                )
                
                job = client.load_table_from_dataframe(
                    df, table_id, job_config=job_config
                )
                job.result() # Wait for job to complete
                
                print(f"  -> Success! Loaded {job.output_rows} rows to {table_id}")
            except Exception as e:
                print(f"  -> Failed to upload sheet '{sheet_name}': {e}")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    upload_budget()
