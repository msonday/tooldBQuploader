import pandas as pd
from google.cloud import bigquery
from google.api_core.exceptions import Conflict
import os
import re
from datetime import datetime

# Configuration
KEY_FILE = 'big-bliss-302909-06cd6e425088.json'
EXCEL_FILE = r'data\ReportUW-07152026150110.xlsx'
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
    """
    clean_name = re.sub(r'[^a-zA-Z0-9]', '_', name)
    if not clean_name:
        return "table"
    if not clean_name[0].isalpha() and clean_name[0] != '_':
        clean_name = '_' + clean_name
    return clean_name

def clean_column_name(col_name):
    """
    Remove spaces and special chars, convert to CamelCase/PascalCase-like or snake_case if preferred.
    Matching the user's previous preference: 'Date Created' -> 'DateCreated' (Remove spaces).
    """
    # Remove all non-alphanumeric characters (spaces, punctuation)
    # But keep the casing as is (e.g. 'Date Created' -> 'DateCreated')
    return re.sub(r'[^a-zA-Z0-9]', '', str(col_name))

def upload_uw():
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
        
        target_sheets = ["Report UW"]
        
        for sheet_name in xls.sheet_names:
            if sheet_name not in target_sheets:
                print(f"Skipping sheet: {sheet_name}")
                continue
                
            print(f"Processing sheet: {sheet_name}")
            try:
                df = pd.read_excel(xls, sheet_name=sheet_name)
                
                # Clean Column Names
                df.columns = [clean_column_name(c) for c in df.columns]
                print(f"  -> Cleaned columns: {list(df.columns)}")

                # Add Audit Columns
                now = datetime.now()
                df['create_date'] = now
                df['modified_date'] = now
                df['create_by'] = 'ETL_Script'
                df['modified_by'] = 'ETL_Script'

                # Date/Time Handling
                # Convert columns containing 'Date' or 'Time' to datetime objects
                # This ensures BigQuery detects them as TIMESTAMP/DATETIME instead of STRING
                date_keywords = ['Date', 'Time']
                for col in df.columns:
                    if any(k in col for k in date_keywords):
                        # Special check: prevent converting audit cols we just added if we don't want to coerce them (they are already datetime)
                        if col in ['create_date', 'modified_date']:
                            continue
                            
                        # Convert to datetime, coerce errors to NaT
                        # Skip columns that look like numeric durations
                        if 'Minutes' in col:
                            continue

                        if df[col].dtype == 'object' or pd.api.types.is_numeric_dtype(df[col]):
                             df[col] = pd.to_datetime(df[col], errors='coerce')

                table_name = sanitize_table_name(sheet_name)
                table_id = f"{client.project}.{DATASET_ID}.{table_name}"
                
                print(f"  -> Uploading to {table_id}...")
                
                # DELETE table if it exists
                client.delete_table(table_id, not_found_ok=True)
                
                job_config = bigquery.LoadJobConfig(
                    write_disposition="WRITE_TRUNCATE",
                    autodetect=True, # Auto-detect schema from dataframe types
                )
                
                job = client.load_table_from_dataframe(
                    df, table_id, job_config=job_config
                )
                job.result() # Wait for job to complete
                
                print(f"  -> Success! Loaded {job.output_rows} rows to {table_id}")
            except Exception as e:
                print(f"  -> Failed to upload sheet '{sheet_name}': {e}")
                if hasattr(e, 'errors'):
                    print(f"     Detailed errors: {e.errors}")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    upload_uw()
