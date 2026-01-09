import pandas as pd
from google.cloud import bigquery
from google.api_core.exceptions import Conflict
import os
import re
from datetime import datetime

# Configuration
KEY_FILE = r'..\big-bliss-302909-06cd6e425088.json'
EXCEL_FILE = r'..\data\ReportUW-07152026150110.xlsx'
DATASET_ID = 'temporary'
LOCATION = 'asia-southeast2' # Jakarta

# Setup Credentials
if not os.path.exists(KEY_FILE):
    print(f"Error: Key file '{KEY_FILE}' not found in {os.getcwd()}")
    exit(1)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = KEY_FILE

def sanitize_table_name(name):
    clean_name = re.sub(r'[^a-zA-Z0-9]', '_', name)
    if not clean_name:
        return "table"
    if not clean_name[0].isalpha() and clean_name[0] != '_':
        clean_name = '_' + clean_name
    return clean_name

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
        
        # Define Schema
        schema = [
            bigquery.SchemaField("IdLogQuotation", "FLOAT"),
            bigquery.SchemaField("DataStatus", "STRING"), # Note: 'Data Status' in source likely needs checking
            bigquery.SchemaField("IdQuotation", "FLOAT"),
            bigquery.SchemaField("InsuredName", "STRING"),
            bigquery.SchemaField("IdTOC", "FLOAT"),
            bigquery.SchemaField("TOC", "STRING"),
            bigquery.SchemaField("IdSOB", "FLOAT"),
            bigquery.SchemaField("SOB", "STRING"),
            bigquery.SchemaField("Marketing", "STRING"),
            bigquery.SchemaField("Branch", "STRING"),
            bigquery.SchemaField("DateCreated", "DATETIME"),
            bigquery.SchemaField("UserSubmit", "STRING"),
            bigquery.SchemaField("SubmitDate", "DATETIME"),
            bigquery.SchemaField("Underwriter", "STRING"),
            bigquery.SchemaField("Reinsurance", "STRING"),
            bigquery.SchemaField("ResponseDate", "STRING"),
            bigquery.SchemaField("ResponseTime", "STRING"),
            bigquery.SchemaField("SLA", "STRING"),
            bigquery.SchemaField("ResponseTimeMinutes", "FLOAT"),
            bigquery.SchemaField("IDCOB", "FLOAT"),
            bigquery.SchemaField("COB", "STRING"),
            bigquery.SchemaField("create_date", "DATETIME"),
            bigquery.SchemaField("modified_date", "DATETIME"),
            bigquery.SchemaField("create_by", "STRING"),
            bigquery.SchemaField("modified_by", "STRING"),
            bigquery.SchemaField("TSI", "FLOAT"),
            bigquery.SchemaField("Share", "FLOAT"),
            bigquery.SchemaField("SharePercentage", "FLOAT"),
            bigquery.SchemaField("CompletionStatus", "STRING"),
            bigquery.SchemaField("LatestPIC", "STRING"),
        ]

        # Column Mapping (Source Excel Name -> Target Schema Name)
        # Based on previous inspection: 'Id Log Quotation', 'Id Quotation', etc.
        column_mapping = {
            'Id Log Quotation': 'IdLogQuotation',
            # 'Data Status': 'DataStatus', # Not seen in inspection output, but adding if present
            'Id Quotation': 'IdQuotation',
            'Insured Name': 'InsuredName',
            'Id TOC': 'IdTOC',
            'TOC': 'TOC',
            'Id SOB': 'IdSOB',
            'SOB': 'SOB',
            'Marketing': 'Marketing',
            'Branch': 'Branch',
            'Date Created': 'DateCreated',
            'User Submit': 'UserSubmit',
            'Submit Date': 'SubmitDate',
            'Underwriter': 'Underwriter',
            'Reinsurance': 'Reinsurance',
            'Response Date': 'ResponseDate',
            'Response Time': 'ResponseTime',
            'SLA': 'SLA',
            'Response Time Minutes': 'ResponseTimeMinutes',
            'ID COB': 'IDCOB',
            'COB': 'COB',
            'TSI': 'TSI',
            'Share': 'Share',
            'Share Percentage': 'SharePercentage',
            'Completion Status': 'CompletionStatus',
            'Latest PIC': 'LatestPIC'
        }

        target_sheets = ["Report UW"]
        
        for sheet_name in xls.sheet_names:
            if sheet_name not in target_sheets:
                print(f"Skipping sheet: {sheet_name}")
                continue
                
            print(f"Processing sheet: {sheet_name}")
            try:
                df = pd.read_excel(xls, sheet_name=sheet_name)
                
                # Rename columns
                df = df.rename(columns=column_mapping)
                
                # Check for 'Data Status' which might be missing from mapping if it wasn't in inspection
                if 'Data Status' in df.columns:
                    df = df.rename(columns={'Data Status': 'DataStatus'})

                # Add Audit Columns
                now = datetime.now()
                df['create_date'] = now
                df['modified_date'] = now
                df['create_by'] = 'ETL_Script'
                df['modified_by'] = 'ETL_Script'

                # Ensure only columns in schema are kept
                schema_cols = [field.name for field in schema]
                
                # Initialize missing columns with None
                for col in schema_cols:
                    if col not in df.columns:
                        print(f"  Warning: Column {col} missing in source, filling with Null")
                        df[col] = None 
                        
                df = df[schema_cols]
                
                # Type Conversions
                
                # 1. FLOATs
                float_cols = [f.name for f in schema if f.field_type == 'FLOAT']
                for col in float_cols:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                
                # 2. STRINGs
                str_cols = [f.name for f in schema if f.field_type == 'STRING']
                for col in str_cols:
                    # Special handling for dates that should be strings (ResponseDate)
                    # If it's datetime, convert to YYYY-MM-DD string
                    if pd.api.types.is_datetime64_any_dtype(df[col]):
                        df[col] = df[col].dt.strftime('%Y-%m-%d')
                    
                    df[col] = df[col].astype(str).replace({'nan': None, 'NaN': None, '<NA>': None, 'None': None})
                    # Replace literal 'NaT' string if it occurred
                    df[col] = df[col].replace('NaT', None)

                # 3. DATETIME
                dt_cols = [f.name for f in schema if f.field_type == 'DATETIME']
                for col in dt_cols:
                     if col in ['create_date', 'modified_date']:
                         continue # Already set as datetime objects
                     df[col] = pd.to_datetime(df[col], errors='coerce')

                table_name = sanitize_table_name(sheet_name)
                table_id = f"{client.project}.{DATASET_ID}.{table_name}"
                
                print(f"  -> Uploading to {table_id}...")
                
                # DELETE table if it exists
                client.delete_table(table_id, not_found_ok=True)
                
                job_config = bigquery.LoadJobConfig(
                    write_disposition="WRITE_TRUNCATE",
                    schema=schema
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
