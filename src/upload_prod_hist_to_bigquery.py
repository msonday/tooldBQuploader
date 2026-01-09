import pandas as pd
from google.cloud import bigquery
from google.api_core.exceptions import Conflict
import os
import glob
import re
from datetime import datetime

# Configuration
KEY_FILE = r'..\big-bliss-302909-06cd6e425088.json'
PROD_HIST_DIR = r'..\data\prod_hist'
DATASET_ID = 'temporary'
LOCATION = 'asia-southeast2' # Jakarta
TARGET_TABLE_NAME = 'Report_Prod_Hist_All'

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

def upload_prod_hist():
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

        # Define Schema
        schema = [
            # User provided schema
            bigquery.SchemaField("TANGGAL", "STRING"), # Note: Schema says STRING, file has datetime. Will convert.
            bigquery.SchemaField("BULAN", "INTEGER"),
            bigquery.SchemaField("TAHUN", "INTEGER"),
            bigquery.SchemaField("BRANCH", "STRING"),
            bigquery.SchemaField("MO", "STRING"),
            bigquery.SchemaField("SARATOGA", "STRING"),
            bigquery.SchemaField("CGROUP", "STRING"),
            bigquery.SchemaField("SCGROUP", "STRING"),
            bigquery.SchemaField("SOURCE", "STRING"),
            bigquery.SchemaField("COB", "STRING"),
            bigquery.SchemaField("TOC", "STRING"),
            bigquery.SchemaField("LOB", "STRING"),
            bigquery.SchemaField("bankersclause", "STRING"),
            bigquery.SchemaField("NEW_", "INTEGER"),
            bigquery.SchemaField("NEW_PREMI", "BIGNUMERIC"),
            bigquery.SchemaField("RENEWAL", "INTEGER"),
            bigquery.SchemaField("RENEWAL_PREMI", "BIGNUMERIC"),
            bigquery.SchemaField("COMM", "BIGNUMERIC"),
            bigquery.SchemaField("COMMSUBS", "BIGNUMERIC"),
            bigquery.SchemaField("DISC", "BIGNUMERIC"),
            bigquery.SchemaField("ENGFEE", "BIGNUMERIC"),
            bigquery.SchemaField("FACPREM", "BIGNUMERIC"),
            bigquery.SchemaField("RIPREM", "BIGNUMERIC"),
            bigquery.SchemaField("FACRICOM", "BIGNUMERIC"),
            bigquery.SchemaField("SOURCE_PROFILEID", "STRING"),
            
            # Audit columns
            bigquery.SchemaField("create_by", "STRING"),
            bigquery.SchemaField("modified_by", "STRING"),
            bigquery.SchemaField("create_date", "DATETIME"),
            bigquery.SchemaField("modified_date", "DATETIME"),
            
            # Added for file tracking
            bigquery.SchemaField("SourceFilename", "STRING"),
        ]

        # Column Mapping (Lower/Mixed case in File -> Upper/Specific case in Schema)
        column_mapping = {
            'tanggal': 'TANGGAL',
            'Bulan': 'BULAN',
            'Tahun': 'TAHUN',
            'branch': 'BRANCH',
            'MO': 'MO',
            'SARATOGA': 'SARATOGA',
            'CGroup': 'CGROUP',
            'SCGroup': 'SCGROUP',
            'source': 'SOURCE',
            'cob': 'COB',
            'toc': 'TOC',
            'LOB': 'LOB',
            'BankersClause': 'bankersclause',
            'New': 'NEW_',
            'New Premi': 'NEW_PREMI',
            'Renewal': 'RENEWAL',
            'Renewal Premi': 'RENEWAL_PREMI',
            'Comm': 'COMM',
            'CommSUBS': 'COMMSUBS',
            'Disc': 'DISC',
            'EngFee': 'ENGFEE',
            'FacPrem': 'FACPREM',
            'RIPrem': 'RIPREM',
            'FacRICom': 'FACRICOM',
            'SourceBusiness': 'SOURCE_PROFILEID'
        }

        # Find all excel files
        files = glob.glob(os.path.join(PROD_HIST_DIR, "*.xlsx"))
        print(f"Found {len(files)} files in {PROD_HIST_DIR}")
        
        all_dfs = []
        for file_path in files:
            print(f"Processing file: {os.path.basename(file_path)}")
            try:
                # Read all columns as object (string) first for BIGNUMERIC safety, 
                # or rely on pandas default types.
                # BIGNUMERIC in BQ can handle large decimals. 
                # We'll stick to default pandas types but convert numeric cols later if needed.
                df = pd.read_excel(file_path)
                
                # Check if empty
                if df.empty:
                    print(f"  Warning: File is empty, skipping.")
                    continue
                    
                # Rename columns
                df = df.rename(columns=column_mapping)
                
                # Add SourceFilename
                df['SourceFilename'] = os.path.basename(file_path)
                
                all_dfs.append(df)
            except Exception as e:
                print(f"  Error reading {file_path}: {e}")

        if not all_dfs:
            print("No data found to upload.")
            return

        # Concatenate all
        final_df = pd.concat(all_dfs, ignore_index=True)
        print(f"Total rows to process: {len(final_df)}")

        # Add Audit Columns
        now = datetime.now()
        final_df['create_date'] = now
        final_df['modified_date'] = now
        final_df['create_by'] = 'ETL_Script'
        final_df['modified_by'] = 'ETL_Script'

        # Ensure schema columns exist
        schema_cols = [field.name for field in schema]
        for col in schema_cols:
            if col not in final_df.columns:
                print(f"  Warning: Column {col} missing in data, filling with Null")
                final_df[col] = None
        
        final_df = final_df[schema_cols]

        # Handling Types
        # TANGGAL: Schema says STRING, file is datetime. Convert to YYYY-MM-DD string.
        if 'TANGGAL' in final_df.columns and pd.api.types.is_datetime64_any_dtype(final_df['TANGGAL']):
            final_df['TANGGAL'] = final_df['TANGGAL'].dt.strftime('%Y-%m-%d')
        
        # BIGNUMERIC fields: Use decimal.Decimal for precision and Arrow compatibility
        import decimal
        bignumeric_cols = [f.name for f in schema if f.field_type == 'BIGNUMERIC']
        for col in bignumeric_cols:
             final_df[col] = final_df[col].apply(lambda x: decimal.Decimal(str(x)) if pd.notnull(x) and str(x).lower() != 'nan' else None)

        # Upload
        table_id = f"{client.project}.{DATASET_ID}.{TARGET_TABLE_NAME}"
        print(f"Uploading to {table_id}...")
        
        # We perform a full refresh (WRITE_TRUNCATE) for the "Folder Logic"
        # upserting by checking existing data would be too slow/complex without a dedicated Key.
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            schema=schema
        )
        
        job = client.load_table_from_dataframe(
            final_df, table_id, job_config=job_config
        )
        job.result()
        
        print(f"Success! Loaded {job.output_rows} rows to {table_id}")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    upload_prod_hist()
