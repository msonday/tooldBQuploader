import pandas as pd
from google.cloud import bigquery
from google.api_core.exceptions import Conflict
import os
import re
from datetime import datetime

# Configuration
KEY_FILE = r'..\big-bliss-302909-06cd6e425088.json'
EXCEL_FILE = r'..\data\ReportLogbookAll-07202026170107.xlsx'
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

def upload_logbook():
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
        
        target_sheets = ["Report Logbook All"]
        
        # Define the target schema
        schema = [
            bigquery.SchemaField("IdLogbook", "FLOAT"),
            bigquery.SchemaField("IdQuotation", "FLOAT"),
            bigquery.SchemaField("InsuredName", "STRING"),
            bigquery.SchemaField("PolicyNo", "STRING"),
            bigquery.SchemaField("IdTOC", "INTEGER"),
            bigquery.SchemaField("TOC", "STRING"),
            bigquery.SchemaField("SOB", "STRING"),
            bigquery.SchemaField("LastStatus", "STRING"),
            bigquery.SchemaField("LastUser", "STRING"),
            bigquery.SchemaField("Marketing", "STRING"),
            bigquery.SchemaField("Branch", "STRING"),
            bigquery.SchemaField("DateCreated", "DATETIME"),
            bigquery.SchemaField("PoolAccount", "STRING"),
            bigquery.SchemaField("DateReceivePoolAccount", "STRING"),
            bigquery.SchemaField("TotalTimePoolAccount", "STRING"),
            bigquery.SchemaField("PolicyProcessing", "STRING"),
            bigquery.SchemaField("DateReceivePolicyProcessing", "STRING"),
            bigquery.SchemaField("TotalTimePolicyProcessing", "STRING"),
            bigquery.SchemaField("Inforcer", "STRING"),
            bigquery.SchemaField("DateReceiveInforcer", "STRING"),
            bigquery.SchemaField("TotalTimeInforcer", "STRING"),
            bigquery.SchemaField("Underwriting", "STRING"),
            bigquery.SchemaField("DateReceiveUnderwriting", "STRING"),
            bigquery.SchemaField("TotalTimeUnderwriting", "STRING"),
            bigquery.SchemaField("Reinsurance", "STRING"),
            bigquery.SchemaField("DateReceiveReinsurance", "STRING"),
            bigquery.SchemaField("TotalTimeReinsurance", "STRING"),
            bigquery.SchemaField("DateReceiveMarketingRevision", "STRING"),
            bigquery.SchemaField("TotalTimeonMarketingRevision", "STRING"),
            bigquery.SchemaField("DateReceiveMarketingFinished", "STRING"),
            bigquery.SchemaField("TotalTimeonMarketingFinished", "STRING"),
            bigquery.SchemaField("CombineTimeonPPPoolAccountPPInforcer", "STRING"),
            bigquery.SchemaField("CombineTimeonTechnicCombineTimeonPPUWReinsurance", "STRING"),
            bigquery.SchemaField("TransactionIteration", "FLOAT"),
            bigquery.SchemaField("InsuranceType", "STRING"),
            bigquery.SchemaField("SLACombineTimeonPP", "STRING"),
            bigquery.SchemaField("SLACombineTimeonTechnic", "STRING"),
            bigquery.SchemaField("SLAUnderwriting", "STRING"),
            bigquery.SchemaField("SLAReinsurance", "STRING"),
            bigquery.SchemaField("DateReceiveMarketingAgreed", "STRING"),
            bigquery.SchemaField("TotalTimeonMarketingAgreed", "STRING"),
            bigquery.SchemaField("create_date", "DATETIME"),
            bigquery.SchemaField("modified_date", "DATETIME"),
            bigquery.SchemaField("create_by", "STRING"),
            bigquery.SchemaField("modified_by", "STRING"),
        ]

        # Map Excel columns to BigQuery schema names
        column_mapping = {
            'Id Logbook': 'IdLogbook',
            'Id Quotation': 'IdQuotation',
            'Insured Name': 'InsuredName',
            'Policy No': 'PolicyNo',
            'Id TOC': 'IdTOC',
            'TOC': 'TOC',
            'SOB': 'SOB',
            'Last Status': 'LastStatus',
            'Last User': 'LastUser',
            'Marketing': 'Marketing',
            'Branch': 'Branch',
            'Date Created': 'DateCreated',
            'Pool Account': 'PoolAccount',
            'Date Receive Pool Account': 'DateReceivePoolAccount',
            'Total Time Pool Account': 'TotalTimePoolAccount',
            'Policy Processing': 'PolicyProcessing',
            'Date Receive Policy Processing': 'DateReceivePolicyProcessing',
            'Total Time Policy Processing': 'TotalTimePolicyProcessing',
            'Inforcer': 'Inforcer',
            'Date Receive Inforcer': 'DateReceiveInforcer',
            'Total Time Inforcer': 'TotalTimeInforcer',
            'Underwriting': 'Underwriting',
            'Date Receive Underwriting': 'DateReceiveUnderwriting',
            'Total Time Underwriting': 'TotalTimeUnderwriting',
            'Reinsurance': 'Reinsurance',
            'Date Receive Reinsurance': 'DateReceiveReinsurance',
            'Total Time Reinsurance': 'TotalTimeReinsurance',
            'Date Receive Marketing Revision': 'DateReceiveMarketingRevision',
            'Total Time on Marketing Revision': 'TotalTimeonMarketingRevision',
            'Date Receive Marketing Finished': 'DateReceiveMarketingFinished',
            'Total Time on Marketing Finished': 'TotalTimeonMarketingFinished',
            'Combine Time on PP (Pool Account + PP + Inforcer)': 'CombineTimeonPPPoolAccountPPInforcer',
            'Combine Time on Technic (Combine Time on PP + UW + Reinsurance)': 'CombineTimeonTechnicCombineTimeonPPUWReinsurance',
            'Transaction Iteration': 'TransactionIteration',
            'Insurance Type': 'Insurance Type', # Wait, detected logic needs to match schema json name
            'SLA Combine Time on PP': 'SLACombineTimeonPP',
            'SLA Combine Time on Technic': 'SLACombineTimeonTechnic',
            'SLA Underwriting': 'SLAUnderwriting',
            'SLA Reinsurance': 'SLAReinsurance',
            'Date Receive Marketing Agreed': 'DateReceiveMarketingAgreed',
            'Total Time on Marketing Agreed': 'TotalTimeonMarketingAgreed'
        }
        # Note: 'Insurance Type' in excel maps to 'InsuranceType' in schema
        column_mapping['Insurance Type'] = 'InsuranceType'

        for sheet_name in xls.sheet_names:
            if sheet_name not in target_sheets:
                print(f"Skipping sheet: {sheet_name}")
                continue
                
            print(f"Processing sheet: {sheet_name}")
            try:
                df = pd.read_excel(xls, sheet_name=sheet_name)
                
                # Rename columns
                df = df.rename(columns=column_mapping)
                
                # Add audit columns
                now = datetime.now()
                df['create_date'] = now
                df['modified_date'] = now
                df['create_by'] = 'ETL_Script'
                df['modified_by'] = 'ETL_Script'

                # Ensure only columns in schema are kept (ignoring any extra in excel)
                schema_cols = [field.name for field in schema]
                
                # Initialize missing columns with None if any
                for col in schema_cols:
                    if col not in df.columns:
                        df[col] = None 
                        
                df = df[schema_cols]
                
                # Type Conversions
                # 1. FLOATs
                float_cols = [f.name for f in schema if f.field_type == 'FLOAT']
                for col in float_cols:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                
                # 2. INTEGERs
                int_cols = [f.name for f in schema if f.field_type == 'INTEGER']
                for col in int_cols:
                     df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64') # Int64 allows NaN

                # 3. DATETIME
                # DateCreated is the only one in source. create_date/modified_date already set.
                if 'DateCreated' in df.columns:
                     df['DateCreated'] = pd.to_datetime(df['DateCreated'], errors='coerce')

                # 4. STRINGs
                str_cols = [f.name for f in schema if f.field_type == 'STRING']
                for col in str_cols:
                    df[col] = df[col].astype(str).replace({'nan': None, 'NaN': None, '<NA>': None})
                
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
    upload_logbook()
