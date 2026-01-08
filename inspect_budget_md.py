import pandas as pd
import os

file_path = 'budget_2026_v1.xlsx'
output_path = r'C:\Users\msond\.gemini\antigravity\brain\9e9c318c-dec0-4abb-bd7d-e5c8c9f409f5\budget_data_summary.md'

try:
    print(f"Loading {file_path}...")
    xls = pd.ExcelFile(file_path)
    
    with open(output_path, 'w') as f:
        f.write("# Budget Data Summary\n\n")
        f.write(f"**File:** `{file_path}`\n\n")
        f.write("## Sheets\n\n")
        for sheet in xls.sheet_names:
            f.write(f"- {sheet}\n")
        
        f.write("\n## Data Preview\n\n")
        
        # Limit to first 5 sheets to avoid huge file
        for sheet_name in xls.sheet_names[:5]:
            f.write(f"### Sheet: {sheet_name}\n\n")
            df = pd.read_excel(xls, sheet_name=sheet_name, nrows=5)
            # Convert to markdown table
            f.write(df.to_markdown(index=False))
            f.write("\n\n")
            
    print(f"Summary written to {output_path}")

except Exception as e:
    print(f"An error occurred: {e}")
