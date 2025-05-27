"""Warning: Work in progress."""

# Import Packages
import os
import pdfplumber
from collections import defaultdict
import pandas as pd
from typing import List, Dict, Any, Tuple
import glob

# Extract Tables from PDF
def extract_tables_from_pdf(pdf_path: str) -> List[Dict[str, Any]]:
    schema: List[Dict[str, Any]] = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            tables = page.extract_tables()
            for table in tables:
                for row in table:
                    try:
                        schema.append(row)
                    except Exception:
                        continue
    return schema

# Reconcile Schemas
def reconcile_schemas(schema_list: List[Tuple[str, List[Dict[str, Any]]]]) -> Dict[Tuple[int, int], Dict[str, Any]]:
    reconciled: Dict[Tuple[int, int], Dict[str, Any]] = defaultdict(dict)
    for version, schema in schema_list:
        for field in schema:
            key = (field['begin'], field['end'])  # or item name
            reconciled[key].update({
                "field_name": field['field_name'],
                f"version_{version}": True # type: ignore # Dynamic key name
            })
    return reconciled

# Extract Tables
pdf_paths = glob.glob('./dictionary_files/pdf_layouts/dailyllmni_*.pdf')
# pdf_paths = glob.glob('./dictionary_files/pdf_layouts/factorA1_*.pdf')
# pdf_paths = glob.glob('./dictionary_files/pdf_layouts/ptermot_*.pdf')
# pdf_paths = glob.glob('./dictionary_files/pdf_layouts/issrinfo_*.pdf')
# pdf_paths = glob.glob('./dictionary_files/pdf_layouts/issuers_*.pdf')
# pdf_paths = glob.glob('./dictionary_files/pdf_layouts/mfpldailymni3_*.pdf')
# pdf_paths = glob.glob('./dictionary_files/pdf_layouts/mfpldailymni2_*.pdf')
# pdf_paths = glob.glob('./dictionary_files/pdf_layouts/llmonliq_*.pdf')
df = []
for pdf_path in pdf_paths:
    schema = extract_tables_from_pdf(pdf_path)
    df_a = pd.DataFrame(schema)
    df_a['File'] = os.path.basename(pdf_path)
    # df_a['File'] = os.path.splitext(os.path.basename(pdf_path))[0]
    df.append(df_a)
    del df_a
df = pd.concat(df)

# Keep Only Variable Rows and Non-empty Columns
# df = df.loc[df[0].str.strip().str.isdigit()]
df = df.dropna(axis=1, how='all')
