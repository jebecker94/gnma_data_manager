"""
GNMA Schema Reader

This module provides functionality for extracting, cleaning, and analyzing GNMA (Government National 
Mortgage Association) schema definitions from PDF documents. It supports:

- PDF table extraction and conversion to CSV
- Schema cleaning and standardization
- Multi-version schema combination and reconciliation
- Temporal coverage analysis across schema versions
- Format detection (delimited vs fixed-width)
- Field name standardization and analysis
- COBOL format parsing for data type mapping

Key Components:
- PDF processing using PyMuPDF
- Data cleaning and grouping logic
- Record type extraction and validation
- Analysis and reporting functions

Usage:
    Run as script to process all schemas, or import functions for custom processing.
    Configuration is handled through config.py and command-line flags in main routine.
"""

# Import Packages
from collections import defaultdict
import pandas as pd
from typing import List, Dict, Any, Tuple
from pathlib import Path
import datetime
import pymupdf
import yaml
import re
import config
import os

# Optional Polars support
try:
    import polars as pl
    POLARS_AVAILABLE = True
except ImportError:
    POLARS_AVAILABLE = False

# Configure directory paths
DICTIONARY_DIR = config.DICT_BASE_PATH

# Extract Dates Between Parentheses
def extract_dates_between_parentheses(
    file_path: str | Path,
) -> List[datetime.datetime | None]:
    """
    Extract dates between parentheses from a file path.
    
    Returns:
        List containing [min_date, max_date] as datetime objects, or [None, None] if parsing fails
    """
    
    filename = Path(file_path).name
    
    # Check if file has parentheses
    if '(' not in filename or ')' not in filename:
        return [None, None]
    
    # Try to extract date range from file paths
    try:
        date_range = filename.split('(')[-1].split(')')[0]
        
        # Check if the content looks like a date range (should contain a hyphen)
        if '-' not in date_range:
            return [None, None]
            
        # Convert min and max dates to datetime
        min_date_str, max_date_str = date_range.split('-', 1)  # Split on first hyphen only
        min_date_str = min_date_str.strip()
        max_date_str = max_date_str.strip()
        
        # Check if we have non-empty strings
        if not min_date_str or not max_date_str:
            return [None, None]
        
        min_date = pd.to_datetime(min_date_str)
        
        if max_date_str.lower() == 'present':
            max_date = datetime.datetime(2099, 12, 31)
        else:
            max_date = pd.to_datetime(max_date_str)
            
        return [min_date, max_date]
        
    except Exception:
        return [None, None]


# Extract Record Type Code
def extract_record_type_code(value: str) -> str:
    """
    Extract the record type code from a record type string.
    
    Handles multiple patterns:
    - "Record Type = XX" format
    - "Record Type X = Description" format  
    - "Record Type (X = Description)" format
    - "Record Type (X)" format
    
    Args:
        value: String containing record type information
        
    Returns:
        Extracted code (e.g., 'H', 'PS', '01') or None if no code found
    """
    if not value or not isinstance(value, str):
        return None
        
    value = str(value).strip()
    
    # Skip if this is just the field name "Record Type"
    if value.lower().strip() == 'record type':
        return None
    
    # Pattern 1: "Record Type = XX" or "Record Type=XX"
    match = re.search(r'record\s*type\s*=\s*([A-Z0-9]+)', value, re.IGNORECASE)
    if match:
        return match.group(1).upper()
    
    # Pattern 2: "Record Type (X = Description)" 
    match = re.search(r'record\s*type\s*\(\s*([A-Z0-9]+)\s*=', value, re.IGNORECASE)
    if match:
        return match.group(1).upper()
    
    # Pattern 3: "Record Type X = Description"
    match = re.search(r'record\s*type\s+([A-Z0-9]+)\s*=', value, re.IGNORECASE)
    if match:
        return match.group(1).upper()
    
    # Pattern 4: "Record Type (X)"
    match = re.search(r'record\s*type\s*\(\s*([A-Z0-9]+)\s*\)', value, re.IGNORECASE)
    if match:
        return match.group(1).upper()
    
    return None


# Add Record Type Column
def add_record_type_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add a 'Record_Type' column to the DataFrame by extracting codes from 
    'Data Item' and 'Data Element' columns, then propagating to entire groups.
    
    Args:
        df: DataFrame with schema data
        
    Returns:
        DataFrame with added 'Record_Type' column propagated to entire groups
    """
    if df.empty:
        return df
    
    # Create a copy to avoid modifying the original
    df_copy = df.copy()
    
    # Initialize record_type column
    df_copy['Record_Type'] = None
    
    # Check both Data Item and Data Element columns
    data_cols = [col for col in df_copy.columns if col in ['Data Item', 'Data Element']]
    
    for col in data_cols:
        if col in df_copy.columns:
            # Extract record type codes
            df_copy['Record_Type'] = df_copy.apply(
                lambda row: extract_record_type_code(row[col]) if pd.isna(row['Record_Type']) else row['Record_Type'],
                axis=1
            )
    
    # If we have group_id column, propagate record types to entire groups
    if 'group_id' in df_copy.columns:
        df_copy = propagate_record_types_to_groups(df_copy)
    
    return df_copy


# Propagate Record Types to Groups
def propagate_record_types_to_groups(df: pd.DataFrame) -> pd.DataFrame:
    """
    Propagate record types to all rows within each group_id.
    
    First validates that each group has at most one unique record type,
    then fills all rows in each group with that record type.
    
    Args:
        df: DataFrame with group_id and record_type columns
        
    Returns:
        DataFrame with record_type propagated to entire groups
    """
    if 'group_id' not in df.columns or 'Record_Type' not in df.columns:
        return df
    
    df_copy = df.copy()
    
    # Check each group for record type consistency
    validation_results = []
    groups_with_conflicts = []
    
    for group_id in df_copy['group_id'].unique():
        group_data = df_copy[df_copy['group_id'] == group_id]
        
        # Get non-null record types in this group
        group_record_types = group_data['Record_Type'].dropna().unique()
        
        validation_results.append({
            'group_id': group_id,
            'record_types': list(group_record_types),
            'count': len(group_record_types),
            'rows_in_group': len(group_data)
        })
        
        # Check for conflicts (more than one record type per group)
        if len(group_record_types) > 1:
            groups_with_conflicts.append({
                'group_id': group_id,
                'record_types': list(group_record_types),
                'rows': len(group_data)
            })
    
    # Report validation results
    total_groups = len(validation_results)
    groups_with_records = sum(1 for r in validation_results if r['count'] > 0)
    groups_with_conflicts_count = len(groups_with_conflicts)
    
    print(f"Record type validation:")
    print(f"  Total groups: {total_groups}")
    print(f"  Groups with record types: {groups_with_records}")
    print(f"  Groups with conflicts: {groups_with_conflicts_count}")
    
    if groups_with_conflicts:
        print(f"Groups with multiple record types:")
        for conflict in groups_with_conflicts[:5]:  # Show first 5 conflicts
            print(f"    Group {conflict['group_id']}: {conflict['record_types']} ({conflict['rows']} rows)")
        if len(groups_with_conflicts) > 5:
            print(f"    ... and {len(groups_with_conflicts) - 5} more conflicts")
    
    # Propagate record types to all rows in each group
    for group_id in df_copy['group_id'].unique():
        group_mask = df_copy['group_id'] == group_id
        group_record_types = df_copy[group_mask]['Record_Type'].dropna().unique()
        
        if len(group_record_types) == 1:
            # Single record type - propagate to all rows in group
            df_copy.loc[group_mask, 'Record_Type'] = group_record_types[0]
        elif len(group_record_types) > 1:
            # Multiple record types - use the first one found (could be improved)
            df_copy.loc[group_mask, 'Record_Type'] = group_record_types[0]
            # Note: In practice, you might want different logic here
    
    return df_copy


# Clean Individual DataFrame
def clean_extracted_dataframe(
    df: pd.DataFrame, 
    filename: str,
    add_record_types: bool = True,
    verbose: bool = False
) -> pd.DataFrame:
    """
    Apply cleaning operations to a single extracted DataFrame.
    
    This includes:
    - Column name standardization
    - Grouping by Item resets
    - Filtering unwanted rows and groups
    - Record type extraction
    
    Args:
        df: Raw DataFrame from PDF extraction
        filename: Source filename for context
        add_record_types: Whether to extract record types
        verbose: Whether to print detailed output
        
    Returns:
        Cleaned DataFrame ready for saving
    """
    if df.empty:
        return df
    
    if verbose:
        print(f"    Cleaning extracted data from {filename}")
        print(f"    Initial rows: {len(df)}")
    
    # Note: Column names already standardized during individual table extraction
    
    # Step 1: Add grouping logic for when Item field resets to 1
    if 'Item' in df.columns:
        # Show sample of Item values for debugging
        if verbose:
            sample_items = df['Item'].dropna().head(5).tolist()
            print(f"    Sample Item values: {sample_items}")
        
        # Extract numeric part from Item column (handles cases like "P1", "P2", etc.)
        # This regex finds the first sequence of digits in each Item value
        df['Item_numeric'] = df['Item'].astype(str).str.extract(r'(\d+)', expand=False)
        df['Item_numeric'] = pd.to_numeric(df['Item_numeric'], errors='coerce')
        
        # Filter out rows where we couldn't extract a number
        valid_item_mask = df['Item_numeric'].notna()
        df = df[valid_item_mask]
        
        if verbose and not valid_item_mask.all():
            removed_non_numeric = (~valid_item_mask).sum()
            print(f"    Removed {removed_non_numeric} rows with non-numeric Item values")
        
        if len(df) > 0:
            # Show range of extracted numeric values
            if verbose:
                min_item = df['Item_numeric'].min()
                max_item = df['Item_numeric'].max()
                print(f"    Extracted Item numbers range: {min_item} to {max_item}")
            
            # Create groups based on Item resets (vectorized approach)
            item_resets = (df['Item_numeric'] == 1) & (df['Item_numeric'].shift(1, fill_value=1) != 1)
            df['group_id'] = item_resets.cumsum() - 1  # Start from 0
            
            # Clean up temporary column
            df = df.drop('Item_numeric', axis=1)
            
            if verbose:
                group_counts = df['group_id'].value_counts().sort_index()
                print(f"    Created {len(group_counts)} groups based on Item field resets")
    
    # Step 2: Filter out unwanted rows
    initial_row_count = len(df)
    
    # Remove rows where 'Data Item' = 'Length of Record'
    if 'Data Item' in df.columns:
        mask = df['Data Item'] != 'Length of Record'
        df = df[mask]
        removed_length_records = initial_row_count - len(df)
        if verbose and removed_length_records > 0:
            print(f"    Removed {removed_length_records} 'Length of Record' rows")
    
    # Remove rows where all original schema fields are missing
    original_schema_columns = [col for col in df.columns 
                             if col not in ['File', 'Min_Date', 'Max_Date', 'group_id']]
    if original_schema_columns:
        mask = df[original_schema_columns].notna().any(axis=1)
        rows_before = len(df)
        df = df[mask]
        removed_empty_rows = rows_before - len(df)
        if verbose and removed_empty_rows > 0:
            print(f"    Removed {removed_empty_rows} rows with all original fields missing")
    
    # Step 3: Filter out groups without proper 'record type' references
    if 'group_id' in df.columns and len(df) > 0:
        groups_before = df['group_id'].nunique()
        
        # Check for 'record type' substring in 'Data Item' and 'Data Element' columns
        # but exclude rows where it's exactly "Record Type" with no additional code
        record_type_mask = pd.Series(False, index=df.index)
        
        for col in ['Data Item', 'Data Element']:
            if col in df.columns:
                col_str = df[col].astype(str).str.lower()
                # Find rows where column contains "record type" but is not exactly "record type"
                contains_rt = col_str.str.contains('record type', na=False)
                not_exact_rt = col_str.str.strip() != 'record type'
                record_type_mask = record_type_mask | (contains_rt & not_exact_rt)
        
        # Check if ANY rows have record type references
        total_record_type_rows = record_type_mask.sum()
        
        if total_record_type_rows > 0:
            # PDF has record type references - apply filtering
            valid_groups = df[record_type_mask]['group_id'].unique()
            df = df[df['group_id'].isin(valid_groups)]
            
            groups_after = df['group_id'].nunique() if len(df) > 0 else 0
            groups_removed = groups_before - groups_after
            
            if verbose and groups_removed > 0:
                print(f"    Removed {groups_removed} groups without proper 'record type' references (excluding exact 'Record Type' matches)")
        else:
            # PDF has no record type references - keep all groups (single record type PDF)
            if verbose:
                print(f"    No 'record type' references found - keeping all {groups_before} groups (single record type PDF)")
    
    # Step 4: Add record types if requested (only for multi-record type PDFs)
    has_record_type_references = False
    if 'group_id' in df.columns and len(df) > 0:
        # Check if this PDF has record type references
        record_type_check_mask = pd.Series(False, index=df.index)
        for col in ['Data Item', 'Data Element']:
            if col in df.columns:
                col_str = df[col].astype(str).str.lower()
                contains_rt = col_str.str.contains('record type', na=False)
                not_exact_rt = col_str.str.strip() != 'record type'
                record_type_check_mask = record_type_check_mask | (contains_rt & not_exact_rt)
        has_record_type_references = record_type_check_mask.sum() > 0
    
    if add_record_types and not df.empty and has_record_type_references:
        # Only add record types for multi-record type PDFs
        df = add_record_type_column(df)
        
        if verbose and 'Record_Type' in df.columns:
            record_type_counts = df['Record_Type'].value_counts()
            print(f"    Extracted record types: {len(record_type_counts)} unique types")
    elif add_record_types and not df.empty and not has_record_type_references:
        if verbose:
            print(f"    Skipping record type extraction for single record type PDF")
    
    # Step 5: Validate and clean up group_id column
    if 'group_id' in df.columns and not df.empty:
        unique_groups = df['group_id'].nunique()
        
        if unique_groups == 1:
            # Only one group - group_id is redundant, drop it
            df = df.drop('group_id', axis=1)
            if verbose:
                print(f"    Only one group found, dropped group_id column")
        elif 'Record_Type' in df.columns:
            # Multiple groups with record types - validate mapping
            record_type_to_groups = df.groupby('Record_Type')['group_id'].nunique()
            groups_to_record_types = df.groupby('group_id')['Record_Type'].nunique()
            
            # Validation: each record type should map to exactly one group, and vice versa
            max_groups_per_record_type = record_type_to_groups.max()
            max_record_types_per_group = groups_to_record_types.max()
            
            if max_groups_per_record_type == 1 and max_record_types_per_group == 1:
                # Perfect 1:1 mapping - we can safely drop group_id
                df = df.drop('group_id', axis=1)
                if verbose:
                    print(f"    Validated 1:1 record_type↔group_id mapping, dropped group_id column")
            else:
                # Keep group_id for debugging/analysis
                if verbose:
                    print(f"    Complex record_type↔group_id mapping detected:")
                    print(f"      Max groups per record type: {max_groups_per_record_type}")
                    print(f"      Max record types per group: {max_record_types_per_group}")
                    print(f"      Keeping group_id column for analysis")
        else:
            # Multiple groups but no record types - keep group_id for structure
            if verbose:
                print(f"    Multiple groups ({unique_groups}) but no record types - keeping group_id column")
    
    if verbose:
        print(f"    Final rows: {len(df)} (removed {initial_row_count - len(df)} total)")
    
    return df


# Extract Tables from PDF
def extract_tables_from_pdf(
    pdf_path: str,
    prefix: str,
    save_document_data: bool = False,
    overwrite: bool = True,
    verbose: bool = False,
) -> None:
    """
    Extract tables from PDF using PyMuPDF.
    """

    pdf_path = Path(pdf_path)
    
    if verbose:
        print(f"Processing PDF: {pdf_path.name}")

    # Early check: if we're saving data and file exists and not overwriting, skip processing
    if save_document_data:
        save_file = DICTIONARY_DIR / f'clean/{prefix}' / pdf_path.with_suffix('.csv').name
        if save_file.exists() and not overwrite:
            if verbose:
                print(f"  Skipping - CSV already exists: {save_file.name}")
            return []  # Return empty list since we're not processing

    # PyMuPDF
    schema: List[pd.DataFrame] = []
    total_standardizations = 0
    tables_with_item = 0
    tables_without_item = 0
    
    with pymupdf.open(pdf_path) as pdf:
        for page_num in range(len(pdf)):
            page = pdf[page_num]
            table_list = list(page.find_tables(vertical_strategy='lines_strict', horizontal_strategy='lines_strict'))
            
            for table in table_list:
                df = table.to_pandas()
                
                # Count columns before standardization
                original_columns = set(df.columns)
                # Standardize column names immediately after extraction
                df = standardize_column_names(df, verbose=False)
                # Count standardizations
                standardized_columns = set(df.columns)
                if original_columns != standardized_columns:
                    total_standardizations += len(original_columns - standardized_columns)
                
                # Track table types for reporting
                has_item = "item" in df.columns.str.lower().values
                if has_item:
                    tables_with_item += 1
                else:
                    tables_without_item += 1
                
                # KEEP ALL TABLES - don't filter by Item column presence
                # This fixes the multi-page table issue where continuation tables lack headers
                # NOTE: Eventually we need to implement custom logic to ensure that tables spanning multiple pages are combined appropriately
                schema.append(df)

    if verbose:
        total_tables = tables_with_item + tables_without_item
        print(f"  Found {total_tables} total tables:")
        print(f"    - {tables_with_item} tables with 'Item' column")
        print(f"    - {tables_without_item} tables without 'Item' column (kept for multi-page support)")
        if total_standardizations > 0:
            print(f"  Standardized {total_standardizations} column names during table extraction")
        elif schema:
            print(f"  No column standardization needed - names already clean")

    # Save Document Data to Separate CSV File
    if save_document_data and schema:
        df = pd.concat(schema, ignore_index=True)
        df['File'] = pdf_path.name
        min_date, max_date = extract_dates_between_parentheses(pdf_path)
        df['Min_Date'] = min_date
        df['Max_Date'] = max_date
        
        # Apply cleaning operations to the extracted data
        df = clean_extracted_dataframe(
            df, 
            filename=pdf_path.name,
            add_record_types=True,
            verbose=verbose
        )
        
        # Only save if we have data after cleaning
        if not df.empty:
            save_file = DICTIONARY_DIR / f'clean/{prefix}' / pdf_path.with_suffix('.csv').name
            
            # Create directory if it doesn't exist
            save_file.parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(save_file, index=False)
            if verbose:
                print(f"  Saved cleaned data to: {save_file.name}")
        else:
            if verbose:
                print(f"  No data remaining after cleaning - skipping save")


# Reconcile Schemas
def reconcile_schemas(
    schema_list: List[Tuple[str, List[Dict[str, Any]]]],
) -> Dict[Tuple[int, int], Dict[str, Any]]:
    """
    Reconcile schemas from different versions of the same document.
    """

    reconciled: Dict[Tuple[int, int], Dict[str, Any]] = defaultdict(dict)
    for version, schema in schema_list:
        for field in schema:
            key = (field['begin'], field['end'])  # or item name
            reconciled[key].update({
                "field_name": field['field_name'],
                f"version_{version}": True # type: ignore # Dynamic key name
            })
    return reconciled


# Extract Schemas from PDFs to CSV Files
def extract_schemas_to_csv(
    prefix: str, 
    overwrite: bool = True,
    verbose: bool = False,
) -> None:
    """
    Extract tables from PDF files and save as individual CSV files in the clean directory.
    """
    
    if verbose:
        print(f"\n=== Extracting schemas for prefix: {prefix} ===")

    # Get All Layout Files
    pdf_directory = DICTIONARY_DIR / f'raw/{prefix}'
    pdf_paths = list(pdf_directory.glob(f'{prefix}_*.pdf'))
    
    if verbose:
        print(f"Found {len(pdf_paths)} PDF files")
    
    # Process each PDF file
    for i, pdf_path in enumerate(pdf_paths, 1):
        if verbose:
            print(f"\n[{i}/{len(pdf_paths)}] Processing: {pdf_path.name}")
        
        # Extract tables and save to CSV
        extract_tables_from_pdf(
            pdf_path, 
            prefix, 
            save_document_data=True, 
            overwrite=overwrite, 
            verbose=verbose
        )


# Standardize Column Names
def standardize_column_names(
    df: pd.DataFrame,
    verbose: bool = False,
) -> pd.DataFrame:
    """
    Standardize column names to handle inconsistencies.
    
    Fixes issues like:
    - "Max\nLength" → "Max Length"
    - Extra whitespace
    - Other formatting inconsistencies
    - Handles potential duplicate columns after standardization
    
    Args:
        df: DataFrame with potentially inconsistent column names
        verbose: Whether to print standardization messages
        
    Returns:
        DataFrame with standardized column names
    """
    if df.empty:
        return df
    
    # Create a copy to avoid modifying the original
    df_copy = df.copy()
    
    # Create mapping of old column names to new standardized names
    column_mapping = {}
    standardized_names = []
    
    for col in df_copy.columns:
        # Start with the original column name
        new_col = str(col)
        
        # Remove newlines and extra whitespace
        new_col = new_col.replace('\n', ' ').replace('\r', ' ')
        
        # Normalize multiple spaces to single space
        new_col = ' '.join(new_col.split())
        
        # Strip leading/trailing whitespace
        new_col = new_col.strip()
        
        # Handle potential duplicates by adding suffix
        original_new_col = new_col
        counter = 1
        while new_col in standardized_names:
            new_col = f"{original_new_col}_{counter}"
            counter += 1
        
        standardized_names.append(new_col)
        
        # Store mapping if different
        if new_col != col:
            column_mapping[col] = new_col
    
    # Apply the column name changes
    if column_mapping:
        df_copy = df_copy.rename(columns=column_mapping)
        
        # Log the changes if verbose mode is enabled
        if verbose:
            print(f"  Standardized {len(column_mapping)} column names:")
            for old_col, new_col in column_mapping.items():
                print(f"    '{old_col}' → '{new_col}'")
    
    return df_copy


# Combine Schemas from Clean CSV Files
def combine_schemas(
    prefix: str, 
    verbose: bool = False,
    add_record_types: bool = False,
) -> pd.DataFrame:
    """
    Combine existing cleaned CSV files from the clean directory into a single DataFrame.
    
    Note: CSV files should already be cleaned by the extract_tables_from_pdf process.
    This function simply concatenates the pre-cleaned files.
    
    Args:
        prefix: File prefix to process
        verbose: Whether to print detailed output
        add_record_types: Legacy parameter (ignored - record types already in cleaned files)
        
    Returns:
        DataFrame with combined schema data
    """

    if verbose:
        print(f"\n=== Combining clean CSV files for prefix: {prefix} ===")

    # Get All Clean CSV Files
    csv_directory = DICTIONARY_DIR / f'clean/{prefix}'
    
    if not csv_directory.exists():
        if verbose:
            print(f"Clean directory does not exist: {csv_directory}")
        return pd.DataFrame()
    
    csv_paths = list(csv_directory.glob(f'{prefix}_*.csv'))
    
    if verbose:
        print(f"Found {len(csv_paths)} CSV files")
    
    if not csv_paths:
        if verbose:
            print(f"No CSV files found for prefix: {prefix}")
        return pd.DataFrame()
    
    # Read and combine all CSV files
    df_list = []
    for i, csv_path in enumerate(csv_paths, 1):
        if verbose:
            print(f"[{i}/{len(csv_paths)}] Reading: {csv_path.name}")
        
        try:
            df = pd.read_csv(csv_path)
            if not df.empty:
                df_list.append(df)
                if verbose:
                    print(f"  Added {len(df)} rows")
            else:
                if verbose:
                    print(f"  File is empty")
        except Exception as e:
            if verbose:
                print(f"  Error reading file: {e}")
    
    # Combine all results (CSV files are already cleaned individually)
    if df_list:
        combined_df = pd.concat(df_list, ignore_index=True)
        
        if verbose:
            print(f"\nCombined dataset: {len(combined_df)} total rows from {len(df_list)} files")
            
            # Show group distribution if available
            if 'group_id' in combined_df.columns:
                group_counts = combined_df['group_id'].value_counts().sort_index()
                print(f"Group distribution: {len(group_counts)} total groups")
                # Show first few groups as sample
                for group_id, count in list(group_counts.items())[:5]:
                    print(f"  Group {group_id}: {count} rows")
                if len(group_counts) > 5:
                    print(f"  ... and {len(group_counts) - 5} more groups")
            
            # Show record type distribution if available
            if 'Record_Type' in combined_df.columns:
                record_type_counts = combined_df['Record_Type'].value_counts()
                print(f"\nRecord type distribution:")
                for rt, count in record_type_counts.items():
                    print(f"  {rt}: {count} rows")
        
        return combined_df
    else:
        if verbose:
            print(f"\nNo data found for prefix: {prefix}")
        return pd.DataFrame()


# Organize Schemas by Record Type
def organize_schemas_by_record_type(
    verbose: bool = False,
) -> None:
    """
    Create subfolders for each Record_Type within data/clean/prefix folders.
    Only processes prefixes where the combined schema file has a Record_Type column.
    
    Args:
        verbose: Whether to print detailed output
    """
    
    if verbose:
        print(f"\n=== Organizing schemas by Record_Type ===")
    
    # Load YAML file to get all prefixes
    yaml_file = DICTIONARY_DIR / "prefix_dictionary.yaml"
    with open(yaml_file, "r") as f:
        prefix_dict = yaml.safe_load(f)
    
    # Process each prefix
    for prefix in prefix_dict.keys():
        if verbose:
            print(f"\nProcessing prefix: {prefix}")
        
        # Check if combined schema file exists
        combined_file = DICTIONARY_DIR / f'combined/{prefix}_combined_schema.csv'
        if not combined_file.exists():
            if verbose:
                print(f"  No combined schema file found: {combined_file.name}")
            continue
        
        try:
            # Load combined schema file
            df = pd.read_csv(combined_file)
            
            if df.empty:
                if verbose:
                    print(f"  Combined schema file is empty")
                continue
            
            # Check if Record_Type column exists
            if 'Record_Type' not in df.columns:
                if verbose:
                    print(f"  No Record_Type column found - skipping organization")
                continue
            
            # Get unique record types
            record_types = df['Record_Type'].dropna().unique()
            
            if len(record_types) == 0:
                if verbose:
                    print(f"  No record types found in Record_Type column")
                continue
            
            if verbose:
                print(f"  Found {len(record_types)} record types: {list(record_types)}")
            
            # Create subfolders for each record type
            base_clean_dir = DATA_DIR / f'clean/{prefix}'
            
            for record_type in record_types:
                record_type_dir = base_clean_dir / str(record_type)
                record_type_dir.mkdir(parents=True, exist_ok=True)
                
                if verbose:
                    print(f"    Created folder: {record_type_dir.relative_to(Path('.'))}")
            
            if verbose:
                print(f"  Success: Organized {prefix} by {len(record_types)} record types")
                
        except Exception as e:
            if verbose:
                print(f"  Error processing {prefix}: {e}")
    
    if verbose:
        print(f"\nCompleted schema organization by Record_Type")


def analyze_temporal_coverage(verbose: bool = False) -> pd.DataFrame:
    """
    Analyze temporal coverage across all combined schema files to identify monthly gaps.
    
    Returns:
        pd.DataFrame: Summary of coverage with gaps identified for each prefix
    """
    combined_dir = DICTIONARY_DIR / 'combined'
    
    if not combined_dir.exists():
        print(f"Combined schemas directory not found: {combined_dir}")
        return pd.DataFrame()
    
    # Find all combined schema files
    schema_files = list(combined_dir.glob('*_combined_schema.csv'))
    
    if not schema_files:
        print(f"No combined schema files found in {combined_dir}")
        return pd.DataFrame()
    
    coverage_results = []
    
    print(f"Analyzing temporal coverage for {len(schema_files)} prefixes...")
    
    for schema_file in schema_files:
        prefix = schema_file.stem.replace('_combined_schema', '')
        
        try:
            # Load the schema file
            df = pd.read_csv(schema_file)
            
            if 'Min_Date' not in df.columns or 'Max_Date' not in df.columns:
                print(f"Warning: {prefix}: Missing date columns")
                continue
                
            # Convert date columns to datetime
            df['Min_Date'] = pd.to_datetime(df['Min_Date'])
            df['Max_Date'] = pd.to_datetime(df['Max_Date'])
            
            # Replace far future dates (2099) with current date for analysis
            current_date = pd.Timestamp.now().normalize()
            df['Max_Date_Analysis'] = df['Max_Date'].where(
                df['Max_Date'] < pd.Timestamp('2099-01-01'), 
                current_date
            )
            
            # Get unique date ranges from files
            date_ranges = df[['Min_Date', 'Max_Date_Analysis']].drop_duplicates().sort_values('Min_Date')
            
            if len(date_ranges) == 0:
                continue
                
            # Calculate overall coverage
            overall_min = date_ranges['Min_Date'].min()
            overall_max = date_ranges['Max_Date_Analysis'].max()
            
            # Generate expected monthly range
            expected_months = pd.date_range(
                start=overall_min.replace(day=1), 
                end=overall_max.replace(day=1), 
                freq='MS'  # Month start
            )
            
            # Generate covered months from all date ranges
            covered_months = set()
            for _, row in date_ranges.iterrows():
                range_months = pd.date_range(
                    start=row['Min_Date'].replace(day=1),
                    end=row['Max_Date_Analysis'].replace(day=1),
                    freq='MS'
                )
                covered_months.update(range_months)
            
            # Find gaps
            missing_months = []
            for month in expected_months:
                if month not in covered_months:
                    missing_months.append(month)
            
            # Calculate gap details
            gap_periods = []
            if missing_months:
                missing_months.sort()
                gap_start = missing_months[0]
                gap_end = missing_months[0]
                
                for i in range(1, len(missing_months)):
                    # Check if this month is consecutive to the previous
                    if missing_months[i] == missing_months[i-1] + pd.DateOffset(months=1):
                        gap_end = missing_months[i]
                    else:
                        # End current gap and start new one
                        gap_periods.append((gap_start, gap_end))
                        gap_start = missing_months[i]
                        gap_end = missing_months[i]
                
                # Add the last gap
                gap_periods.append((gap_start, gap_end))
            
            # Create summary
            total_months_expected = len(expected_months)
            total_months_covered = len(covered_months)
            total_months_missing = len(missing_months)
            coverage_pct = (total_months_covered / total_months_expected) * 100 if total_months_expected > 0 else 0
            
            # Format gap periods for display
            gap_summary = []
            for gap_start, gap_end in gap_periods:
                if gap_start == gap_end:
                    gap_summary.append(gap_start.strftime('%Y-%m'))
                else:
                    gap_summary.append(f"{gap_start.strftime('%Y-%m')} to {gap_end.strftime('%Y-%m')}")
            
            coverage_results.append({
                'Prefix': prefix,
                'Files_Count': len(date_ranges),
                'Coverage_Start': overall_min.strftime('%Y-%m-%d'),
                'Coverage_End': overall_max.strftime('%Y-%m-%d'),
                'Total_Months_Expected': total_months_expected,
                'Total_Months_Covered': total_months_covered,
                'Total_Months_Missing': total_months_missing,
                'Coverage_Percentage': round(coverage_pct, 1),
                'Gap_Periods': '; '.join(gap_summary) if gap_summary else 'None',
                'Has_Gaps': len(missing_months) > 0
            })
            
            if verbose:
                print(f"{prefix}:")
                print(f"   Coverage: {overall_min.strftime('%Y-%m')} to {overall_max.strftime('%Y-%m')}")
                print(f"   Files: {len(date_ranges)}")
                print(f"   Coverage: {total_months_covered}/{total_months_expected} months ({coverage_pct:.1f}%)")
                if gap_summary:
                    print(f"   Gaps: {'; '.join(gap_summary)}")
                else:
                    print(f"Complete coverage")
                    
        except Exception as e:
            print(f"Error analyzing {prefix}: {str(e)}")
            continue
    
    # Convert to DataFrame and sort by coverage percentage
    results_df = pd.DataFrame(coverage_results)
    if not results_df.empty:
        results_df = results_df.sort_values(['Has_Gaps', 'Coverage_Percentage'], ascending=[True, False])
    
    return results_df

def print_coverage_summary(coverage_df: pd.DataFrame) -> None:
    """
    Print a formatted summary of temporal coverage analysis.
    """
    if coverage_df.empty:
        print("No coverage data to summarize")
        return
    
    total_prefixes = len(coverage_df)
    prefixes_with_gaps = coverage_df['Has_Gaps'].sum()
    prefixes_complete = total_prefixes - prefixes_with_gaps
    
    print(f"\n" + "="*80)
    print(f"TEMPORAL COVERAGE SUMMARY")
    print(f"="*80)
    print(f"Total Prefixes Analyzed: {total_prefixes}")
    print(f"Complete Coverage: {prefixes_complete}")
    print(f"With Gaps: {prefixes_with_gaps}")
    print(f"="*80)
    
    if prefixes_with_gaps > 0:
        print(f"PREFIXES WITH GAPS:")
        print(f"{'-'*80}")
        gap_df = coverage_df[coverage_df['Has_Gaps']].copy()
        for _, row in gap_df.iterrows():
            print(f"{row['Prefix']:<20} | {row['Coverage_Percentage']:>5.1f}% | {row['Gap_Periods']}")
    
    if prefixes_complete > 0:
        print(f"PREFIXES WITH COMPLETE COVERAGE:")
        print(f"{'-'*80}")
        complete_df = coverage_df[~coverage_df['Has_Gaps']].copy()
        for _, row in complete_df.iterrows():
            print(f"{row['Prefix']:<20} | {row['Coverage_Start']} to {row['Coverage_End']}")
    
    print(f"\n" + "="*80)

def analyze_schema_formats(verbose: bool = False) -> pd.DataFrame:
    """
    Analyze schema files to identify potential delimited vs fixed-width file formats
    based on column structure patterns.
    
    Returns:
        pd.DataFrame: Analysis of schema formats with file type predictions
    """
    combined_dir = DICTIONARY_DIR / 'combined'
    
    if not combined_dir.exists():
        print(f"Combined schemas directory not found: {combined_dir}")
        return pd.DataFrame()
    
    # Find all combined schema files
    schema_files = list(combined_dir.glob('*_combined_schema.csv'))
    
    if not schema_files:
        print(f"No combined schema files found in {combined_dir}")
        return pd.DataFrame()
    
    format_results = []
    
    print(f"Analyzing schema formats for {len(schema_files)} prefixes...")
    
    for schema_file in schema_files:
        prefix = schema_file.stem.replace('_combined_schema', '')
        
        try:
            # Read just the header to analyze column structure
            df_header = pd.read_csv(schema_file, nrows=0)  # Just get column names
            columns = list(df_header.columns)
            
            # Analyze column patterns
            has_max_length = 'Max Length' in columns
            has_length = 'Length' in columns
            has_begin_end = 'Begin' in columns and 'End' in columns
            has_format = 'Format' in columns
            has_description = 'Description' in columns
            has_definition = 'Definition' in columns
            has_remarks = 'Remarks' in columns
            has_data_element = 'Data Element' in columns
            has_data_item = 'Data Item' in columns
            
            # Read a few rows to check for additional patterns
            df_sample = pd.read_csv(schema_file, nrows=10)
            
            # Count total columns
            total_columns = len(columns)
            
            # Predict file format based on patterns
            format_indicators = []
            confidence_score = 0
            
            if has_max_length:
                format_indicators.append("Max Length column")
                confidence_score += 3
            
            if has_begin_end:
                format_indicators.append("Begin/End columns")  
                confidence_score -= 3  # This suggests fixed-width
            
            if has_format:
                format_indicators.append("Format column")
                confidence_score += 2
            
            if has_description and has_definition:
                format_indicators.append("Description/Definition columns")
                confidence_score += 1
            
            if has_remarks and not (has_description or has_definition):
                format_indicators.append("Remarks column only")
                confidence_score -= 1  # Suggests older/simpler fixed-width format
            
            if has_data_element:
                format_indicators.append("Data Element naming")
                confidence_score += 1
            elif has_data_item:
                format_indicators.append("Data Item naming")
                confidence_score -= 1
            
            # Make prediction
            if confidence_score >= 3:
                predicted_format = "DELIMITED"
            elif confidence_score <= -2:
                predicted_format = "FIXED-WIDTH"
            else:
                predicted_format = "UNCERTAIN"
            
            # Check for any explicit delimiter indicators in the data
            delimiter_hints = []
            if not df_sample.empty:
                for col in ['Format', 'Description', 'Definition', 'Remarks']:
                    if col in df_sample.columns:
                        sample_text = ' '.join(df_sample[col].dropna().astype(str).head(10))
                        if 'delimit' in sample_text.lower():
                            delimiter_hints.append(f"'{col}' mentions delimiter")
                        if 'fixed' in sample_text.lower() and 'width' in sample_text.lower():
                            delimiter_hints.append(f"'{col}' mentions fixed width")
                        if 'pipe' in sample_text.lower() or '|' in sample_text:
                            delimiter_hints.append(f"'{col}' mentions pipe delimiter")
                        if 'comma' in sample_text.lower() and 'separat' in sample_text.lower():
                            delimiter_hints.append(f"'{col}' mentions comma separator")
            
            format_results.append({
                'Prefix': prefix,
                'Total_Columns': total_columns,
                'Has_Max_Length': has_max_length,
                'Has_Length': has_length,
                'Has_Begin_End': has_begin_end,
                'Has_Format': has_format,
                'Has_Description': has_description,
                'Has_Definition': has_definition,
                'Has_Remarks': has_remarks,
                'Has_Data_Element': has_data_element,
                'Has_Data_Item': has_data_item,
                'Confidence_Score': confidence_score,
                'Predicted_Format': predicted_format,
                'Format_Indicators': '; '.join(format_indicators),
                'Delimiter_Hints': '; '.join(delimiter_hints) if delimiter_hints else 'None',
                'Column_Structure': ', '.join(columns[:6])  # First 6 columns for reference
            })
            
            if verbose:
                print(f"{prefix}:")
                print(f"   Predicted Format: {predicted_format} (score: {confidence_score})")
                print(f"   Key Indicators: {'; '.join(format_indicators)}")
                if delimiter_hints:
                    print(f"   Delimiter Hints: {'; '.join(delimiter_hints)}")
                    
        except Exception as e:
            print(f"Error analyzing {prefix}: {str(e)}")
            continue
    
    # Convert to DataFrame and sort by prediction confidence
    results_df = pd.DataFrame(format_results)
    if not results_df.empty:
        results_df = results_df.sort_values(['Predicted_Format', 'Confidence_Score'], 
                                           ascending=[True, False])
    
    return results_df

def print_format_analysis_summary(format_df: pd.DataFrame) -> None:
    """
    Print a formatted summary of schema format analysis.
    """
    if format_df.empty:
        print("No format analysis data to summarize")
        return
    
    total_prefixes = len(format_df)
    delimited_count = (format_df['Predicted_Format'] == 'DELIMITED').sum()
    fixed_width_count = (format_df['Predicted_Format'] == 'FIXED-WIDTH').sum()
    uncertain_count = (format_df['Predicted_Format'] == 'UNCERTAIN').sum()
    
    print(f"\n" + "="*80)
    print(f"SCHEMA FORMAT ANALYSIS SUMMARY")
    print(f"="*80)
    print(f"Total Prefixes Analyzed: {total_prefixes}")
    print(f"Predicted Delimited: {delimited_count}")
    print(f"Predicted Fixed-Width: {fixed_width_count}")
    print(f"Uncertain: {uncertain_count}")
    print(f"="*80)
    
    if delimited_count > 0:
        print(f"LIKELY DELIMITED FILES:")
        print(f"{'-'*80}")
        delimited_df = format_df[format_df['Predicted_Format'] == 'DELIMITED'].copy()
        for _, row in delimited_df.iterrows():
            print(f"{row['Prefix']:<20} | Score: {row['Confidence_Score']:>2} | {row['Format_Indicators']}")
    
    if fixed_width_count > 0:
        print(f"LIKELY FIXED-WIDTH FILES:")
        print(f"{'-'*80}")
        fixed_df = format_df[format_df['Predicted_Format'] == 'FIXED-WIDTH'].copy()
        for _, row in fixed_df.iterrows():
            print(f"{row['Prefix']:<20} | Score: {row['Confidence_Score']:>2} | {row['Format_Indicators']}")
    
    if uncertain_count > 0:
        print(f"UNCERTAIN FORMAT:")
        print(f"{'-'*80}")
        uncertain_df = format_df[format_df['Predicted_Format'] == 'UNCERTAIN'].copy()
        for _, row in uncertain_df.iterrows():
            print(f"{row['Prefix']:<20} | Score: {row['Confidence_Score']:>2} | {row['Format_Indicators']}")
    
    print(f"\n" + "="*80)

def count_field_names_from_clean_schemas(clean_dir: str | Path = 'dictionary_files/clean', 
                                        output_dir: str | Path = 'output'):
    """
    Extract and count all Data Item and Data Element values from individual CSV schemas.
    
    Args:
        clean_dir: Directory containing cleaned schema CSV files (default: 'dictionary_files/clean')
        output_dir: Directory to save results (default: 'output')
        
    Returns:
        pd.DataFrame: Field name counts sorted by frequency
        
    Saves results to {output_dir}/field_name_counts.csv.
    """
    clean_dir = Path(clean_dir)
    output_dir = Path(output_dir)
    all_field_names = []
    
    # Find all CSV files in clean directory and subdirectories
    csv_files = list(clean_dir.rglob('*.csv'))
    
    for csv_file in csv_files:
        try:
            df = pd.read_csv(csv_file)
            
            # Extract Data Item values
            if 'Data Item' in df.columns:
                data_items = df['Data Item'].dropna().astype(str)
                all_field_names.extend([item.strip() for item in data_items if item and item != 'nan'])
            
            # Extract Data Element values
            if 'Data Element' in df.columns:
                data_elements = df['Data Element'].dropna().astype(str)
                all_field_names.extend([element.strip() for element in data_elements if element and element != 'nan'])
                
        except Exception:
            continue
    
    # Count occurrences
    field_counts = {}
    for name in all_field_names:
        field_counts[name] = field_counts.get(name, 0) + 1
    
    # Convert to DataFrame and save
    results_df = pd.DataFrame([
        {'Field_Name': name, 'Count': count} 
        for name, count in field_counts.items()
    ]).sort_values('Count', ascending=False)
    
    # Save to output directory
    output_dir.mkdir(parents=True, exist_ok=True)
    results_df.to_csv(output_dir / 'field_name_counts.csv', index=False)
    
    return results_df


def standardize_field_names(input_dir: str | Path = 'output', 
                           output_dir: str | Path = 'output'):
    """
    Standardize field names by removing newlines and parenthetical text.
    
    Args:
        input_dir: Directory containing field_name_counts.csv (default: 'output')
        output_dir: Directory to save standardized results (default: 'output')
        
    Returns:
        pd.DataFrame: Standardized field names with change tracking
        
    Reads from {input_dir}/field_name_counts.csv and saves to {output_dir}/field_name_counts_w_new.csv
    """
    # Read the field name counts from input directory
    input_dir = Path(input_dir)
    output_dir = Path(output_dir)
    df = pd.read_csv(input_dir / 'field_name_counts.csv')
    
    # Apply standardization rules
    df['Standardized_Field_Name'] = df['Field_Name'].apply(standardize_single_name)
    
    # Create aggregated counts for standardized names
    standardized_counts = df.groupby('Standardized_Field_Name')['Count'].sum().reset_index()
    standardized_counts.columns = ['Standardized_Field_Name', 'Standardized_Count']
    
    # Merge back to original data
    result_df = df.merge(standardized_counts, on='Standardized_Field_Name')
    
    # Add changed flag
    result_df['Changed'] = result_df['Field_Name'] != result_df['Standardized_Field_Name']
    
    # Reorder columns
    result_df = result_df[['Field_Name', 'Count', 'Standardized_Field_Name', 'Standardized_Count', 'Changed']]
    
    # Sort by original count descending
    result_df = result_df.sort_values('Count', ascending=False)
    
    # Save results to output directory
    output_dir.mkdir(parents=True, exist_ok=True)
    result_df.to_csv(output_dir / 'field_name_counts_w_new.csv', index=False)
    
    return result_df

def standardize_single_name(name):
    """
    Apply standardization rules to a single field name.
    """
    if not isinstance(name, str):
        return str(name)
    
    # Step 1: Remove newline characters
    standardized = name.replace('\n', ' ').replace('\r', ' ')
    
    # Step 2: Handle "Filler" special case - replace anything with "filler" to just "Filler"
    if 'filler' in standardized.lower():
        standardized = 'Filler'
        return standardized
    
    # Step 3: Handle "Record Type" special case - any field starting with "Record Type" becomes "Record Type"
    if standardized.startswith('Record Type'):
        standardized = 'Record Type'
        return standardized
    
    # Step 4: Handle specific parentheses cases before general removal
    standardized = standardized.replace('WAGM (ARM pools only)', 'WAGM_ARM')
    standardized = standardized.replace('WAGM (AR pools only)', 'WAGM_AR')
    standardized = standardized.replace('Issuer Name (Long Name)', 'Issuer_Name_Long')
    standardized = standardized.replace('Issuer Name (Short Name)', 'Issuer_Name_Short')
    
    # Step 5: Remove text within parentheses (including the parentheses)
    standardized = re.sub(r'\([^)]*\)', '', standardized)
    
    # Step 6: Replace special characters
    standardized = standardized.replace('%', 'Percent')
    standardized = standardized.replace('/', '_or_')
    standardized = standardized.replace('=', '_equals_')
    standardized = standardized.replace('-', '_')
    standardized = standardized.replace('–', '_')  # em dash
    standardized = standardized.replace('+', '_Plus')
    standardized = standardized.replace('*', '')  # Remove asterisks
    standardized = standardized.replace(',', '')  # Remove commas
    standardized = standardized.replace("'", '')  # Remove apostrophes
    standardized = standardized.replace('"', '')  # Remove quotes
    standardized = standardized.replace('"', '')  # Remove smart quotes

    # Step 7: Replace spaces with underscores
    standardized = standardized.replace(' ', '_')
    
    # Step 8: Handle fields that start with a number
    if standardized and standardized[0].isdigit():
        standardized = f"Field_{standardized}"
    
    # Clean up multiple underscores and trim
    standardized = re.sub(r'_+', '_', standardized)  # Remove multiple underscores
    standardized = standardized.strip('_')  # Remove leading/trailing underscores
    
    return standardized


def parse_cobol_format(cobol_notation):
    """
    Parse COBOL format notation into Python data types and processing instructions.
    
    Args:
        cobol_notation (str): COBOL format string like 'X(9)', '9(6)', '9(13)v9(2)'
        
    Returns:
        dict: Contains 'python_type', 'pandas_dtype', 'polars_dtype', 'decimal_places', 'total_length', 'validation_pattern'
        
    Examples:
        >>> parse_cobol_format('X(9)')
        {'python_type': str, 'pandas_dtype': 'string', 'polars_dtype': 'pl.Utf8', 'decimal_places': 0, 'total_length': 9, 'validation_pattern': r'^.{9}$'}
        
        >>> parse_cobol_format('9(6)')
        {'python_type': int, 'pandas_dtype': 'Int64', 'polars_dtype': 'pl.Int64', 'decimal_places': 0, 'total_length': 6, 'validation_pattern': r'^\\d{6}$'}
        
        >>> parse_cobol_format('9(13)v9(2)')
        {'python_type': float, 'pandas_dtype': 'float64', 'polars_dtype': 'pl.Float64', 'decimal_places': 2, 'total_length': 15, 'validation_pattern': r'^\\d{15}$'}
    """
    if not isinstance(cobol_notation, str):
        return None
    
    cobol_notation = cobol_notation.strip()
    
    # Handle alphanumeric fields: X(n)
    x_match = re.match(r'^X\((\d+)\)$', cobol_notation)
    if x_match:
        length = int(x_match.group(1))
        result = {
            'python_type': str,
            'pandas_dtype': 'string',
            'decimal_places': 0,
            'total_length': length,
            'validation_pattern': f'^.{{{length}}}$',
            'cobol_type': 'alphanumeric'
        }
        # Add Polars dtype if available
        if POLARS_AVAILABLE:
            result['polars_dtype'] = pl.Utf8
        else:
            result['polars_dtype'] = 'pl.Utf8'  # String representation for reference
        return result
    
    # Handle numeric fields with implied decimal: 9(m)v9(n)
    decimal_match = re.match(r'^9\((\d+)\)v9\((\d+)\)$', cobol_notation)
    if decimal_match:
        before_decimal = int(decimal_match.group(1))
        after_decimal = int(decimal_match.group(2))
        total_length = before_decimal + after_decimal
        result = {
            'python_type': float,
            'pandas_dtype': 'float64',
            'decimal_places': after_decimal,
            'total_length': total_length,
            'validation_pattern': f'^\\d{{{total_length}}}$',
            'cobol_type': 'numeric_decimal'
        }
        # Add Polars dtype if available
        if POLARS_AVAILABLE:
            result['polars_dtype'] = pl.Float64
        else:
            result['polars_dtype'] = 'pl.Float64'  # String representation for reference
        return result
    
    # Handle integer numeric fields: 9(n)
    int_match = re.match(r'^9\((\d+)\)$', cobol_notation)
    if int_match:
        length = int(int_match.group(1))
        result = {
            'python_type': int,
            'pandas_dtype': 'Int64',  # Nullable integer
            'decimal_places': 0,
            'total_length': length,
            'validation_pattern': f'^\\d{{{length}}}$',
            'cobol_type': 'numeric_integer'
        }
        # Add Polars dtype if available
        if POLARS_AVAILABLE:
            result['polars_dtype'] = pl.Int64
        else:
            result['polars_dtype'] = 'pl.Int64'  # String representation for reference
        return result
    
    # If no pattern matches, return None
    return None


def convert_cobol_value(raw_value, cobol_format_info):
    """
    Convert a raw string value from a fixed-width file using COBOL format information.
    
    Args:
        raw_value (str): Raw string value from file
        cobol_format_info (dict): Output from parse_cobol_format()
        
    Returns:
        Converted value in appropriate Python type, or None if conversion fails
        
    Examples:
        >>> info = parse_cobol_format('9(13)v9(2)')
        >>> convert_cobol_value('000012345000', info)
        123450.0
        
        >>> info = parse_cobol_format('X(9)')
        >>> convert_cobol_value('ABC123   ', info)
        'ABC123   '
        
        >>> info = parse_cobol_format('9(6)')
        >>> convert_cobol_value('202401', info)
        202401
    """
    if not cobol_format_info or not isinstance(raw_value, str):
        return raw_value
    
    try:
        # Handle alphanumeric fields - return as-is (but could trim if needed)
        if cobol_format_info['cobol_type'] == 'alphanumeric':
            return raw_value
        
        # Handle numeric fields
        elif cobol_format_info['cobol_type'] == 'numeric_integer':
            # Convert to integer, handling leading zeros
            return int(raw_value)
        
        # Handle decimal fields with implied decimal point
        elif cobol_format_info['cobol_type'] == 'numeric_decimal':
            decimal_places = cobol_format_info['decimal_places']
            # Convert to integer first, then divide by 10^decimal_places
            int_value = int(raw_value)
            float_value = int_value / (10 ** decimal_places)
            return float_value
        
        else:
            return raw_value
            
    except (ValueError, TypeError):
        return None


def validate_cobol_value(raw_value, cobol_format_info):
    """
    Validate a raw string value against COBOL format requirements.
    
    Args:
        raw_value (str): Raw string value from file
        cobol_format_info (dict): Output from parse_cobol_format()
        
    Returns:
        dict: Contains 'is_valid' (bool) and 'error_message' (str)
        
    Examples:
        >>> info = parse_cobol_format('9(6)')
        >>> validate_cobol_value('202401', info)
        {'is_valid': True, 'error_message': None}
        
        >>> validate_cobol_value('20240A', info)
        {'is_valid': False, 'error_message': 'Value contains non-numeric characters'}
    """
    if not cobol_format_info:
        return {'is_valid': False, 'error_message': 'No format information provided'}
    
    if not isinstance(raw_value, str):
        return {'is_valid': False, 'error_message': 'Value must be a string'}
    
    # Check length
    expected_length = cobol_format_info['total_length']
    if len(raw_value) != expected_length:
        return {
            'is_valid': False, 
            'error_message': f'Expected length {expected_length}, got {len(raw_value)}'
        }
    
    # Check pattern
    pattern = cobol_format_info['validation_pattern']
    if not re.match(pattern, raw_value):
        cobol_type = cobol_format_info['cobol_type']
        if 'numeric' in cobol_type:
            return {'is_valid': False, 'error_message': 'Value contains non-numeric characters'}
        else:
            return {'is_valid': False, 'error_message': 'Value does not match expected pattern'}
    
    return {'is_valid': True, 'error_message': None}


def create_pandas_dtype_mapping(schema_df, remarks_column='Remarks'):
    """
    Create a pandas dtype mapping from a schema DataFrame containing COBOL format notation.
    
    Args:
        schema_df (pd.DataFrame): Schema with COBOL format notation in remarks column
        remarks_column (str): Name of column containing COBOL notation
        
    Returns:
        dict: Mapping of field names to pandas dtypes
        
    Example:
        >>> schema = pd.DataFrame({
        ...     'Data Item': ['Pool Number', 'Issue Date', 'Interest Rate'],
        ...     'Remarks': ['X(6)', '9(8)', '9(2)v9(3)']
        ... })
        >>> create_pandas_dtype_mapping(schema)
        {'Pool Number': 'string', 'Issue Date': 'Int64', 'Interest Rate': 'float64'}
    """
    dtype_mapping = {}
    
    for _, row in schema_df.iterrows():
        field_name = row.get('Data Item') or row.get('Data Element')
        if not field_name or pd.isna(field_name):
            continue
            
        cobol_notation = row.get(remarks_column)
        if not cobol_notation or pd.isna(cobol_notation):
            continue
            
        format_info = parse_cobol_format(cobol_notation)
        if format_info:
            dtype_mapping[field_name] = format_info['pandas_dtype']
    
    return dtype_mapping


def create_polars_dtype_mapping(schema_df, remarks_column='Remarks'):
    """
    Create a Polars dtype mapping from a schema DataFrame containing COBOL format notation.
    
    Args:
        schema_df (pd.DataFrame): Schema with COBOL format notation in remarks column
        remarks_column (str): Name of column containing COBOL notation
        
    Returns:
        dict: Mapping of field names to Polars dtypes
        
    Example:
        >>> schema = pd.DataFrame({
        ...     'Data Item': ['Pool Number', 'Issue Date', 'Interest Rate'],
        ...     'Remarks': ['X(6)', '9(8)', '9(2)v9(3)']
        ... })
        >>> create_polars_dtype_mapping(schema)
        {'Pool Number': pl.Utf8, 'Issue Date': pl.Int64, 'Interest Rate': pl.Float64}
    
    Note:
        Requires Polars to be installed. If Polars is not available, returns string representations.
    """
    if not POLARS_AVAILABLE:
        raise ImportError("Polars is not installed. Install with: pip install polars")
    
    dtype_mapping = {}
    
    for _, row in schema_df.iterrows():
        field_name = row.get('Data Item') or row.get('Data Element')
        if not field_name or pd.isna(field_name):
            continue
            
        cobol_notation = row.get(remarks_column)
        if not cobol_notation or pd.isna(cobol_notation):
            continue
            
        format_info = parse_cobol_format(cobol_notation)
        if format_info:
            dtype_mapping[field_name] = format_info['polars_dtype']
    
    return dtype_mapping


def create_polars_schema(schema_df, remarks_column='Remarks'):
    """
    Create a Polars schema dictionary from a schema DataFrame containing COBOL format notation.
    This is the preferred format for Polars DataFrame creation with explicit schemas.
    
    Args:
        schema_df (pd.DataFrame): Schema with COBOL format notation in remarks column
        remarks_column (str): Name of column containing COBOL notation
        
    Returns:
        dict: Polars schema mapping {column_name: polars_dtype}
        
    Example:
        >>> schema = pd.DataFrame({
        ...     'Data Item': ['Pool Number', 'Issue Date', 'Interest Rate'],
        ...     'Remarks': ['X(6)', '9(8)', '9(2)v9(3)']
        ... })
        >>> polars_schema = create_polars_schema(schema)
        >>> df = pl.DataFrame(data, schema=polars_schema)
    
    Note:
        Requires Polars to be installed.
    """
    return create_polars_dtype_mapping(schema_df, remarks_column)


# Main execution routine
if __name__ == '__main__':

    # Configure Folder
    DATA_DIR = config.DATA_DIR
    DICTIONARY_DIR = config.DICTIONARY_DIR
    PROJECT_DIR = config.PROJECT_DIR
    OUTPUT_DIR = PROJECT_DIR / 'output'

    # Configuration for debugging
    VERBOSE = True  # Set to True for detailed output
    OVERWRITE = False  # Set to False to skip existing files
    
    # Processing steps - set to True for the steps you want to run
    DO_EXTRACTION = True   # Extract PDFs to CSV files
    DO_COMBINATION = True  # Combine CSV files into DataFrames
    DO_ORGANIZATION = False  # Organize data by Record_Type into subfolders
    DO_TEMPORAL_ANALYSIS = False  # Analyze temporal coverage and gaps
    DO_FORMAT_ANALYSIS = False  # Analyze schema formats (delimited vs fixed-width)
    DO_STANDARDIZATION = False  # Standardize field names
    
    # Load YAML file as dictionary
    yaml_file = DICTIONARY_DIR / "prefix_dictionary.yaml"
    with open(yaml_file, "r") as f:
        prefix_dict = yaml.safe_load(f)

    # Loop through Prefixes
    for prefix in prefix_dict.keys():
        if VERBOSE:
            print(f"\n{'='*50}")
            print(f"Processing prefix: {prefix}")
        
        # Step 1: Extract PDFs to CSV files
        if DO_EXTRACTION:
            extract_schemas_to_csv(
                prefix, 
                overwrite=OVERWRITE, 
                verbose=VERBOSE,
            )
        
        # Step 2: Combine CSV files into DataFrame
        if DO_COMBINATION:
            df = combine_schemas(
                prefix, 
                verbose=VERBOSE
            )

            # Save Combined DataFrame to CSV
            df.to_csv(DICTIONARY_DIR / f'combined/{prefix}_combined_schema.csv', index=False)
    
    # Step 3: Organize schemas by Record_Type
    if DO_ORGANIZATION:
        organize_schemas_by_record_type(verbose=VERBOSE)
    
    # Step 4: Analyze temporal coverage
    if DO_TEMPORAL_ANALYSIS:
        print(f"\n{'='*80}")
        print("TEMPORAL COVERAGE ANALYSIS")
        print("="*80)
        
        coverage_df = analyze_temporal_coverage(verbose=VERBOSE)
        
        if not coverage_df.empty:
            print_coverage_summary(coverage_df)
            
            # Save detailed results to CSV
            OUTPUT_DIR.mkdir(parents=True, exist_ok=True)  # Create output directory if it doesn't exist
            output_file = OUTPUT_DIR / 'temporal_coverage_analysis.csv'
            coverage_df.to_csv(output_file, index=False)
            print(f"Detailed results saved to: {output_file}")
        else:
            print("No coverage analysis results generated")
    
    # Step 5: Analyze schema formats
    if DO_FORMAT_ANALYSIS:
        print(f"\n{'='*80}")
        print("SCHEMA FORMAT ANALYSIS")
        print("="*80)
        
        format_df = analyze_schema_formats(verbose=VERBOSE)
        
        if not format_df.empty:
            print_format_analysis_summary(format_df)
            
            # Save detailed results to CSV
            OUTPUT_DIR.mkdir(parents=True, exist_ok=True)  # Create output directory if it doesn't exist
            output_file = OUTPUT_DIR / 'schema_format_analysis.csv'
            format_df.to_csv(output_file, index=False)
            print(f"Detailed results saved to: {output_file}")
        else:
            print("No format analysis results generated")

    # Step 6: Standardize field names
    if DO_STANDARDIZATION:
        result_df = standardize_field_names()
        print(f"Processed {len(result_df)} field names")
        print(f"Changes made to {result_df['Changed'].sum()} field names")
        print(f"Unique standardized names: {len(result_df['Standardized_Field_Name'].unique())}") 