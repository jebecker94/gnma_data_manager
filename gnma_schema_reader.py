"""Warning: Work in progress."""

# Import Packages
import pdfplumber
from collections import defaultdict
import pandas as pd
from typing import List, Dict, Any, Tuple
from pathlib import Path
import datetime
import pymupdf
import yaml
import re


# Extract Dates Between Parentheses
def extract_dates_between_parentheses(
    file_path: str | Path,
) -> List[str]:
    """
    Extract dates between parentheses from a file path.
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
        print(f"\n⚠️  Groups with multiple record types:")
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
                print(f"    ✓ Only one group found, dropped group_id column")
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
                    print(f"    ✓ Validated 1:1 record_type↔group_id mapping, dropped group_id column")
            else:
                # Keep group_id for debugging/analysis
                if verbose:
                    print(f"    ⚠ Complex record_type↔group_id mapping detected:")
                    print(f"      Max groups per record type: {max_groups_per_record_type}")
                    print(f"      Max record types per group: {max_record_types_per_group}")
                    print(f"      Keeping group_id column for analysis")
        else:
            # Multiple groups but no record types - keep group_id for structure
            if verbose:
                print(f"    ⚠ Multiple groups ({unique_groups}) but no record types - keeping group_id column")
    
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
        save_file = Path(f'./dictionary_files/clean/{prefix}') / pdf_path.with_suffix('.csv').name
        if save_file.exists() and not overwrite:
            if verbose:
                print(f"  Skipping - CSV already exists: {save_file.name}")
            return []  # Return empty list since we're not processing

    # PyMuPDF
    schema: List[pd.DataFrame] = []
    total_standardizations = 0
    with pymupdf.open(pdf_path) as pdf:
        for page in pdf.pages():
            tables = page.find_tables(vertical_strategy='lines_strict', horizontal_strategy='lines_strict')
            for table in tables:
                df = table.to_pandas()
                if "item" in df.columns.str.lower().values:
                    # Count columns before standardization
                    original_columns = set(df.columns)
                    # Standardize column names immediately after extraction
                    df = standardize_column_names(df, verbose=False)
                    # Count standardizations
                    standardized_columns = set(df.columns)
                    if original_columns != standardized_columns:
                        total_standardizations += len(original_columns - standardized_columns)
                    schema.append(df)

    if verbose:
        print(f"  Found {len(schema)} tables with 'item' column")
        if total_standardizations > 0:
            print(f"  Standardized {total_standardizations} column names during table extraction")
        elif schema:
            print(f"  No column standardization needed - names already clean")

    # Save Document Data to Separate CSV File
    if save_document_data and schema:
        df = pd.concat(schema)
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
            save_file = Path(f'./dictionary_files/clean/{prefix}') / pdf_path.with_suffix('.csv').name
            
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
    pdf_directory = Path(f'./dictionary_files/raw/{prefix}')
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
    csv_directory = Path(f'./dictionary_files/clean/{prefix}')
    
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
    yaml_file = Path("dictionary_files/prefix_dictionary.yaml")
    with open(yaml_file, "r") as f:
        prefix_dict = yaml.safe_load(f)
    
    # Process each prefix
    for prefix in prefix_dict.keys():
        if verbose:
            print(f"\nProcessing prefix: {prefix}")
        
        # Check if combined schema file exists
        combined_file = Path(f'dictionary_files/combined/{prefix}_combined_schema.csv')
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
            base_clean_dir = Path(f'data/clean/{prefix}')
            
            for record_type in record_types:
                record_type_dir = base_clean_dir / str(record_type)
                record_type_dir.mkdir(parents=True, exist_ok=True)
                
                if verbose:
                    print(f"    Created folder: {record_type_dir.relative_to(Path('.'))}")
            
            if verbose:
                print(f"  ✓ Organized {prefix} by {len(record_types)} record types")
                
        except Exception as e:
            if verbose:
                print(f"  Error processing {prefix}: {e}")
    
    if verbose:
        print(f"\n✓ Completed schema organization by Record_Type")


#%% Main Routine
if __name__ == '__main__':

    # Configuration for debugging
    VERBOSE = True  # Set to True for detailed output
    OVERWRITE = False  # Set to False to skip existing files
    
    # Processing steps - set to True for the steps you want to run
    DO_EXTRACTION = True   # Extract PDFs to CSV files
    DO_COMBINATION = True  # Combine CSV files into DataFrames
    DO_ORGANIZATION = True  # Organize schemas by Record_Type into subfolders
    
    # Load YAML file as dictionary
    yaml_file = Path("dictionary_files/prefix_dictionary.yaml")
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
            df.to_csv(f'./dictionary_files/combined/{prefix}_combined_schema.csv', index=False)
    
    # Step 3: Organize schemas by Record_Type
    if DO_ORGANIZATION:
        organize_schemas_by_record_type(verbose=VERBOSE)
