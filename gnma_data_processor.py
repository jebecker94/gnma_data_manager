#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
GNMA Data Processor - Modular approach using combined schemas
Created on: Jan 2025
@author: Assistant

This script processes GNMA data files using combined schema definitions
from the dictionary_files/combined folder.
"""

import os
import glob
import zipfile
import subprocess
import pandas as pd
from pathlib import Path
from typing import Optional, Dict, Any
import config
import io
from io import StringIO
import polars as pl
import yaml


def read_combined_schema(
    file_prefix: str,
    schema_folder: str | Path = config.DICTIONARY_DIR/'combined',
) -> Dict[str, pd.DataFrame]:
    """
    Read the combined schema for a given file prefix.
    
    Parameters
    ----------
    file_prefix : str
        The file prefix (e.g., 'dailyllmni')
    schema_folder : str | Path
        Path to the folder containing combined schema files
        
    Returns
    -------
    Dict[str, pd.DataFrame]
        Dictionary with record types as keys and their schemas as DataFrames
    """
    schema_file = Path(schema_folder) / f"{file_prefix}_combined_schema.csv"
    
    if not schema_file.exists():
        raise FileNotFoundError(f"Schema file not found: {schema_file}")
    
    # Read the combined schema
    schema_df = pd.read_csv(schema_file)
    
    # Group by Record_Type
    schemas_by_record_type = {}
    for record_type in schema_df['Record_Type'].unique():
        record_schema = schema_df[schema_df['Record_Type'] == record_type].copy()
        record_schema = record_schema.sort_values('Item')
        schemas_by_record_type[record_type] = record_schema
    
    return schemas_by_record_type


def extract_records_from_file(
    file_content: list,
    record_type: str | None,
    schema: pd.DataFrame,
) -> pd.DataFrame:
    """
    Extract records of a specific type from file content using the schema.
    
    Parameters
    ----------
    file_content : list
        List of lines from the file
    record_type : str | None
        The record type to extract (e.g., 'L', 'P', 'H')
    schema : pd.DataFrame
        Schema DataFrame for this record type
        
    Returns
    -------
    pd.DataFrame
        Extracted and formatted data
    """

    # Get Data Item Column Name
    if "Data Item" in schema.columns:
        data_item_column = "Data Item"
    elif "Data Element" in schema.columns:
        data_item_column = "Data Element"
    else:
        raise ValueError("Data Item or Data Element column not found in schema")

    # Get Length Column Name
    if "Length" in schema.columns:
        length_column = "Length"
    elif "Max Length" in schema.columns:
        length_column = "Max Length"
    else:
        raise ValueError("Length or Max Length column not found in schema")

    # Filter lines by record type
    filtered_lines = [line for line in file_content if line.startswith(record_type)]

    if not filtered_lines:
        print(f"No records found for record type: {record_type}")
        return pd.DataFrame()

    # Read as Fixed Width File
    filtered_lines = '\n'.join(filtered_lines).replace('\n\n','\n')
    filtered_lines = io.StringIO(filtered_lines)
    df = pd.read_fwf(filtered_lines, widths=schema[length_column], names=schema[data_item_column], dtype='str')
    
    # Convert Numeric Fields
    # Fill in later
    
    return df


def process_zip_files(
    file_prefix: str,
    raw_data_folder: str | Path,
    output_folder: str | Path,
    record_type: str = 'L',
    replace_files: bool = False,
    verbose: bool = False
) -> None:
    """
    Process all zip files for a given file prefix and record type.
    
    Parameters
    ----------
    file_prefix : str
        File prefix to process (e.g., 'dailyllmni')
    raw_data_folder : str | Path
        Folder containing raw zip files
    output_folder : str | Path
        Folder to save processed parquet files
    record_type : str, default 'L'
        Record type to extract
    replace_files : bool, default False
        Whether to replace existing files
    verbose : bool, default False
        Whether to print progress messages
    """
    
    # Read schema for this file prefix
    try:
        schemas = read_combined_schema(file_prefix)
        if record_type not in schemas:
            print(f"Record type '{record_type}' not found in schema for {file_prefix}")
            return
        schema = schemas[record_type]
    except FileNotFoundError as e:
        print(f"Error reading schema: {e}")
        return
    
    # Find zip files with this prefix
    zip_files = Path(raw_data_folder).rglob(f'{file_prefix}_*.zip')
    
    if not zip_files:
        print(f"No zip files found for prefix: {file_prefix}")
        return
    
    # Ensure output folder exists
    os.makedirs(output_folder, exist_ok=True)
    
    # Process each zip file
    for zip_file in zip_files:

        # Extract year-month from filename
        ym = Path(zip_file).stem.split(f'{file_prefix}_')[1]
        output_file = Path(output_folder) / file_prefix / record_type / f"{file_prefix}_{ym}_{record_type}.parquet"
        
        # Skip if file exists and not replacing
        if output_file.exists() and not replace_files:
            if verbose:
                print(f"Skipping existing file: {output_file}")
            continue
        
        if verbose:
            print(f"Processing: {zip_file} -> {output_file}")
        
        try:
            with zipfile.ZipFile(zip_file) as z:

                # Find .txt files in the zip
                txt_files = [name for name in z.namelist() if name.lower().endswith('.txt')]
                
                if not txt_files:
                    print(f"No .txt files found in {zip_file}")
                    continue
                
                # Process each txt file (usually just one)
                all_data = []
                for txt_file in txt_files:
                    if verbose:
                        print(f"  Extracting: {txt_file}")
                    
                    # Extract to temporary location
                    temp_file = Path(raw_data_folder) / txt_file
                    try:
                        z.extract(txt_file, path=raw_data_folder)
                    except Exception as e:
                        if verbose:
                            print(f"  Error with zipfile: {e}")
                            print("  Trying 7z...")
                        # Fallback to 7z
                        subprocess.run([
                            "C:/Program Files/7-Zip/7z.exe", "e", 
                            str(zip_file), f"-o{raw_data_folder}", 
                            txt_file, "-y"
                        ], check=True)
                    
                    # Read file content
                    with open(temp_file, 'r', encoding='iso-8859-1') as f:
                        lines = f.readlines()
                    
                    # File Schema
                    ym = pd.to_datetime(ym, format='%Y%m').strftime('%Y-%m-%d')
                    file_schema = schema[(schema['Min_Date'] <= ym) & (ym <= schema['Max_Date']) & (schema['Record_Type'] == record_type)]

                    # Extract records of specified type
                    df = extract_records_from_file(lines, record_type, file_schema)

                    if not df.empty:
                        all_data.append(df)

                    # Clean up temporary file
                    temp_file.unlink()
                
                # Combine all data and save
                if all_data:
                    combined_df = pd.concat(all_data, ignore_index=True)
                    combined_df.to_parquet(output_file, index=False)
                    if verbose:
                        print(f"  Saved {len(combined_df)} records to {output_file}")
                else:
                    print(f"No data extracted from {zip_file}")
                
        except Exception as e:
            print(f"Error processing {zip_file}: {e}")



# Read Fixed-Width Files
def read_fixed_width_files(
    fwf_name: str | StringIO,
    final_column_names: list[str],
    widths:list[int],
    has_header: bool=False,
    skip_rows: int=0,
) -> pl.LazyFrame :

    # Load the Factor Data from a CSV/TXT file
    df = pl.scan_csv(
        fwf_name,
        has_header=has_header,
        skip_rows=skip_rows,
        new_columns=["full_str"],
        separator='|',
    )

    # Calculate slice values from widths.
    slice_tuples = []
    offset = 0
    for i in widths:
        slice_tuples.append((offset, i))
        offset += i

    # Create Final DataFrame (and drop full string)
    df = df.with_columns(
        [
        pl.col("full_str").str.slice(slice_tuple[0], slice_tuple[1]).str.strip_chars().alias(col)
        for slice_tuple, col in zip(slice_tuples, final_column_names)
        ]
    ).drop("full_str")

    # Return DataFrame
    return df

# Read Fixed-Width Files
def read_parquet_fixed_width(
    parquet_file: str | StringIO,
    final_column_names: list[str],
    widths: list[int],
) -> pl.LazyFrame :

    # Load the Factor Data from a Parquet file
    df = pl.scan_parquet(parquet_file)

    # Calculate slice values from widths.
    slice_tuples = []
    offset = 0
    for i in widths:
        slice_tuples.append((offset, i))
        offset += i

    # Add Number Suffixes to Final Column Names with "Filler"
    final_column_names = [x + f"_{i}" if "Filler" in x else x for i, x in enumerate(final_column_names)]

    # Create Final DataFrame (and drop full string)
    df = df.with_columns(
        [
        pl.col("text_content").str.slice(slice_tuple[0], slice_tuple[1]).str.strip_chars().alias(col)
        for slice_tuple, col in zip(slice_tuples, final_column_names)
        ]
    ).drop("text_content")

    # Return DataFrame
    return df


# Process Raw Parquets
def process_raw_parquet(
    prefix: str,
    record_types: list[str] | None = None,
    overwrite: bool = False,
) :

    # Get Files
    parquet_files = RAW_DIR.rglob(f'{prefix}_*.parquet')

    # Get Schema
    schema_file = DICTIONARY_DIR / 'combined' / f'{prefix}_combined_schema.csv'
    schema = pd.read_csv(schema_file, low_memory=False)

    # Get Record Type
    if record_types is None :
        record_types = schema['Record_Type'].unique().tolist()
        record_types = [x for x in record_types if isinstance(x, str)]

    # Check for length column (not max length, which is only for delimited files)
    if 'length' not in schema.columns.str.lower().tolist() :
        print(f"Length column not found in schema for file type: {prefix}")
        return

    # Loop through files
    for parquet_file in parquet_files :

        for record_type in record_types :

            # Get Save File Name
            ym = Path(parquet_file).stem.split(f'{prefix}_')[1]
            save_file_name = CLEAN_DIR / prefix / record_type / f'{prefix}_{ym}_{record_type}.parquet'

            # If file doesn't exist or overwrite is True, process the file
            if not save_file_name.exists() or overwrite :

                # Limit Files to Date Range
                ym_dt = pd.to_datetime(ym, format='%Y%m').strftime('%Y-%m-%d')
                file_schema = schema[(schema['Min_Date'] <= ym_dt) & (ym_dt <= schema['Max_Date'])]

                # Limit Schema to Record Type
                file_schema = file_schema[file_schema['Record_Type'] == record_type]

                # If there's a schema, read the file
                if not file_schema.empty :

                    # Get Data Item and Length
                    data_item_column = [x for x in file_schema.columns if 'Data Item' in x or 'Data Element' in x][0]
                    length_column = [x for x in file_schema.columns if x=='Length' or x=='Max Length'][0]

                    # Read the file contents w/ fixed-width logic
                    df = read_parquet_fixed_width(parquet_file, file_schema[data_item_column].tolist(), file_schema[length_column].tolist())

                    # Get record type column
                    record_type_column = [x for x in df.columns if 'Record Type' in x][0]

                    # Filter by record type
                    df = df.filter(pl.col(record_type_column) == record_type)

                    # Save to Clean Directory
                    df.sink_parquet(save_file_name)


# Process Delimited Parquet
def process_delimited_parquet(
    prefix: str,
    record_types: list[str] = None,
    delimiter: str = '|',
    overwrite: bool = False,
) :

    # Get Parquet Files
    parquet_files = RAW_DIR.rglob(f'{prefix}_*.parquet')

    # Get Schema
    schema_file = DICTIONARY_DIR / 'combined' / f'{prefix}_combined_schema.csv'
    schema = pd.read_csv(schema_file, low_memory=False)

    # Get Record Type
    if record_types is None :
        record_types = schema['Record_Type'].unique().tolist()
        record_types = [x for x in record_types if isinstance(x, str)]

    # Check for length column (not max length, which is only for delimited files)
    if 'max length' not in schema.columns.str.lower().tolist() :
        print(f"Max Length column not found in schema for file type: {prefix}")
        return

    # Loop through files
    for parquet_file in parquet_files :

        # Open Parquet File
        print(parquet_file)
        ym = Path(parquet_file).stem.split(f'{prefix}_')[1]
        df = pl.read_parquet(parquet_file)

        for record_type in record_types :

            # Get Save File Name
            save_file_name = CLEAN_DIR / prefix / record_type / f'{prefix}_{ym}_{record_type}.parquet'

            # If file doesn't exist or overwrite is True, process the file
            if not save_file_name.exists() or overwrite :

                # Limit Files to Date Range
                ym_dt = pd.to_datetime(ym, format='%Y%m').strftime('%Y-%m-%d')
                file_schema = schema[(schema['Min_Date'] <= ym_dt) & (ym_dt <= schema['Max_Date'])]

                # Limit Schema to Record Type
                file_schema = file_schema[file_schema['Record_Type'] == record_type]

                # Get Column Names
                final_column_names = file_schema['Data Element'].tolist()

                if final_column_names :

                    # Get Number of Columns
                    number_columns = len(final_column_names)

                    # Split Text Content into Columns
                    df_record = df.with_columns(
                            pl.col("text_content")
                            .str.split_exact(delimiter, number_columns-1)   # split into exactly number_columns pieces
                            .alias("parts")
                        ).unnest("parts").drop("text_content")

                    # Rename Columns using final_column_names
                    df_record = df_record.rename({
                        col: final_column_names[i]
                        for i, col in enumerate(df_record.columns)
                    })

                    # Get Record Type Column and Filter by Record Type
                    record_type_column = [x for x in df_record.columns if 'Record Type' in x][0]
                    df_record = df_record.filter(pl.col(record_type_column) == record_type)

                    # Save to Clean Directory
                    df_record.write_parquet(save_file_name)

## Main Routine
if __name__ == "__main__":

    # Set up directories from config
    RAW_DIR = config.RAW_DIR
    CLEAN_DIR = config.CLEAN_DIR
    DICTIONARY_DIR = config.DICTIONARY_DIR

    # Ensure directories exist
    for directory in [RAW_DIR, CLEAN_DIR]:
        os.makedirs(directory, exist_ok=True)

    # Load YAML file as dictionary
    yaml_file = DICTIONARY_DIR / "prefix_dictionary.yaml"
    with open(yaml_file, "r") as f:
        prefix_dict = yaml.safe_load(f)
        prefixes = prefix_dict.keys()

    # Process Fixed Width Parquets
    prefixes = [
        # 'dailyllmni',
        # 'hdailyllmni',
        # 'monthly',
        # 'hllmon1',
        # 'hllmon2',
        # 'monthly',
    ]
    # for prefix in prefixes:
    #     process_raw_parquet(prefix)

    # Process Delimited Parquets
    prefixes = [
        'monthlySFPS',
        'monthlySFS',
        'nimonSFPS',
        'nimonSFS',
    ]
    for prefix in prefixes:
        process_delimited_parquet(prefix)


    ## Deprecated Code (Keep for reference)
    # for record_type in ["H","L","P","T","Z"]:
    #     process_zip_files(
    #         file_prefix="llmon1",
    #         raw_data_folder=RAW_DIR,
    #         output_folder=CLEAN_DIR,
    #         record_type=record_type,
    #         replace_files=False,
    #         verbose=True
    #     )
