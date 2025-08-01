#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Zip to Parquet Processor - Stream text files from zips to parquet format
Created on: July 23, 2025

This script processes zip files containing text files and saves each line
as a single observation in a parquet file using polars lazy loading.
"""

import os
import zipfile
import tempfile
from pathlib import Path
from typing import Optional, Union
import polars as pl
import config


def process_zip_to_parquet(
    zip_file_path: Union[str, Path],
    output_file_path: Union[str, Path],
    text_column_name: str = "text_content",
    encoding: str = "utf-8",
    verbose: bool = False
) -> None:
    """
    Process a single zip file containing a text file and save to parquet.
    
    Parameters
    ----------
    zip_file_path : Union[str, Path]
        Path to the zip file
    output_file_path : Union[str, Path]
        Path for the output parquet file
    text_column_name : str, default "text_content"
        Name of the column to store text lines
    encoding : str, default "utf-8"
        Text file encoding (try "iso-8859-1" if utf-8 fails)
    verbose : bool, default False
        Whether to print progress messages
    """
    
    zip_path = Path(zip_file_path)
    output_path = Path(output_file_path)
    
    if verbose:
        print(f"Processing: {zip_path} -> {output_path}")
    
    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            # Find text files in the zip
            text_files = [name for name in zip_ref.namelist() 
                         if name.lower().endswith(('.txt', '.dat', '.csv'))]
            
            if not text_files:
                print(f"No text files found in {zip_path}")
                return
            
            if len(text_files) > 1 and verbose:
                print(f"Multiple text files found, processing all: {text_files}")
            
            all_lines = []
            
            # Process each text file in the zip
            for text_file in text_files:
                if verbose:
                    print(f"  Extracting: {text_file}")
                
                try:
                    # Read file content directly from zip
                    with zip_ref.open(text_file) as file:
                        content = file.read().decode(encoding)
                        lines = content.splitlines()
                        all_lines.extend(lines)
                        
                except UnicodeDecodeError:
                    # Fallback to different encoding
                    if verbose:
                        print(f"  Encoding {encoding} failed, trying iso-8859-1...")
                    with zip_ref.open(text_file) as file:
                        content = file.read().decode('iso-8859-1')
                        lines = content.splitlines()
                        all_lines.extend(lines)
            
            # Create polars DataFrame and save as parquet
            if all_lines:
                # Use polars lazy loading for efficiency
                df = pl.LazyFrame({text_column_name: all_lines})
                
                # Write to parquet
                df.sink_parquet(output_path)
                
                if verbose:
                    print(f"  Saved {len(all_lines)} lines to {output_path}")
            else:
                print(f"No content found in {zip_path}")
                
    except Exception as e:
        print(f"Error processing {zip_path}: {e}")


def process_zip_folder(
    zip_folder: Union[str, Path],
    output_folder: Union[str, Path],
    text_column_name: str = "text_content",
    encoding: str = "utf-8",
    file_pattern: str = "*.zip",
    replace_existing: bool = False,
    verbose: bool = False
) -> None:
    """
    Process all zip files in a folder and save as parquet files.
    
    Parameters
    ----------
    zip_folder : Union[str, Path]
        Folder containing zip files
    output_folder : Union[str, Path]
        Folder to save parquet files
    text_column_name : str, default "text_content"
        Name of the column to store text lines
    encoding : str, default "utf-8"
        Text file encoding
    file_pattern : str, default "*.zip"
        Pattern to match zip files
    replace_existing : bool, default False
        Whether to replace existing parquet files
    verbose : bool, default False
        Whether to print progress messages
    """
    
    zip_folder = Path(zip_folder)
    output_folder = Path(output_folder)
    
    if not zip_folder.exists():
        raise FileNotFoundError(f"Zip folder not found: {zip_folder}")
    
    # Find all zip files
    zip_files = list(zip_folder.rglob(file_pattern))
    
    if not zip_files:
        print(f"No zip files found in {zip_folder} with pattern {file_pattern}")
        return
    
    if verbose:
        print(f"Found {len(zip_files)} zip files to process")
    
    # Process each zip file
    for zip_file in zip_files:
        # Generate output filename (replace .zip with .parquet)
        relative_path = zip_file.relative_to(zip_folder)
        output_file = output_folder / relative_path.with_suffix('.parquet')
        
        # Skip if file exists and not replacing
        if output_file.exists() and not replace_existing:
            if verbose:
                print(f"Skipping existing file: {output_file}")
            continue

        process_zip_to_parquet(
            zip_file_path=zip_file,
            output_file_path=output_file,
            text_column_name=text_column_name,
            encoding=encoding,
            verbose=verbose
        )


## Main Routine
if __name__ == "__main__":
    
    # Set your input and output folders
    ZIP_FOLDER = config.RAW_DIR  # Update this path
    OUTPUT_FOLDER = ZIP_FOLDER
    
    # Process all zip files in the folder
    process_zip_folder(
        zip_folder=ZIP_FOLDER,
        output_folder=OUTPUT_FOLDER,
        text_column_name="text_content",
        encoding="utf-8",  # Try "iso-8859-1" if you get encoding errors
        file_pattern = '*.zip',
        replace_existing=False,
        verbose=True
    )
