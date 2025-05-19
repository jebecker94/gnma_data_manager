#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on: Sat Dec 3 09:49:44 2022
Last updated on: Friday May 2 23:21:00 2025
@author: Jonathan E. Becker
"""

#%% Setup
# Import Packages
import os
import glob
import zipfile
import subprocess
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from io import StringIO
import config

#%% UNZIPPING
# Skip Row (For Handling Reading Errors)
def skip_row(row) :
    """
    Function for handling bad rows when reading CSVs with pyarrow.

    Parameters
    ----------
    row : TYPE
        DESCRIPTION.

    Returns
    -------
    str
        DESCRIPTION.

    """

    return 'skip'

# Convert MBS/HMBS Files to Compressed CSV
def unzip_gnma_data(data_folder, save_folder, formatting_file, file_prefix='dailyllmni', replace_files=False, record_type='L', verbose=False) :
    """
    Unzip files from GNMA disclosure data collection, and create parquet files.

    Parameters
    ----------
    data_folder : string
        Folder containing raw data in zip archives.
    save_folder : string
        Folder to save parquet files.
    formatting_file : string
        File path of GNMA formatting files.
    file_prefix : string, optional
        Prefix of files to clean. The default is 'dailyllmni'.
    replace_files : boolean, optional
        Whether to replace clean files if already exist. The default is False.

    Returns
    -------
    None.

    """

    # Get Formatting
    formatting = pd.read_csv(formatting_file)
    formatting = formatting[formatting['File Prefix'] == file_prefix]

    # Get Zip Folders
    folders = glob.glob(f'{data_folder}/{file_prefix}_*.zip')
    for folder in folders :

        # Get Year-Month Suffix
        ym = int(folder.split(f'{file_prefix}_')[1].split('.zip')[0])
        save_file_name = f'{save_folder}/{file_prefix}_{ym}{record_type}.parquet'

        if not os.path.exists(save_file_name) or replace_files :

            with zipfile.ZipFile(folder) as z :

                # Only Worry about .txt Files
                txt_files = [x for x in z.namelist() if ".txt" in x.lower()]
                for file in txt_files :

                    # Get Specific Format
                    formats = formatting[(formatting['First Month'] <= ym) & (formatting['Last Month'] >= ym) & (formatting['Record Type'] == record_type)]

                    # Extract and Create Temporary File
                    if verbose :
                        print('Extracting File:', file, 'Year/Month:', ym)
                    try :
                        z.extract(file, path=data_folder)
                    except :
                        if verbose :
                            print('Could not unzip file:', file, 'with Pythons Zipfile package. Using 7z instead.')
                        unzip_string = "C:/Program Files/7-Zip/7z.exe"
                        p = subprocess.Popen([unzip_string, "e", f"{folder}", f"-o{data_folder}", f"{file}", "-y"])
                        p.wait()

                    # Open File
                    newfilename = f'{data_folder}/{file}'
                    with open(newfilename, encoding='iso-8859-1') as f :
                        lines = f.readlines()
                    lines = [x for x in lines if x[0]==record_type]

                    # Read Data
                    df = pd.DataFrame([])
                    for _, field in formats.iterrows() :

                        # Read Format File
                        field_name = field['Data Item'].strip()
                        begin_char = field['Begin']-1
                        end_char = field['End']

                        # Display Progress and Read Lines
                        if verbose :
                            print("Reading in Field:", field_name)
                        df[field_name] = [x[begin_char:end_char].strip() for x in lines]

                        # Convert if Numeric
                        if field['Type'] == 'Numeric' :
                            df[field_name] = pd.to_numeric(df[field_name], errors='coerce')

                    # Remove Temporary File
                    os.remove(newfilename)

                    # Save
                    df.to_parquet(save_file_name, index=False)

# Convert MBS/HMBS Files to Compressed CSV
def unzip_gnma_nissues_data(data_folder, save_folder, formatting_file, file_prefix = 'dailyllmni', replace_files = False, record_type = 'L', verbose = False) :
    """
    Unzip files from GNMA disclosure data collection, and create gzipped csvs.

    Parameters
    ----------
    data_folder : string
        Folder containing raw data in zip archives.
    save_folder : string
        Folder to save gzipped csvs.
    formatting_file : string
        File path of GNMA formatting files.
    file_prefix : string, optional
        Prefix of files to clean. The default is 'dailyllmni'.
    replace_files : boolean, optional
        Whether to replace clean files if already exist. The default is False.

    Returns
    -------
    None.

    """

    # Get Formatting
    formatting = pd.read_csv(formatting_file)
    formatting = formatting[formatting['File Prefix'] == file_prefix]

    # Get Zip Folders
    folders = glob.glob(f'{data_folder}/{file_prefix}_*.zip')
    for folder in folders :

        # Get Year-Month Suffix
        ym = int(folder.split(f'{file_prefix}_')[1].split('.zip')[0])
        save_file_name = f'{save_folder}/{file_prefix}_{ym}{record_type}.parquet'

        if not os.path.exists(save_file_name) or replace_files :

            with zipfile.ZipFile(folder) as z :

                # Only Worry about .txt Files
                txt_files = [x for x in z.namelist() if ".txt" in x.lower()]

                for file in txt_files :

                    # Get Specific Format
                    formats = formatting[(formatting['First Month'] <= ym) & (formatting['Last Month'] >= ym) & (formatting['Record Type'] == record_type)]

                    # Extract and Create Temporary File
                    if verbose :
                        print('Extracting File:', file, 'Year/Month:', ym)
                    try :
                        z.extract(file, path = data_folder)
                    except :
                        if verbose :
                            print('Could not unzip file:', file, 'with Pythons Zipfile package. Using 7z instead.')
                        unzip_string = "C:/Program Files/7-Zip/7z.exe"
                        p = subprocess.Popen([unzip_string, "e", f"{folder}", f"-o{data_folder}", f"{file}", "-y"])
                        p.wait()

                    # Open File
                    newfilename = f'{data_folder}/{file}'
                    with open(newfilename, encoding = 'iso-8859-1') as f :
                        lines = f.readlines()
                    lines = [x for x in lines if x[18:19]==record_type]

                    # Read Data
                    df = pd.DataFrame([])
                    for _, field in formats.iterrows() :

                        # Read Format File
                        field_name = field['Data Item'].strip()
                        begin_char = field['Begin'] - 1
                        end_char = field['End']

                        # Display Progress and Read Lines
                        if verbose :
                            print("Reading in Field:", field_name)
                        df[field_name] = [x[begin_char:end_char].strip() for x in lines]

                        # Convert if Numeric
                        if field['Type'] == 'Numeric' :
                            df[field_name] = pd.to_numeric(df[field_name], errors = 'coerce')

                    # Remove Temporary File
                    os.remove(newfilename)

                    # Save
                    dt = pa.Table.from_pandas(df)
                    pq.write_table(dt, save_file_name)

# Convert MBS/HMBS Files to Compressed CSV
def unzip_gnma_nimon_data(data_folder, save_folder, formatting_file, file_prefix='nimonSFPS', replace_files=False, record_type='PS', verbose=False) :
    """
    Unzip files from GNMA disclosure data collection, and create gzipped csvs.

    Parameters
    ----------
    data_folder : string
        Folder containing raw data in zip archives.
    save_folder : string
        Folder to save gzipped csvs.
    formatting_file : string
        File path of GNMA formatting files.
    file_prefix : string, optional
        Prefix of files to clean. The default is 'nimonSFPS'.
    replace_files : boolean, optional
        Whether to replace clean files if already exist. The default is False.

    Returns
    -------
    None.

    """

    # Get Formatting
    cols = pd.read_csv(formatting_file)
    # cols = cols[cols['File Prefix'] == file_prefix]
    cols = cols.drop_duplicates(subset = ['Record Type', 'Item'])
    cols = cols.loc[cols['Record Type'] == record_type]
    
    #
    folders = glob.glob(f'{data_folder}/{file_prefix}_*.zip')
    for folder in folders :

        # Get Year-Month Suffix
        ym = int(folder.split(f'{file_prefix}_')[1].split('.zip')[0])
        save_file_name = f'{save_folder}/{file_prefix}_{ym}{record_type}.parquet'

        if not os.path.exists(save_file_name) or replace_files :

            with zipfile.ZipFile(folder) as z :

                # Only Worry about .txt Files
                txt_files = [x for x in z.namelist() if ".txt" in x.lower()]
                for file in txt_files :
                    
                    # Get Specific Format
                    # formats = formatting[(formatting['First Month'] <= ym) & (formatting['Last Month'] >= ym) & (formatting['Record Type'] == record_type)]

                    # Extract and Create Temporary File
                    if verbose :
                        print('Extracting File:', file, 'Year/Month:', ym)
                    try :
                        z.extract(file, path = data_folder)
                    except :
                        if verbose :
                            print('Could not unzip file:', file, 'with Pythons Zipfile package. Using 7z instead.')
                        unzip_string = "C:/Program Files/7-Zip/7z.exe"
                        p = subprocess.Popen([unzip_string, "e", f"{folder}", f"-o{data_folder}", f"{file}", "-y"])
                        p.wait()

                    # Open File
                    newfilename = f'{data_folder}/{file}'
                    with open(newfilename, encoding = 'iso-8859-1') as f :
                        lines = f.readlines()
                    lines = [x for x in lines if x.startswith(record_type)]
                    content = ''.join(lines)
                    df = pd.read_csv(StringIO(content), sep='|', header=None)
                    
                    # Rename Columns
                    df.columns = list(cols['Data Element'])[:len(df.columns)] # May want to adjust this... just making a concession to the formatting file
                    
                    # Save
                    df.to_parquet(save_file_name, index=False)

                    # Remove Temporary File
                    os.remove(newfilename)

#%% SUMMARY
# Combine Ginnie Mae Data
def combine_gnma_data(data_folder, save_folder, file_prefix = 'dailyllmni', record_type = 'L', file_suffix = '') :
    """
    Combine all GNMA files with a given prefix and record type.

    Parameters
    ----------
    data_folder : TYPE
        DESCRIPTION.
    save_folder : TYPE
        DESCRIPTION.
    file_prefix : TYPE, optional
        DESCRIPTION. The default is 'dailyllmni'.
    file_suffix : TYPE, optional
        DESCRIPTION. The default is ''.

    Returns
    -------
    None.

    """

    # Load
    df = []
    files = glob.glob(f'{data_folder}/{file_prefix}_*{record_type}*.parquet')
    files.sort()
    for file in files :
        df_a = pq.read_table(file).to_pandas(date_as_object = False)
        df.append(df_a)

    # Combine and Save
    # df = pa.concat_tables(df, promote = True)
    df = pd.concat(df)
    df = pa.Table.from_pandas(df, preserve_index = False)
    pq.write_table(df, f'{save_folder}/{file_prefix}_combined_{file_suffix}{record_type}.parquet')

#%% Main Routine
if __name__ == '__main__' :

    # Set Folder Structure
    DATA_DIR = config.DATA_DIR
    RAW_DIR = config.RAW_DIR
    CLEAN_DIR = config.CLEAN_DIR
    PROJECT_DIR = config.PROJECT_DIR

    # Make Data Directories if missing
    if not os.path.exists(DATA_DIR) :
        os.makedirs(DATA_DIR)
    if not os.path.exists(RAW_DIR) :
        os.makedirs(RAW_DIR)
    if not os.path.exists(CLEAN_DIR) :
        os.makedirs(CLEAN_DIR)

    # Set Formatting Files
    FORMATTING_FILE = PROJECT_DIR / 'dictionary_files/gnma_file_layouts.csv'

    # Import Ginnie Mae Data
    for FILE_PREFIX in ['hdailyllmni'] :
        unzip_gnma_data(RAW_DIR, CLEAN_DIR, FORMATTING_FILE, file_prefix=FILE_PREFIX, record_type='L')
    for FILE_PREFIX in ['hdailyllmni'] :
        unzip_gnma_data(RAW_DIR, CLEAN_DIR, FORMATTING_FILE, file_prefix=FILE_PREFIX, record_type='P')

    ## HMBS
    combine_gnma_data(CLEAN_DIR, DATA_DIR, file_prefix='hdailyllmni', record_type='L', file_suffix='201407-202502')
