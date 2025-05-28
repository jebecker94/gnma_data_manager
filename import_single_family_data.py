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
import re
import polars as pl
from pathlib import Path

#%% UNZIPPING
# Get Combined Suffix
def get_combined_suffix(files: list[str]) -> str:
    """Create a suffix for a combined file from a list of individual files."""

    # Get Suffixes from File Names and Create Combined Suffix from Min and Max Dates
    suffixes = [os.path.splitext(os.path.basename(file))[0].split('_')[-1] for file in files]
    suffixes = ["".join(re.findall(r'\d', suffix)) for suffix in suffixes] # Extract only numeric characters
    combined_suffix = '_'+min(suffixes)+'-'+max(suffixes)

    # Return Combined Suffix
    return combined_suffix

# Convert MBS/HMBS Files to Compressed CSV
def unzip_gnma_data(
    data_folder: str | Path,
    save_folder: str | Path,
    formatting_file: str | Path,
    file_prefix: str = 'dailyllmni',
    replace_files: bool = False,
    record_type: str = 'L',
    verbose: bool = False
) -> None:
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
    folders.sort(reverse=False)
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
                    except Exception as e:
                        if verbose :
                            print('Error:', e)
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
def unzip_gnma_nissues_data(
    data_folder: str | Path,
    save_folder: str | Path,
    formatting_file: str | Path,
    file_prefix: str = 'dailyllmni',
    replace_files: bool = False,
    record_type: str = 'L',
    verbose: bool = False
) -> None:
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
                    except Exception as e:
                        if verbose :
                            print('Error:', e)
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
def unzip_gnma_nimon_data(
    data_folder: str | Path,
    save_folder: str | Path,
    formatting_file: str | Path,
    file_prefix: str = 'nimonSFPS',
    replace_files: bool = False,
    record_type: str = 'PS',
    verbose: bool = False
) -> None:
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
                    except Exception as e:
                        if verbose :
                            print('Error:', e)
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
def combine_gnma_data(
    data_folder: str | Path,
    save_folder: str | Path,
    file_prefix: str = 'dailyllmni',
    record_type: str = 'L',
    file_suffix: str | None = None
) -> None:
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

    # Verify that data and save folders are different
    if data_folder == save_folder :
        raise ValueError('Data and save folders cannot be the same.')

    # Get Files
    files = glob.glob(f'{data_folder}/{file_prefix}_*{record_type}*.parquet')
    files.sort()

    # Get File Suffix and Create Save File Name
    if not file_suffix :
        file_suffix = get_combined_suffix(files)
    save_file_name = f'{save_folder}/{file_prefix}_combined{file_suffix}{record_type}.parquet'

    # Combine and Save
    if not os.path.exists(save_file_name) :
        df = []
        for file in files :
            df_a = pl.scan_parquet(file)
            df.append(df_a)
        df = pl.concat(df, how='diagonal_relaxed')
        df.sink_parquet(save_file_name)

# Combine Ginnie Mae Data
def combine_gnma_pools(
    data_folder: str | Path,
    save_folder: str | Path,
    file_suffix: str | None = None
) -> None:
    """
    Combine GNMA pools data from issuance files.

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

    # Get Files
    files = glob.glob(f'{data_folder}/*dailyllmni_*P*.parquet')
    files.sort()

    # Get File Suffix and Create Save File Name
    if not file_suffix :
        file_suffix = get_combined_suffix(files)
    save_file_name = f'{save_folder}/gnma_combined_pools{file_suffix}.parquet'

    # Combine Monthly Files
    df = []
    for file in files :
        df_a = pl.scan_parquet(file)
        if 'As-Of Date' in df_a.columns :
            df_a = df_a.rename({'As-Of Date': 'As of Date'})
        df.append(df_a)
    df = pl.concat(df, how='diagonal_relaxed')

    # Save
    df.sink_parquet(save_file_name)

# Get GNMA Liquidation Reasons
def get_liquidation_reasons(
    data_folder: str | Path,
    save_folder: str | Path,
    file_suffix: str | None = None,
    verbose: bool = False
) -> None:
    """
    Read liquidation reasons from performance files.

    Parameters
    ----------
    data_folder : TYPE
        DESCRIPTION.
    save_folder : TYPE
        DESCRIPTION.
    file_suffix : TYPE, optional
        DESCRIPTION. The default is ''.
    verbose : TYPE, optional
        DESCRIPTION. The default is False.

    Returns
    -------
    None.

    """

    # Read and Write Options
    liquidation_columns = [
        'Disclosure Sequence Number',
        'As of Date',
        'Current Month Liquidation Flag',
        'Removal Reason',
        'Months Pre-Paid',
        'Months Delinquent',
        'Pool ID',
        'Issuer ID',
        'First Payment Date',
        'Unpaid Principal Balance',
    ]

    # Get Performance Files
    files = glob.glob(f'{data_folder}/llmon1_*.parquet')
    files += glob.glob(f'{data_folder}/llmon2_*.parquet')
    files.sort(reverse=True)

    # Get File Suffix and Create Save File Name
    if not file_suffix :
        file_suffix = get_combined_suffix(files)

    # Read Monthly Liquidations
    df = []
    for file in files :

        if verbose :
            print('Importing liquidation/termination reasons from file:', file)

        try :
            df_a = pl.scan_parquet(file)
            if 'Current Month Liquidation Flag' in df_a.columns :
                df_a = df_a.select(liquidation_columns)#, strict=False)
                df_a = df_a.filter([pl.col('Current Month Liquidation Flag')=='Y'])
                df_a = df_a.drop('Current Month Liquidation Flag')
                # df_a['Removal Reason'] = [int(x) for x in df_a['Removal Reason']]
                df.append(df_a)
        except Exception as e:
            print('Error:', e, '\nSkipping for now, but you may wish to investigate.')
            pass

    # Combine Monthly DataFrames
    df = pl.concat(df, how='diagonal_relaxed')

    # Rename Columns
    rename_map = {
        'As of Date': 'Liquidation Date',
        'Months Pre-Paid': 'Months Pre-Paid at Liquidation',
        'Months Delinquent': 'Months Delinquent at Liquidation',
        'Pool ID': 'Final Pool ID',
        'Issuer ID': 'Final Issuer ID',
        'Unpaid Principal Balance': 'Final Unpaid Principal Balance',
    }
    df = df.rename(rename_map)

    # Save
    save_file = f'{save_folder}/gnma_combined_loan_liquidations{file_suffix}.parquet'
    df.sink_parquet(save_file)

# Add Old Observations to Issuance Data
def create_final_dataset(
    data_folder: str | Path,
    save_folder: str | Path,
    file_suffix: str | None = None,
) -> None:
    """
    Create final dataset from issuance, pools, and liquidations data.

    Parameters
    ----------
    data_folder : str | Path
        DESCRIPTION.
    save_folder : str | Path
        DESCRIPTION.
    file_suffix : str, optional
        DESCRIPTION. The default is ''.

    Returns
    -------
    None.

    """

    # Load Combined Issuances Data
    combined_issuance_files = glob.glob(f'{save_folder}/dailyllmni_combined_*L.parquet')
    combined_issuance_files.sort(reverse=True)
    combined_issuance_file = combined_issuance_files[0]
    df = pl.scan_parquet(combined_issuance_file)

    # Read Initial Observations
    first_period_files = [f'{data_folder}/llmon1_201310L.parquet', f'{data_folder}/llmon2_201310L.parquet']
    df_old = []
    for file in first_period_files :
        df_a = pl.scan_parquet(file)
        df_old.append(df_a)
    df_old = pl.concat(df_old, how='diagonal_relaxed')
    df_old = df_old.with_columns(pl.lit(201310).alias('As of Date'))

    # Append and Drop Duplicates
    df = pl.concat([df, df_old], how='diagonal_relaxed')
    del df_old
    df = df.unique(subset = ['Disclosure Sequence Number', 'First Payment Date'], keep='first')

    # Load Pools Data
    combined_pools_files = glob.glob(f'{save_folder}/gnma_combined_pools_*.parquet')
    combined_pools_files.sort(reverse=True)
    combined_pools_file = combined_pools_files[0]
    df_pools = pl.scan_parquet(combined_pools_file)
    df_pools = df_pools.drop(['Record Type','As of Date'])
    df_pools = df_pools.rename({'Issuer ID': 'Pool Issuer ID'})
    df = df.join(df_pools, on=['Pool ID'], how='left')
    del df_pools

    # Load Liquidations Data
    combined_liquidation_files = glob.glob(f'{save_folder}/gnma_combined_loan_liquidations_*.parquet')
    combined_liquidation_files.sort(reverse=True)
    combined_liquidation_file = combined_liquidation_files[0]
    df_liq = pl.scan_parquet(combined_liquidation_file)
    df_liq = df_liq.unique(subset=['Disclosure Sequence Number'], keep='first')
    # Ensure 'Removal Reason' column exists before trying to drop it
    if 'Removal Reason' in df.columns:
        df = df.drop(['Removal Reason'])
    df = df.join(df_liq, on=['Disclosure Sequence Number','First Payment Date'], how='left')
    del df_liq

    # Save Combined File
    save_file = f'{save_folder}/gnma_combined_data{file_suffix}.parquet'
    df.sink_parquet(save_file)

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
    FORMATTING_FILE = PROJECT_DIR / 'dictionary_files/clean/gnma_file_layouts.csv'
    NIMON_FILE = PROJECT_DIR / 'dictionary_files/clean/nimonSFPS_layout_combined.csv'

    # Import Ginnie Mae Data
    for FILE_PREFIX in ['llmon1', 'llmon2', 'dailyllmni'] :
        unzip_gnma_data(RAW_DIR, CLEAN_DIR, FORMATTING_FILE, file_prefix=FILE_PREFIX, record_type='L')
    for FILE_PREFIX in ['dailyllmni'] :
        unzip_gnma_data(RAW_DIR, CLEAN_DIR, FORMATTING_FILE, file_prefix=FILE_PREFIX, record_type='P')
    for FILE_PREFIX in ['dailyllmni'] :
        unzip_gnma_data(RAW_DIR, CLEAN_DIR, FORMATTING_FILE, file_prefix=FILE_PREFIX, record_type='T')
    for FILE_PREFIX in ['nissues'] :
        unzip_gnma_nissues_data(RAW_DIR, CLEAN_DIR, FORMATTING_FILE, file_prefix=FILE_PREFIX, record_type='D')
    for FILE_PREFIX in ['nimonSFPS'] :
        unzip_gnma_nimon_data(RAW_DIR, CLEAN_DIR, NIMON_FILE, file_prefix=FILE_PREFIX, record_type='PS')

    ## SUMMARY
    # Combine GNMA Issuance Data
    # combine_gnma_data(CLEAN_DIR, DATA_DIR, file_prefix='dailyllmni', record_type='L')
    # combine_gnma_pools(CLEAN_DIR, DATA_DIR)
    # combine_gnma_data(CLEAN_DIR, DATA_DIR, file_prefix='nissues', record_type='D')
    # combine_gnma_data(CLEAN_DIR, DATA_DIR, file_prefix='nimonSFPS', record_type='PS')

    # Get GNMA Liquidation Reasons
    # get_liquidation_reasons(CLEAN_DIR, DATA_DIR)

    # Create Final Dataset
    # create_final_dataset(CLEAN_DIR, DATA_DIR, file_suffix='_201309-202503')
