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
import pandas as pd
import numpy as np
import config
import re

# Get Combined Suffix
def get_combined_suffix(files: list) :
    """Create a suffix for a combined file from a list of individual files."""

    # Get Suffixes from File Names and Create Combined Suffix from Min and Max Dates
    suffixes = [os.path.splitext(os.path.basename(file))[0].split('_')[-1] for file in files]
    suffixes = ["".join(re.findall(r'\d', suffix)) for suffix in suffixes] # Extract only numeric characters
    combined_suffix = '_'+min(suffixes)+'-'+max(suffixes)

    # Return Combined Suffix
    return combined_suffix

#%% Combine Issuers
# Combine Ginnie Mae Issuer Files
def combine_gnma_issuer_files(data_folder, save_folder, formatting_file, file_prefix='issrinfo', file_suffix='') :
    """
    Combine GNMA issuer files for either issrinfo or issuer (active) prefixes.

    Parameters
    ----------
    data_folder : TYPE
        DESCRIPTION.
    save_folder : TYPE
        DESCRIPTION.
    formatting_file : TYPE
        DESCRIPTION.
    file_prefix : TYPE, optional
        DESCRIPTION. The default is 'issrinfo'.
    file_suffix : TYPE, optional
        DESCRIPTION. The default is ''.

    Returns
    -------
    None.

    """

    # Get Formatting
    formatting = pd.read_csv(formatting_file)
    formats = formatting.loc[formatting['File Prefix'] == file_prefix]

    # Get Issuer Files
    files = glob.glob(f'{data_folder}/{file_prefix}_*.txt')
    files.sort()

    # Add Issuer Data One Month at a Time
    issuers = []
    for file in files :

        # Display Progress
        print('Adding issuers from file:', file)
        ym = os.path.basename(file).split(f'{file_prefix}_')[1].split('.txt')[0]

        # Read File
        with open(file, encoding = 'iso-8859-1') as f :
            lines = f.readlines()

        # Read Data
        df = pd.DataFrame([])
        for _, field in formats.iterrows() :

            # Read Format File
            field_name = field['Data Item'].strip()
            begin_char = field['Begin'] - 1
            end_char = field['End']

            # Display Progress and Read Lines
            df[field_name] = [x[begin_char:end_char].strip() for x in lines]
            
            # Convert if Numeric
            if field['Type'] == 'Numeric' :
                df[field_name] = pd.to_numeric(df[field_name], errors='coerce')

        # Append Monthly Issuers
        df['Year/Month'] = int(ym)
        issuers.append(df)

    # Combine and Save
    issuers = pd.concat(issuers)
    filler_columns = [x for x in issuers.columns if 'Filler' in x]
    issuers = issuers.drop(columns=filler_columns)
    issuers.to_parquet(
        f'{save_folder}/{file_prefix}_combined_{file_suffix}.parquet',
        index = False,
    )

# Clean GNMA Issuers
def clean_gnma_issuers(data_folder, save_folder, issrinfo_suffix = '', issuers_suffix = '') :
    """
    Combine and clean issuer data, keeping only one observation at the level of
    institution-by-location.

    Parameters
    ----------
    data_folder : TYPE
        DESCRIPTION.
    save_folder : TYPE
        DESCRIPTION.
    issrinfo_suffix : TYPE, optional
        DESCRIPTION. The default is ''.
    issuers_suffix : TYPE, optional
        DESCRIPTION. The default is ''.

    Returns
    -------
    None.

    """
    
    ## Issuer Info Data
    # Import Data
    df = pd.read_parquet(f'{data_folder}/issrinfo_combined_{issrinfo_suffix}.parquet')
    
    # Create Group ID Variable
    df['Index'] = range(0, df.shape[0])
    df['Group ID'] = df.groupby(['Issuer ID', 'Program', 'Issuer Name', 'Issuer Address', 'Issuer City', 'Issuer State', 'Issuer Zip Code', 'Issuer Phone Number', 'Issuer Status Indicator'], dropna = False)['Index'].transform('min')
    
    # Convert Dates
    df['Record As-Of Date'] = pd.to_datetime(df['Record As-Of Date'], format='%Y%m%d')
    df['Year/Month'] = pd.to_datetime(df['Year/Month'], format='%Y%m')
    
    # Get First Date by Group
    df.sort_values(by = ['Issuer ID', 'Year/Month'], inplace = True)
    df['Group ID Shifted'] = df['Group ID'].shift(1)
    df['1(Same Group As Above)'] = (df['Group ID'] == df['Group ID Shifted'])
    df['First Observed Date'] = np.nan
    df.loc[df['1(Same Group As Above)'] == False, 'First Observed Date'] = df.loc[df['1(Same Group As Above)'] == False, 'Year/Month']
    df['First Observed Date'] = df[['First Observed Date']].ffill(axis = 0)

    # Create New Group and Get Last Date
    df.drop(columns = ['Index', 'Group ID', 'Group ID Shifted', '1(Same Group As Above)'], inplace = True)
    df['Last Observed Date'] = df.groupby(['Issuer ID', 'Program', 'Issuer Name', 'Issuer Address', 'Issuer City', 'Issuer State', 'Issuer Zip Code', 'Issuer Phone Number', 'Issuer Status Indicator', 'First Observed Date'], dropna = False)['Year/Month'].transform('max')

    # Drop Duplicates and Clean Up
    df.drop(columns = ['Record As-Of Date', 'Year/Month'], inplace = True)
    df.drop_duplicates(inplace = True)
    df.sort_values(by = ['Issuer ID', 'First Observed Date'], inplace = True)
    df.reset_index(drop = True, inplace = True)
    
    # Save
    df.to_parquet(f'{save_folder}/issrinfo_combined_cleaned_{issrinfo_suffix}.parquet', index = False)
    
    ## Issuer Data
    # Import Data
    df = pd.read_parquet(f'{data_folder}/issuers_combined_{issuers_suffix}.parquet')
    
    # Create Group ID Variable
    df['Index'] = range(0, df.shape[0])
    df['Group ID'] = df.groupby(['Issuer Number', 'Issuer Name (Short Name)', 'Issuer Name (Long Name)',
                                 'Issuer Address 1', 'Issuer Address 2', 'Issuer City', 'Issuer State',
                                 'Issuer Zip', 'Issuer Phone Number 1', 'Issuer Phone Number 2',
                                 'Issuer Phone Number 3', 'Issuer Contact Name 1',
                                 'Issuer Contact Name 2', 'Issuer Contact Name 3',], dropna = False)['Index'].transform('min')
    
    # Convert Dates
    df['Year/Month'] = pd.to_datetime(df['Year/Month'], format='%Y%m')
    
    # Get First Date by Group
    df.sort_values(by = ['Issuer Number', 'Year/Month'], inplace = True)
    df['Group ID Shifted'] = df['Group ID'].shift(1)
    df['1(Same Group As Above)'] = (df['Group ID'] == df['Group ID Shifted'])
    df['First Observed Date'] = np.nan
    df.loc[df['1(Same Group As Above)'] == False, 'First Observed Date'] = df.loc[df['1(Same Group As Above)'] == False, 'Year/Month']
    df['First Observed Date'] = df[['First Observed Date']].ffill(axis = 0)

    # Create New Group and Get Last Date
    df.drop(columns = ['Index', 'Group ID', 'Group ID Shifted', '1(Same Group As Above)'], inplace = True)
    df['Last Observed Date'] = df.groupby(['Issuer Number', 'Issuer Name (Short Name)', 'Issuer Name (Long Name)',
                                           'Issuer Address 1', 'Issuer Address 2', 'Issuer City', 'Issuer State',
                                           'Issuer Zip', 'Issuer Phone Number 1', 'Issuer Phone Number 2',
                                           'Issuer Phone Number 3', 'Issuer Contact Name 1',
                                           'Issuer Contact Name 2', 'Issuer Contact Name 3', 'First Observed Date'], dropna = False)['Year/Month'].transform('max')

    # Drop Duplicates and Clean Up
    df.drop(columns = ['Year/Month'], inplace = True)
    df.drop_duplicates(inplace = True)
    df.sort_values(by = ['Issuer Number', 'First Observed Date'], inplace = True)
    df.reset_index(drop = True, inplace = True)
    
    # Save
    df.to_parquet(f'{save_folder}/issuers_combined_cleaned_{issuers_suffix}.parquet', index=False)

#%% Import Functions
# Import ptermplatot
def import_ptermplatot():
    pass

# Import ptermot
def import_ptermot():
    pass

# Import Issuers
def import_issuers():
    pass

# Import ptermmon
def import_ptermmon():
    pass

# Import liqloanqtr
def import_liqloanqtr():
    pass

# Import issrinfo
def import_issrinfo():
    pass

# Import issrcutoff
def import_issrcutoff():
    pass

# Import plmonforb
def import_plmonforb():
    pass

# Import llmonforb
def import_llmonforb():
    pass

# Import llpaymhist
def import_llpaymhist():
    pass

# Import LoanPerf
def import_LoanPerf():
    pass

# Import LoanPerfAnn
def import_LoanPerfAnn():
    pass

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

    # Set formatting file
    FORMATTING_FILE = PROJECT_DIR / 'dictionary_files/clean/other_layouts_combined.csv'

    ## ISSUERS
    # Combine Ginnie Issuers
    combine_gnma_issuer_files(RAW_DIR, CLEAN_DIR, FORMATTING_FILE, file_prefix='issrinfo', file_suffix='201804-202503')
    combine_gnma_issuer_files(RAW_DIR, CLEAN_DIR, FORMATTING_FILE, file_prefix='issuers', file_suffix='201208-202503')

    # Clean Ginnie Issuers
    clean_gnma_issuers(CLEAN_DIR, DATA_DIR, issrinfo_suffix='201804-202503', issuers_suffix='201208-202503')
