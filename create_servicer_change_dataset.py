#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on: Sat Dec 3 09:49:44 2022
Last updated on: Friday May 2 23:21:00 2025
@author: Jonathan E. Becker
"""

# Import Packages
import os
import glob
import pandas as pd
import numpy as np
from dateutil import relativedelta
import config

# Import GNMA Servicer Changes
def import_gnma_servicer_changes(data_folder, save_folder, file_suffix = '', verbose = False) :
    """
    Track changes in active servicer occurring after issuance.

    Parameters
    ----------
    data_folder : str
        Folder where data is stored as parquet files.
    save_folder : str
        Folder where the combined servicing transfer file will be saved.
    file_suffix : str, optional
        String to add to the end of the combined file paths. The default is ''.
    verbose : bool, optional
        Whether to print progress while loading. The default is False.

    Returns
    -------
    None.

    """

    # Read and Write Options
    columns = [
        'Disclosure Sequence Number',
        'As of Date',
        'Issuer ID',
        'Seller Issuer ID',
        'Loan Age',
        'First Payment Date',
        'Third-Party Origination Type',
    ]

    # Get Performance Files
    files = glob.glob(f'{data_folder}/llmon1_*.parquet')
    files += glob.glob(f'{data_folder}/llmon2_*.parquet')
    files.sort(reverse = True)

    # Read Monthly Servicer Changes
    df = []
    for file in files :
        
        # Display Progress
        if verbose :
            print('Importing servicer changes from file:', file)

        try :
            df_a = pd.read_parquet(file, columns=columns, filters=[('Seller Issuer ID','not in',[None, np.nan])])
            df_a['As of Date'] = pd.to_datetime(df_a['As of Date'], format='%Y%m')
            df_a['First Payment Date'] = pd.to_datetime(df_a['First Payment Date'], format='%Y%m%d')
            df.append(df_a)
            del df_a
        except Exception as e :
            print('Error:', e, '\nSkipping for now, but you may want to investigate.')
            pass

    # Combine Monthly DataFrames
    df = pd.concat(df)

    # Combine and Save (Write with PyArrow)
    save_file = f'{save_folder}/gnma_combined_loan_servicer_changes{file_suffix}.parquet'
    df.to_parquet(save_file, index=False)

    # Replace Loan Age Where Missing
    df['Loan Age (New)'] = [relativedelta.relativedelta(x['As of Date'],x['First Payment Date']) for i,x in df.iterrows()]
    df['Loan Months'] = [x['Loan Age (New)'].months + 12*x['Loan Age (New)'].years + 1 for _,x in df.iterrows()]
    df['Loan Age'] = df['Loan Age'].fillna(df['Loan Months'])
    df = df.drop(columns=['Loan Age (New)','Loan Months'])

    # Rank
    df = df.sort_values(by=['Disclosure Sequence Number','First Payment Date','Loan Age'])
    df['Rank'] = df.groupby(['Disclosure Sequence Number','First Payment Date'])['Loan Age'].transform('rank')

    # Combine
    df_master = df.query('Rank == 1')[['Disclosure Sequence Number','First Payment Date']]
    for rank in range(1, int(np.max(df['Rank'])) + 1) :
        df_a = df.query(f'Rank == {rank}')
        df_a = df_a[['Disclosure Sequence Number','As of Date','Issuer ID','Seller Issuer ID','Loan Age','First Payment Date']]
        for column in ['As of Date', 'Issuer ID', 'Seller Issuer ID', 'Loan Age'] :
            df_a = df_a.rename(columns = {column: f'{column} {rank}'})
        df_master = df_master.merge(
            df_a,
            on=['Disclosure Sequence Number','First Payment Date'],
            how='left',
        )

    # Save Wide
    df_master.to_parquet(f'{save_folder}/gnma_combined_loan_servicer_changes_wide{file_suffix}.parquet', index=False)

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

    # Get GNMA Servicer Changes
    import_gnma_servicer_changes(CLEAN_DIR, DATA_DIR, file_suffix='_201309-202412')
