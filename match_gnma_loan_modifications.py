#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on: Sat Dec 3 09:49:44 2022
Last updated on: Friday May 2 23:21:00 2025
@author: Jonathan E. Becker
"""

# Import Packages
import os
import pandas as pd
import config

# Match Modified and Non-modified Loans
def match_gnma_loan_modifications(gnma_file) :
    """
    Match modified and non-modified loans in GNMA data.

    Parameters
    ----------
    gnma_file : str
        Master GNMA file.

    Returns
    -------
    None.

    """

    # Load Data
    gnma_file = DATA_DIR / 'gnma_combined_data_201309-202502.parquet'
    df = pd.read_parquet(gnma_file, filters = [('Loan Origination Date','>=',20180101)])

    # Sort Data
    group_columns = ['Loan Origination Date', 'Original Principal Balance', 'State', 'Agency', 'Loan Interest Rate']
    df['idx'] = df.groupby(group_columns).ngroup()
    df = df.sort_values(by = group_columns+['Seller Issuer ID'])
    
    # Check if Group Has Both Modified and Non-Modified Loan
    df['i_Modification'] = (df['Loan Purpose'] == 5)
    df['i_LoanHasModification'] = df.groupby(['idx'])['i_Modification'].transform('max')
    df = df.query('i_LoanHasModification == 1')
    df['i_LoanHasNonModification'] = 1 - df.groupby(['idx'])['i_Modification'].transform('min')
    df = df.query('i_LoanHasNonModification == 1')

    # Drop Temporary Columns
    df = df.drop(columns = ['idx','i_LoanHasModification','i_LoanHasNonModification','i_Modification'])

    # Separate Mods and Non-Mods then Merge
    df_mods = df.loc[df['Loan Purpose'] == 5]
    df_nonmods = df.loc[df['Loan Purpose'] != 5]
    df = df_mods.merge(df_nonmods,
                       on = group_columns,
                       how = 'inner',
                       suffixes = (' (Mods)',' (Non Mods)'),
                       )

    # Sort
    df = df[df.columns.sort_values()]
    df = df[group_columns+[x for x in df.columns if x not in group_columns]]
    df = df.sort_values(by = group_columns)

    # Date Match
    df = df.loc[df['As of Date (Mods)'] >= df['Liquidation Date (Non Mods)']]

    # Keep Uniques
    df['CountModification'] = df.groupby(['Disclosure Sequence Number (Mods)'])['Agency'].transform('count')
    df['CountNonmodification'] = df.groupby(['Disclosure Sequence Number (Non Mods)'])['Agency'].transform('count')
    df = df.query('CountModification == 1 & CountNonmodification == 1')
    df = df.drop(columns = ['CountModification','CountNonmodification'])

    # Strict Matches
    df = df.loc[df['Credit Score (Mods)'] == df['Credit Score (Non Mods)']]
    df = df.loc[df['Number of Borrowers (Mods)'] == df['Number of Borrowers (Non Mods)']]

    # Return Potential Matches
    return df

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

    # Match Modified and Non-modified Loans
    match_gnma_loan_modifications(gnma_file = DATA_DIR / 'gnma_combined_data_201309-202502.parquet')
