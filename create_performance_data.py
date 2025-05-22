# -*- coding: utf-8 -*-
"""
Created on: Saturday Dec 3 2022
Updated on: Wednesday May 21 2025
@author: Jonathan E. Becker
"""

# Import Packages
import os
import glob
import pandas as pd
from pyarrow import csv
import pyarrow as pa
import pyarrow.parquet as pq
import config

# Create Ginnie Mae Performance Files
def create_yearly_gnma_performance_files(data_folder, save_folder, file_prefix='llmon1', replace=False, verbose=False) :
    """
    Creates yearly performance files from monthly performance.

    Parameters
    ----------
    data_folder : str
        Folder where data is stored.
    save_folder : str
        Folder where cleaned performance data will be saved.
    file_prefix : str, optional
        The prefix of the files to use when creating yearly performance files. The default is 'llmon1'.

    Returns
    -------
    None.

    """

    # Monthly and Numeric Columns
    monthly_columns = [
        'Disclosure Sequence Number',
        'First Payment Date',
        'Unpaid Principal Balance',
        'Loan Age',
        'Months Delinquent',
        'Months Pre-Paid',
        'As of Date',
    ]
    numeric_columns = [
        'Unpaid Principal Balance',
        'First Payment Date',
        'Loan Age',
        'Months Delinquent',
        'Months Pre-Paid',
        'As of Date',
    ]

    # Yearly File Metadata
    combined_metadata_file = f'{save_folder}/yearly_metadata_{file_prefix}.csv'
    if os.path.exists(combined_metadata_file) :
        print('Metadata exists. Only adding information from new files.')
        metadata = pd.read_csv(combined_metadata_file, sep = ',')
    else :
        print('No metadata exists. Creating metadata file')
        metadata = pd.DataFrame(data = [], columns = ['Logged Files'])
        metadata.to_csv(combined_metadata_file, sep = ',', index = False)

    # Import and Combine Files
    files = glob.glob(f'{data_folder}/{file_prefix}_*.parquet')
    for file in files :

        # Convert Name to be Linux Friendly
        linux_file_name = file.replace('V:/','/')

        # Add Information from File if not Logged in Metadata File
        if not linux_file_name in metadata['Logged Files'].tolist() :

            # Read File
            if verbose :
                print("Importing Performance Variables from File:", file)
            df = pd.read_parquet(file, columns=monthly_columns)

            # Convert Columns to Numerics, Normalize UPBs, and Convert Dates
            for col in numeric_columns :
                df[col] = pd.to_numeric(df[col], errors = 'coerce')
            df['Unpaid Principal Balance'] = df['Unpaid Principal Balance']/100
            df['As of Date'] = pd.to_datetime(df['As of Date'], format='%Y%m')
            df['First Payment Date'] = pd.to_datetime(df['First Payment Date'], format='%Y%m%d')

            # Add Monthly Performance Files Year-by-Year
            df['First Payment Year'] = pd.DatetimeIndex(df['First Payment Date']).year
            for year in df['First Payment Year'].drop_duplicates().sort_values() :

                # Display Progress
                if verbose :
                    print('Adding data for year:', year)

                # Yearly File Name
                yearly_file_name = f'{save_folder}/yearly_performance_{file_prefix}_{year}.csv.gz'

                # Write Yearly Data to Yearly Files
                df_y = df.loc[df['First Payment Year'] == year]
                df_y = df_y.drop(columns = ['First Payment Year'])
                if os.path.exists(yearly_file_name) :
                    df_y.to_csv(yearly_file_name,
                                sep = '|',
                                index = False,
                                mode = 'a',
                                header = False,
                                compression = 'gzip',
                                )
                else :
                    df_y.to_csv(yearly_file_name,
                                sep = '|',
                                index = False,
                                mode = 'a',
                                header = True,
                                compression = 'gzip',
                                )

            # Create Line in Metadata Log
            file_log = pd.DataFrame(data = [linux_file_name], columns = ['Logged Files'])
            file_log.to_csv(combined_metadata_file,
                            sep = ',',
                            mode = 'a',
                            index = False,
                            header = False,
                            )

        # Skip if File Already Logged
        else :

            if verbose :
                print('File', file, 'already logged.')

# Create Performance Summaries
def create_performance_summaries(data_folder, save_folder, file_prefixes = ['llmon1','llmon2'], DELTA = .98, min_year = 1983, max_year = 2023, verbose = False) :
    """
    Create performance summary files with summary metrics.

    Parameters
    ----------
    data_folder : TYPE
        DESCRIPTION.
    save_folder : TYPE
        DESCRIPTION.
    file_prefix : TYPE, optional
        DESCRIPTION. The default is 'llmon1'.
    DELTA : TYPE, optional
        DESCRIPTION. The default is .98.

    Returns
    -------
    None.

    """

    # Convert Yearly Discount Rate to Monthly
    RATE = (1 - DELTA)/DELTA
    rate = 12*((1+RATE)**(1/12) - 1)
    delta = 1/(1+rate/12)

    # CSV Read Options
    id_columns = ['Disclosure Sequence Number']
    use_columns = ['Disclosure Sequence Number', 'As of Date', 'Unpaid Principal Balance', 'Loan Age', 'First Payment Date']

    # Set PyArrow Read/Write Options
    parse_options = csv.ParseOptions(delimiter = '|')
    convert_options = csv.ConvertOptions(include_columns = use_columns)
    write_options = csv.WriteOptions(include_header = True, delimiter = '|')

    # Loop Over Years
    for year in range(min_year, max_year+1) :

        # File
        save_file = f'{save_folder}/gnma_yearly_performance_summary_{year}.csv.gz'

        # Load Data
        df = []
        for file_prefix in file_prefixes :
            file = f"{data_folder}/yearly_performance_{file_prefix}_{year}.csv.gz"
            df_a = csv.read_csv(file, parse_options = parse_options, convert_options = convert_options).to_pandas()
            df.append(df_a)
        df = pd.concat(df)

        # Drop Observations Missing As of Date
        df = df.loc[~pd.isna(df['As of Date'])]

        # Create Discount Factor
        df['Discount Factor'] = delta**df['Loan Age']

        # First and Last-Observed Information
        summary_df = df.sort_values(by = ['As of Date'], ascending = True).drop_duplicates(subset = id_columns)
        summary_df.rename(columns = {'Unpaid Principal Balance':'First Observed UPB',
                                     'Loan Age': 'First Observed Loan Age',
                                     'As of Date': 'First Observed Date'},
                          inplace = True)
        summary_df.drop(columns = ['Discount Factor'], inplace = True)

        temp = df.sort_values(by = ['As of Date'], ascending = False).drop_duplicates(subset = id_columns)
        temp.rename(columns = {'Unpaid Principal Balance':'Last Observed UPB',
                               'Loan Age': 'Last Observed Loan Age',
                               'As of Date': 'Last Observed Date'},
                    inplace = True)
        temp.drop(columns = ['Discount Factor', 'First Payment Date'], inplace = True)
        summary_df = summary_df.merge(temp, on = id_columns, how = 'left')

        # Add Age Specific Summaries
        df = df[['Disclosure Sequence Number', 'Loan Age', 'Unpaid Principal Balance', 'Discount Factor']]
        for age in range(6, 66, 6) :

            # UPB at Time
            temp = df.loc[df['Loan Age'] == age]
            temp = temp[['Disclosure Sequence Number', 'Unpaid Principal Balance']]
            temp.sort_values(by = ['Disclosure Sequence Number','Unpaid Principal Balance'],
                             ascending = True,
                             inplace = True)
            temp.drop_duplicates(subset = ['Disclosure Sequence Number'], inplace = True)
            temp.rename(columns = {'Unpaid Principal Balance': f'UPB at {age} Months'},
                        inplace = True)
            summary_df = summary_df.merge(temp, on = id_columns, how = 'left')
        
            # Discounted Sum
            temp = df.loc[(df['Loan Age'] > 0) & (df['Loan Age'] <= age)]
            temp.sort_values(by = ['Disclosure Sequence Number','Loan Age','Unpaid Principal Balance'],
                             ascending = True,
                             inplace = True)
            temp.drop_duplicates(subset = ['Disclosure Sequence Number','Loan Age'], inplace = True)
            temp['Discounted UPB'] = temp['Unpaid Principal Balance']*temp['Discount Factor']
            temp = temp.groupby(id_columns)['Discounted UPB'].sum()
            temp = temp.to_frame().reset_index()
            temp.rename(columns = {'index': id_columns[0],
                                   'Discounted UPB': f'Discounted UPB {age}'},
                        inplace = True)
            summary_df = summary_df.merge(temp, on = id_columns, how = 'left')

        # Round UPB Values to Two Decimal Places
        upb_cols = {x:2 for x in summary_df.columns if "UPB" in x}
        summary_df = summary_df.round(upb_cols)

        # Convert IDs to Strings (Needed for PyArrow Save)
        for col in id_columns :
            summary_df[col] = summary_df[col].astype('str')

        # Combine and Save (Write with PyArrow)
        dt = pa.Table.from_pandas(summary_df, preserve_index = False)
        with pa.CompressedOutputStream(save_file, "gzip") as out:
            csv.write_csv(dt, out, write_options = write_options)

# Create Performance Summaries
def create_performance_summary_final(data_folder, save_folder, file_prefixes = ['llmon1','llmon2'], DELTA = .98, min_year = 1983, max_year = 2023, verbose = False) :
    """
    Construct the final version of the performance summaries.

    Parameters
    ----------
    data_folder : TYPE
        DESCRIPTION.
    save_folder : TYPE
        DESCRIPTION.
    file_prefix : TYPE, optional
        DESCRIPTION. The default is 'llmon1'.
    DELTA : TYPE, optional
        DESCRIPTION. The default is .98.

    Returns
    -------
    None.

    """

    # Convert Yearly Discount Rate to Monthly
    RATE = (1 - DELTA)/DELTA
    rate = 12*((1+RATE)**(1/12) - 1)
    delta = 1/(1+rate/12)

    # CSV Read Options
    id_columns = ['Disclosure Sequence Number']
    use_columns = ['Disclosure Sequence Number', 'As of Date', 'Unpaid Principal Balance', 'Loan Age', 'First Payment Date']

    # Set PyArrow Read/Write Options
    parse_options = csv.ParseOptions(delimiter = '|')
    convert_options = csv.ConvertOptions(include_columns = use_columns)
    write_options = csv.WriteOptions(include_header = True, delimiter = '|')

    # Loop Over Years
    for year in range(min_year, max_year+1) :

        # File
        save_file = f'{save_folder}/gnma_final_performance_summary_{year}.csv.gz'

        # Load Data
        df = []
        for file_prefix in file_prefixes :
            file = f"{data_folder}/yearly_performance_{file_prefix}_{year}.csv.gz"
            df_a = csv.read_csv(file, parse_options = parse_options, convert_options = convert_options).to_pandas()
            df.append(df_a)
        df = pd.concat(df)

        # Drop Observations Missing As of Date
        df = df.loc[~pd.isna(df['As of Date'])]

        # Create Discount Factor
        df['Discount Factor'] = delta**df['Loan Age']

        # Information at first observed date
        summary_df = df.sort_values(by = ['As of Date'], ascending = True).drop_duplicates(subset = id_columns)
        summary_df.rename(columns = {'Unpaid Principal Balance':'First Observed UPB',
                                     'Loan Age': 'First Observed Loan Age',
                                     'As of Date': 'First Observed Date'},
                          inplace = True)
        summary_df.drop(columns = ['Discount Factor'], inplace = True)

        # Information at last observed date
        temp = df.sort_values(by = ['As of Date'], ascending = False).drop_duplicates(subset = id_columns)
        temp.rename(columns = {'Unpaid Principal Balance':'Last Observed UPB',
                               'Loan Age': 'Last Observed Loan Age',
                               'As of Date': 'Last Observed Date'},
                    inplace = True)
        temp.drop(columns = ['Discount Factor', 'First Payment Date'], inplace = True)
        summary_df = summary_df.merge(temp, on = id_columns, how = 'left')

        # Add Discounted Sum of UPBs
        df = df[['Disclosure Sequence Number', 'Loan Age', 'Unpaid Principal Balance', 'Discount Factor']]
        temp = df.loc[df['Loan Age'] > 0]
        temp.sort_values(by = ['Disclosure Sequence Number', 'Loan Age', 'Unpaid Principal Balance'],
                         ascending = True,
                         inplace = True)
        temp.drop_duplicates(subset = ['Disclosure Sequence Number','Loan Age'], inplace = True)
        temp['Discounted UPB'] = temp['Unpaid Principal Balance']*temp['Discount Factor']
        temp = temp.groupby(id_columns)['Discounted UPB'].sum()
        temp = temp.to_frame().reset_index()
        temp.rename(columns = {'index': id_columns[0]}, inplace = True)
        summary_df = summary_df.merge(temp, on = id_columns, how = 'left')

        # Round UPB Values to Two Decimal Places
        upb_cols = {x:2 for x in summary_df.columns if "UPB" in x}
        summary_df = summary_df.round(upb_cols)

        # Convert IDs to Strings (Needed for PyArrow Save)
        for col in id_columns :
            summary_df[col] = summary_df[col].astype('str')

        # Combine and Save (Write with PyArrow)
        dt = pa.Table.from_pandas(summary_df, preserve_index = False)
        with pa.CompressedOutputStream(save_file, "gzip") as out:
            csv.write_csv(dt, out, write_options = write_options)

# Combine Performance Summaries
def combine_performance_summaries(data_folder, save_folder, file_suffix = '', verbose = False) :
    """
    Combine all performance summary files for master file containing all
    relevant performance data

    Parameters
    ----------
    data_folder : TYPE
        DESCRIPTION.
    save_folder : TYPE
        DESCRIPTION.
    file_suffix : TYPE, optional
        DESCRIPTION. The default is ''.

    Returns
    -------
    None.

    """

    # CSV Read/Write Options
    parse_options = csv.ParseOptions(delimiter = '|')
    write_options = csv.WriteOptions(include_header = True, delimiter = '|')

    # Combine Files
    files = glob.glob(f"{data_folder}/gnma_yearly_performance_summary_*.csv.gz")
    files.sort()
    df = []
    for file in files :
        if verbose :
            print('Adding Performance Summary Data from File:', file)
        df_a = csv.read_csv(file, parse_options = parse_options).to_pandas()
        df.append(df_a)
    df = pd.concat(df)

    # Combine and Save (Write with PyArrow)
    save_file = f'{save_folder}/gnma_combined_performance_summary{file_suffix}.csv.gz'
    dt = pa.Table.from_pandas(df, preserve_index = False)
    with pa.CompressedOutputStream(save_file, "gzip") as out:
        csv.write_csv(dt, out, write_options = write_options)

# Performance Time-Series
def create_performance_time_series(data_folder, save_folder, min_year=1983, max_year=2025) :
    """
    Create time-series files of performance. Will save files with the full set
    of loan balances at each time period.

    Parameters
    ----------
    data_folder : TYPE
        DESCRIPTION.
    save_folder : TYPE
        DESCRIPTION.

    Returns
    -------
    None.

    """
    
    for year in range(min_year, max_year+1) :

        df = []
        for lltype in range(1, 2 + 1) :

            # Load Data
            file = DATA_DIR / f"clean/yearly_performance_llmon{lltype}_{year}.csv.gz"
            parse_options = csv.ParseOptions(delimiter = '|')
            df_a = csv.read_csv(file, parse_options = parse_options).to_pandas(date_as_object = False)
            df.append(df_a)
        
        # Combine GNMA Is and GNMA IIs
        df = pd.concat(df)

        # Convert Loan Age to Integer
        df['Loan Age'] = df['Loan Age'].astype('Int16')

        # Impute Loan Ages
        df['As of Year'] = pd.to_datetime(df['As of Date']).dt.year
        df['As of Month'] = pd.to_datetime(df['As of Date']).dt.month
        df['First Payment Year'] = pd.to_datetime(df['First Payment Date']).dt.year
        df['First Payment Month'] = pd.to_datetime(df['First Payment Date']).dt.month
        df['Imputed Loan Age'] = 1 + 12*(df['As of Year'] - df['First Payment Year']) + (df['As of Month'] - df['First Payment Month'])
        df['Loan Age'] = df['Loan Age'].fillna(df['Imputed Loan Age'])
        df = df.drop(columns = ['First Payment Year', 'First Payment Month', 'As of Year', 'As of Month', 'Imputed Loan Age'])

        # Convert Loan Age to a String
        df['Loan Age'] = df['Loan Age'].astype('str')
        entries = {}
        for group in df.groupby(['Disclosure Sequence Number']) :
            entry = group[1][['Unpaid Principal Balance','Loan Age', 'Months Delinquent', 'Months Pre-Paid']].set_index(['Loan Age']).to_dict()
            entry['First Payment Date'] = group[1]['First Payment Date'].iloc[0]
            entries[str(group[0])] = entry

        # Save Time Series
        df_js = pd.DataFrame.from_dict(entries, orient = 'index')
        dt = pa.Table.from_pandas(df_js, preserve_index = True)
        pq.write_table(dt, DATA_DIR / f'gnma_performance_time_series_{year}.parquet')

# Main Routine
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

    # Create GNMA Yearly Performance Files
    for FILE_PREFIX in ['llmon1', 'llmon2'] :
        create_yearly_gnma_performance_files(CLEAN_DIR, CLEAN_DIR, file_prefix=FILE_PREFIX)
    create_performance_summary_final(CLEAN_DIR, CLEAN_DIR, file_prefixes=['llmon1','llmon2'], DELTA=.98, min_year=1983, max_year=2025)
    create_performance_summaries(CLEAN_DIR, CLEAN_DIR, file_prefixes=['llmon1','llmon2'], DELTA=.98, min_year=1983, max_year=2025)

    # Combine Performance Summaries
    combine_performance_summaries(CLEAN_DIR, DATA_DIR, file_suffix='_1983-2025')
