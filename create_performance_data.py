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
import numpy as np
from pyarrow import csv
import pyarrow as pa
from dateutil import relativedelta
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

# Combine Ginnie Mae Data
def combine_gnma_pools(data_folder, save_folder, file_suffix = '') :
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

    # Load
    df = []
    files = glob.glob(f'{data_folder}/*dailyllmni_*P*.parquet')
    files.sort()
    for file in files :
        df_a = pq.read_table(file).to_pandas(date_as_object = False)
        df_a.rename(columns = {'As-Of Date':'As of Date'}, inplace = True)
        df.append(df_a)

    # Combine and Save
    df = pd.concat(df)
    dt = pa.Table.from_pandas(df, preserve_index = False)
    pq.write_table(dt, f'{save_folder}/gnma_combined_pools_{file_suffix}.parquet')

# Get GNMA Liquidation Reasons
def get_liquidation_reasons(data_folder, save_folder, file_suffix = '', verbose = False) :
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
    liquidation_columns = ['Disclosure Sequence Number',
                           'As of Date',
                           'Current Month Liquidation Flag',
                           'Removal Reason',
                           'Months Pre-Paid',
                           'Months Delinquent',
                           'Pool ID',
                           'Issuer ID',
                           'First Payment Date',
                           'Unpaid Principal Balance']

    # Get Performance Files
    files = glob.glob(f'{data_folder}/llmon1_*.parquet')
    files += glob.glob(f'{data_folder}/llmon2_*.parquet')
    files.sort(reverse = True)

    # Read Monthly Liquidations
    df = []
    for file in files :

        if verbose :
            print('Importing liquidation/termination reasons from file:', file)

        try :
            df_a = pq.read_table(file,
                                 columns = liquidation_columns,
                                 filters = [('Current Month Liquidation Flag','==','Y')],
                                 ).to_pandas(date_as_object = False)
            df_a.drop(columns = ['Current Month Liquidation Flag'], inplace = True)
            df_a['Removal Reason'] = [int(x) for x in df_a['Removal Reason']]
            df.append(df_a)
        except :
            pass

    # Combine Monthly DataFrames
    df = pd.concat(df)

    # Rename Columns
    df = df.rename(columns = {'As of Date': 'Liquidation Date',
                              'Months Pre-Paid': 'Months Pre-Paid at Liquidation',
                              'Months Delinquent': 'Months Delinquent at Liquidation',
                              'Pool ID': 'Final Pool ID',
                              'Issuer ID': 'Final Issuer ID',
                              'Unpaid Principal Balance': 'Final Unpaid Principal Balance'})

    # Combine and Save (Write with PyArrow)
    save_file = f'{save_folder}/gnma_combined_loan_liquidations{file_suffix}.parquet'
    dt = pa.Table.from_pandas(df, preserve_index = False)
    pq.write_table(dt, save_file)

# Add Old Observations to Issuance Data
def create_final_dataset(data_folder, save_folder, file_suffix = '') :
    """
    Create final dataset from issuance, pools, and liquidations data.

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

    # Load Combined Issuances Data
    combined_issuance_file = f'{save_folder}/dailyllmni_combined{file_suffix}L.parquet'
    df = pq.read_table(combined_issuance_file).to_pandas(date_as_object = False)

    # Read Initial Observations
    first_period_files = [f'{data_folder}/llmon1_201310L.parquet', f'{data_folder}/llmon2_201310L.parquet']
    df_old = []
    for file in first_period_files :
        df_a = pq.read_table(file).to_pandas(date_as_object = False)
        df_old.append(df_a)
    df_old = pd.concat(df_old)
    df_old['As of Date'] = 201310

    # Append and Drop Duplicates
    df = pd.concat([df, df_old])
    df = df.drop_duplicates(subset = ['Disclosure Sequence Number', 'First Payment Date'])

    # Load Pools Data
    file_pools = f'{save_folder}/gnma_combined_pools{file_suffix}.parquet'
    df_pools = pq.read_table(file_pools).to_pandas(date_as_object = False)
    df_pools = df_pools.drop(columns = ['Record Type','As of Date'])
    df_pools = df_pools.rename(columns = {'Issuer ID': 'Pool Issuer ID'})
    df = df.merge(df_pools,
                  on = ['Pool ID'],
                  how = 'left',
                  )

    # Load Liquidations Data
    file_liq = f'{save_folder}/gnma_combined_loan_liquidations{file_suffix}.parquet'
    df_liq = pq.read_table(file_liq).to_pandas(date_as_object = False)
    df_liq = df_liq.drop_duplicates(subset = ['Disclosure Sequence Number'])
    df = df.drop(columns = ['Removal Reason'])
    df = df.merge(df_liq,
                  on = ['Disclosure Sequence Number', 'First Payment Date'],
                  how = 'left',
                  )

    # Save Combined File
    save_file = f'{save_folder}/gnma_combined_data{file_suffix}.parquet'
    dt = pa.Table.from_pandas(df, preserve_index = False)
    pq.write_table(dt, save_file)

#%% PERFORMANCE
# Create Ginnie Mae Performance Files
def create_yearly_gnma_performance_files(data_folder, save_folder, file_prefix='llmon1', replace=False, verbose=False) :
    """
    Creates yearly performance files from monthly performance.

    Parameters
    ----------
    data_folder : TYPE
        DESCRIPTION.
    save_folder : TYPE
        DESCRIPTION.
    file_prefix : TYPE, optional
        DESCRIPTION. The default is 'llmon1'.

    Returns
    -------
    None.

    """

    # Monthly and Numeric Columns
    monthly_columns = ['Disclosure Sequence Number',
                       'First Payment Date',
                       'Unpaid Principal Balance',
                       'Loan Age',
                       'Months Delinquent',
                       'Months Pre-Paid',
                       'As of Date']
    numeric_columns = ['Unpaid Principal Balance',
                       'First Payment Date',
                       'Loan Age',
                       'Months Delinquent',
                       'Months Pre-Paid',
                       'As of Date']

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
        linux_file_name = file.replace('V:/',f'/')

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
def create_performance_time_series(data_folder, save_folder) :
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
    
    for year in range(1983, 2023 + 1) :

        df = []
        for lltype in range(1, 2 + 1) :

            # Load Data
            file = DATA_DIR / f"clean/yearly_performance_llmon{lltype}_{year}.csv.gz"
            parse_options = csv.ParseOptions(delimiter = '|')
            df_a = csv.read_csv(file, parse_options = parse_options).to_pandas(date_as_object = False)
            df.append(df_a)
        
        # Combine GNMAIs and GNMAIIs
        df = pd.concat(df)

        df['Loan Age'] = df['Loan Age'].astype('Int16')

        # Impute Loan Ages
        df['As of Year'] = pd.to_datetime(df['As of Date']).dt.year
        df['As of Month'] = pd.to_datetime(df['As of Date']).dt.month
        df['First Payment Year'] = pd.to_datetime(df['First Payment Date']).dt.year
        df['First Payment Month'] = pd.to_datetime(df['First Payment Date']).dt.month
        df['Imputed Loan Age'] = 1 + 12*(df['As of Year'] - df['First Payment Year']) + (df['As of Month'] - df['First Payment Month'])
        df['Loan Age'] = df['Loan Age'].fillna(df['Imputed Loan Age'])
        df = df.drop(columns = ['First Payment Year', 'First Payment Month', 'As of Year', 'As of Month', 'Imputed Loan Age'])

        #
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

#%% ISSUERS
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
    df = pd.read_csv(f'{data_folder}/issrinfo_combined_{issrinfo_suffix}.csv.gz',
                     compression = 'gzip',
                     sep = '|',
                     )
    
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
    df.to_csv(f'{save_folder}/issrinfo_combined_cleaned_{issrinfo_suffix}.csv.gz',
              compression = 'gzip',
              sep = '|',
              index = False,
              )
    
    ## Issuer Data
    # Import Data
    df = pd.read_csv(f'{data_folder}/issuers_combined_{issuers_suffix}.csv.gz',
                     compression = 'gzip',
                     sep = '|',
                     )
    
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
    df.to_csv(f'{save_folder}/issuers_combined_cleaned_{issuers_suffix}.csv.gz',
              compression = 'gzip',
              sep = '|',
              index = False,
              )

#%% MISCELLANEOUS
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
        except :
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

    ## PERFORMANCE
    # Create GNMA Yearly Performance Files
    for FILE_PREFIX in ['llmon1', 'llmon2'] :
        create_yearly_gnma_performance_files(CLEAN_DIR, CLEAN_DIR, file_prefix=FILE_PREFIX)
    create_performance_summary_final(CLEAN_DIR, CLEAN_DIR, file_prefixes=['llmon1','llmon2'], DELTA=.98, min_year=1983, max_year=2025)
    create_performance_summaries(CLEAN_DIR, CLEAN_DIR, file_prefixes=['llmon1','llmon2'], DELTA=.98, min_year=1983, max_year=2025)

    # Combine Performance Summaries
    combine_performance_summaries(CLEAN_DIR, DATA_DIR, file_suffix='_1983-2025')
