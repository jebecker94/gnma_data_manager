# Import Packages
import os
import zipfile
import pathlib
import glob
import polars as pl
from io import StringIO
import config
import re

#%% Support Functions
# Read Text from Zips
def read_text_from_zip(zip_file_path: str, encoding: str='utf-8'):
    """
    Opens a zip file, identifies the primary text file within it,
    and returns its filename and content as a string.

    Args:
        zip_file_path (pathlib.Path or str): Path to the zip file.
        encoding (str): The encoding of the text file. Defaults to 'utf-8'.

    Returns:
        tuple (str, str) or (None, None): 
        (filename_in_zip, content_string) if successful,
        (filename_in_zip, None) if decoding or read error for that file,
        (None, None) if no suitable file is found or a major error occurs.
    """

    try:

        with zipfile.ZipFile(zip_file_path, 'r') as zf:

            # Get all non-directory file infos, excluding common metadata folders
            candidate_files = [
                info for info in zf.infolist()
                if not info.is_dir() and not info.filename.startswith('__MACOSX/')
            ]

            if not candidate_files:
                print(f"Info: No processable files found in '{zip_file_path.name}'.")
                return None, None

            target_file_info = None
            if len(candidate_files) == 1:
                target_file_info = candidate_files[0]

            # Handle Multiple Files
            else:
                print(f"Info: Multiple files found in '{zip_file_path.name}'. Attempting to identify the primary text file.")
                common_text_extensions = ('.txt', '.dat', '.csv', '.text', '.log') # Add more if needed
                text_files = [
                    info for info in candidate_files
                    if pathlib.Path(info.filename).suffix.lower() in common_text_extensions
                ]

                if len(text_files) == 1:
                    target_file_info = text_files[0]
                    print(f"Info: Selected text file: '{target_file_info.filename}'")
                elif len(text_files) > 1:
                    target_file_info = text_files[0] # Pick the first one
                    print(f"Warning: Multiple text files ({[f.filename for f in text_files]}) found. Using the first one: '{target_file_info.filename}'")
                else: # No specific text files, pick the first candidate from all non-directory files
                    target_file_info = candidate_files[0]
                    print(f"Warning: No specific text file (e.g., .txt) found among multiple files. Using the first available file: '{target_file_info.filename}'")

            # Decode Content if Found
            if target_file_info:
                try:
                    with zf.open(target_file_info.filename, 'r') as f_in_zip:
                        binary_content = f_in_zip.read()
                        return target_file_info.filename, binary_content.decode(encoding)
                except UnicodeDecodeError:
                    print(f"Error: Could not decode '{target_file_info.filename}' from '{zip_file_path.name}' using '{encoding}' encoding. "
                          f"The file might be corrupted or use a different encoding.")
                    return target_file_info.filename, None
                except Exception as e:
                    print(f"Error reading file '{target_file_info.filename}' from '{zip_file_path.name}': {e}")
                    return target_file_info.filename, None
            else:
                # This case should ideally not be reached if candidate_files is not empty
                print(f"Warning: Could not determine the target text file in '{zip_file_path.name}'.")
                return None, None

    # Handle Exceptions
    except zipfile.BadZipFile:
        print(f"Error: '{zip_file_path.name}' is not a valid zip file or is corrupted.")
        return None, None
    except FileNotFoundError:
        print(f"Error: Zip file '{zip_file_path}' not found.")
        return None, None
    except Exception as e:
        print(f"An unexpected error occurred with '{zip_file_path.name}': {e}")
        return None, None

# Read Fixed-Width Files
def read_fixed_width_files(fwf_name, final_column_names: list, widths:list, has_header: bool=False, skip_rows: int=0) :

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

# Get Combined Suffix
def get_combined_suffix(files: list) :
    """Create a suffix for a combined file from a list of individual files."""

    # Get Suffixes from File Names and Create Combined Suffix from Min and Max Dates
    suffixes = [os.path.splitext(os.path.basename(file))[0].split('_')[-1] for file in files]
    suffixes = ["".join(re.findall(r'\d', suffix)) for suffix in suffixes] # Extract only numeric characters
    combined_suffix = '_'+min(suffixes)+'-'+max(suffixes)

    # Return Combined Suffix
    return combined_suffix

#%% Read Dictionaries
# Read Factor Dictionary
def read_factor_dictionary(factor_dictionary_file: str) :

    try :
        formats = pl.read_csv(factor_dictionary_file)
        formats = formats.filter(pl.col('Prefix')==file_prefix)
        column_names = formats.select(pl.col('Data Item')).to_series().to_list()
        widths = formats.select(pl.col('Length')).to_series().to_list()

        # Fix Column Names for Filler Values
        filler_count = 0
        final_column_names = []
        for column_name in column_names :
            if column_name == 'Filler' :
                column_name += f' {filler_count+1}'
                filler_count += 1
            final_column_names.append(column_name)

        # Return Columns and Widths
        return final_column_names, widths

    except Exception as e:
        print('Error reading the dictionary file with the supplied prefix:', e)
        return None, None

# Read REMIC Dictionary
def read_remic_dictionary(remic_dictionary_file: str) :

    # Read Dictionary File
    try :
        formats = pl.read_csv(remic_dictionary_file)
        formats = formats.filter(pl.col('PREFIX')==file_prefix)
        formats = formats.filter(pl.col('LINE TYPE')=='data')
        column_names = formats.select(pl.col('FIELD NAME')).to_series().to_list()
        widths = formats.select(pl.col('LENGTH')).to_series().to_list()

        # Fix Column Names for Filler Values
        filler_count = 0
        final_column_names = []
        for column_name in column_names :
            if column_name == 'Filler' :
                column_name += f' {filler_count+1}'
                filler_count += 1
            final_column_names.append(column_name)

        # Return Columns and Widths
        return final_column_names, widths

    except Exception as e:
        print('Error reading the dictionary file with the supplied prefix:', e)
        return None, None

# Read FRR Dictionary
def read_frr_dictionary(frr_dictionary_file: str) :

    try :
        formats = pl.read_csv(frr_dictionary_file)
        column_names = formats.select(pl.col('Field Name')).to_series().to_list()
        widths = formats.select(pl.col('Length')).to_series().to_list()

        # Fix Column Names for Filler Values
        filler_count = 0
        final_column_names = []
        for column_name in column_names :
            if column_name == 'Filler' :
                column_name += f' {filler_count+1}'
                filler_count += 1
            final_column_names.append(column_name)

        # Return Columns and Widths
        return final_column_names, widths

    except Exception as e:
        print('Error reading the dictionary file with the supplied prefix:', e)
        return None, None

# Read SRF Dictionary
def read_srf_dictionary(srf_dictionary_file: str) :

    try :
        formats = pl.read_csv(srf_dictionary_file)
        column_names = formats.select(pl.col('Field name')).to_series().to_list()
        widths = formats.select(pl.col('Length')).to_series().to_list()

        # Fix Column Names for Filler Values
        filler_count = 0
        final_column_names = []
        for column_name in column_names :
            if column_name == 'Filler' :
                column_name += f' {filler_count+1}'
                filler_count += 1
            final_column_names.append(column_name)

        # Return Columns and Widths
        return final_column_names, widths

    except Exception as e:
        print('Error reading the dictionary file with the supplied prefix:', e)
        return None, None

#%% Import Files
# Import Factor Files
def import_factor_files(data_folder: str, save_folder: str, file_prefix: str, dictionary_file: str) :
    """Import the full history of the factor file types."""

    # Create Save Folder if Doesn't Yet Exist
    if not os.path.exists(save_folder) :
        os.makedirs(save_folder)

    # Get All Files for the Given Prefix
    files = glob.glob(f'{data_folder}/{file_prefix}_*')

    # Load Dictionary
    column_names, widths = read_factor_dictionary(dictionary_file)

    # Convert All Files to Parquet
    for file in files :

        # Create Save File Name
        file_name = os.path.splitext(os.path.basename(file))[0]
        save_file_name = f'{save_folder}/{file_name}.parquet'

        # Save as Parquet if file doesn't exist yet
        if not os.path.exists(save_file_name) :

            # Handle ZIPs vs TXTs
            if zipfile.is_zipfile(file) :
                _,content = read_text_from_zip(file)
                df = read_fixed_width_files(StringIO(content), column_names, widths, has_header=False, skip_rows=0)
            else :
                df = read_fixed_width_files(file, column_names, widths, has_header=False, skip_rows=0)
            
            # Save to Parquet
            df.sink_parquet(save_file_name)

# Import REMIC Files
def import_remic_files(data_folder: str, save_folder: str, file_prefix: str, dictionary_file: str) :

    # Create Save Folder if Doesn't Yet Exist
    if not os.path.exists(save_folder) :
        os.makedirs(save_folder)

    # Get All Files for the Given Prefix
    files = glob.glob(f'{data_folder}/{file_prefix}_*')

    # Load Dictionary
    column_names, widths = read_remic_dictionary(dictionary_file)

    # Convert All Files to Parquet
    for file in files :

        # Create Save File Name
        file_name = os.path.splitext(os.path.basename(file))[0]
        save_file_name = f'{save_folder}/{file_name}.parquet'

        # Save as Parquet if file doesn't exist yet
        if not os.path.exists(save_file_name) :

            # Handle ZIPs vs TXTs
            if zipfile.is_zipfile(file) :
                _,content = read_text_from_zip(file)
                df = read_fixed_width_files(StringIO(content), column_names, widths, has_header=False, skip_rows=0)
            else :
                df = read_fixed_width_files(file, column_names, widths, has_header=False, skip_rows=0)

            # Save File
            df = df.filter([pl.col('Record_indicator')=='2'])
            df.sink_parquet(save_file_name)

# Import FRR Data
def import_frr_files(data_folder: str, save_folder: str, file_prefix: str, dictionary_file: str) :

    # Create Save Folder if Doesn't Yet Exist
    if not os.path.exists(save_folder) :
        os.makedirs(save_folder)

    # Get All Files for the Given Prefix
    files = glob.glob(f'{data_folder}/{file_prefix}_*')

    # Load Dictionary
    column_names, widths = read_frr_dictionary(dictionary_file)

    # Convert All Files to Parquet
    for file in files :

        # Create Save File Name
        file_name = os.path.splitext(os.path.basename(file))[0]
        save_file_name = f'{save_folder}/{file_name}.parquet'

        # Save as Parquet if file doesn't exist yet
        if not os.path.exists(save_file_name) :

            # Handle ZIPs vs TXTs
            if zipfile.is_zipfile(file) :
                _,content = read_text_from_zip(file)
                df = read_fixed_width_files(StringIO(content), column_names, widths, has_header=False, skip_rows=0)
            else :
                df = read_fixed_width_files(file, column_names, widths, has_header=False, skip_rows=0)

            # Save File
            df.sink_parquet(save_file_name)

# Import SRF Data
def import_srf_files(data_folder: str, save_folder: str, file_prefix: str, dictionary_file: str) :

    # Create Save Folder if Doesn't Yet Exist
    if not os.path.exists(save_folder) :
        os.makedirs(save_folder)

    # Get All Files for the Given Prefix
    files = glob.glob(f'{data_folder}/{file_prefix}_*')

    # Load Dictionary
    column_names, widths = read_srf_dictionary(dictionary_file)

    # Convert All Files to Parquet
    for file in files :

        # Create Save File Name
        file_name = os.path.splitext(os.path.basename(file))[0]
        save_file_name = f'{save_folder}/{file_name}.parquet'

        # Save as Parquet if file doesn't exist yet
        if not os.path.exists(save_file_name) :

            # Handle ZIPs vs TXTs
            if zipfile.is_zipfile(file) :
                _,content = read_text_from_zip(file)
                df = read_fixed_width_files(StringIO(content), column_names, widths, has_header=False, skip_rows=0)
            else :
                df = read_fixed_width_files(file, column_names, widths, has_header=False, skip_rows=0)

            # Save File
            df.sink_parquet(save_file_name)

#%% Combine Files
# Combine Files
def combine_files(data_folder: str, save_folder: str, file_prefix: str) :

    # Get All Files w/ Given Prefix
    files = glob.glob(f'{data_folder}/{file_prefix}_*.parquet')
    
    # Get Combined Suffix from File List
    combined_suffix = get_combined_suffix(files)

    # Save File Name
    save_file_name = f'{save_folder}/{file_prefix}{combined_suffix}.parquet'

    # Only Combine if No Combined File Exists
    if not os.path.exists(save_file_name) :

        # Combine Data From Parquets
        df = []
        for file in files :
            df_a = pl.scan_parquet(file)
            df.append(df_a)
        df = pl.concat(df)

        # Get Combined Suffix from File List
        combined_suffix = get_combined_suffix(files)

        # Save
        df.sink_parquet(save_file_name)

#%% Main Routine
if __name__ == "__main__":

    # Set Folder Structure
    DATA_DIR = config.DATA_DIR
    RAW_DIR = config.RAW_DIR
    CLEAN_DIR = config.CLEAN_DIR
    PROJECT_DIR = config.PROJECT_DIR

    # Import Fixed-Width Factor Files
    dictionary_file = './dictionary_files/clean/factor_layouts_combined.csv'
    for file_prefix in ['factorA1','factorA2','factorAAdd','factorAplat','factorB1','factorB2']:
        import_factor_files(data_folder=RAW_DIR, save_folder=CLEAN_DIR, file_prefix=file_prefix, dictionary_file=dictionary_file)

    # Import Fixed Width REMIC Files
    dictionary_file = './dictionary_files/clean/remic_layouts_combined.csv'
    for file_prefix in ['remic1','remic2']:
        import_remic_files(data_folder=RAW_DIR, save_folder=CLEAN_DIR, file_prefix=file_prefix, dictionary_file=dictionary_file)

    # Read FRR and SRF Files
    dictionary_file = './dictionary_files/clean/FRR_layout.csv'
    import_frr_files(data_folder=RAW_DIR, save_folder=CLEAN_DIR, file_prefix='FRR', dictionary_file=dictionary_file)

    dictionary_file = './dictionary_files/clean/SRF_layout.csv'
    import_srf_files(data_folder=RAW_DIR, save_folder=CLEAN_DIR, file_prefix='SRF', dictionary_file=dictionary_file)

    # Create Combined Files
    for file_prefix in ['factorA1','factorA2','factorAAdd','factorAplat','factorB1','factorB2','remic1','remic2','FRR','SRF']:
        combine_files(data_folder=CLEAN_DIR, save_folder=DATA_DIR, file_prefix=file_prefix)
