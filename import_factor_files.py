# Import Packages
import zipfile
import pathlib
import glob
import polars as pl

#%% Support Functions
# Read Text from Zips
def read_text_from_zip(zip_file_path, encoding='utf-8'):
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

# Process Zip Files in Folder
def process_zip_files_in_folder(folder_path_str, save_folder_path_str, file_prefix='', file_encoding='utf-8'):
    """
    Processes all zip files in a given folder. For each zip file,
    it reads the content of the identified text file within.

    Args:
        folder_path_str (str): The path to the folder containing zip files.
        file_encoding (str): The encoding of the text files within the zips.
                             Defaults to 'utf-8'.

    Returns:
        dict: A dictionary where keys are strings like "zip_filename/text_filename_in_zip"
              and values are the content of the text files.
              Entries with content=None indicate a read/decode failure for that file.
    """

    folder_path = pathlib.Path(folder_path_str)

    zip_files = glob.glob(f'{folder_path}/{file_prefix}*.zip')
    print(zip_files)

    if not folder_path.is_dir():
        print(f"Error: Folder '{folder_path_str}' not found or is not a directory.")
        return {}

    print(f"Scanning for zip files in folder: {folder_path}\n")

    # for item_path in folder_path.iterdir():
    for item_path in zip_files:
        item_path = pathlib.Path(item_path)
        if item_path.is_file() and item_path.suffix.lower() == '.zip':
            print(f"--- Processing Zip File: {item_path.name} ---")
            
            filename_in_zip, content = read_text_from_zip(item_path, encoding=file_encoding)

# Read Fixed-Width Files
def read_fixed_width_files(fwf_name, dictionary_file, file_prefix, has_header=False, skip_rows=0) :

    # Load the Factor Data
    df = pl.read_csv(
        fwf_name,
        has_header=has_header,
        skip_rows=skip_rows,
        new_columns=["full_str"]
    )

    # Read Dictionary File
    try :
        formats = pl.read_csv(dictionary_file)
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

    except Exception as e:
        print('Error reading the dictionary file with the supplied prefix:', e)
        return None

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
def convert_fixed_width_files(fwf_name, dictionary_file, file_prefix, has_header=False, skip_rows=0) :

    # Load the Factor Data
    df = pl.read_csv(
        fwf_name,
        has_header=has_header,
        skip_rows=skip_rows,
        new_columns=["full_str"]
    )

    # Read Dictionary File
    try :
        formats = pl.read_csv(dictionary_file)
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

    except Exception as e:
        print('Error reading the dictionary file with the supplied prefix:', e)
        return None

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

    # Save to Parquet
    df.write_parquet(fwf_name.replace('.txt', '.parquet'))

#%% Import and Combine Factor Files
# Import Factor Files
def import_factor_files() :

    pass

# Combine Factor Files
def combine_factor_files() :

    pass

#%% Import and Combine Remic Files
# Import Remic Files
def import_remic_files() :

    pass

# Combine Remic Files
def combine_remic_files() :

    pass

#%% Import and Combine FRR/SRF Datasets
# Import FRR Data
def import_frr_data() :

    pass

# Import SRF Data
def import_srf_data() :

    pass

# Combine FRR Data
def combine_frr_data() :

    pass

# Combine SRF Data
def combine_srf_data() :

    pass

#%% Main Routine
if __name__ == "__main__":

    # Process Zip Files
    folder_to_scan = "./data/raw"
    folder_to_save = './data/clean'
    text_file_encoding = 'utf-8'
    # process_zip_files_in_folder(folder_to_scan, folder_to_save, file_prefix='factorA1_', file_encoding=text_file_encoding)

    # Read Fixed-Width Files
    fwf_name = './data/raw/factorAplat_202501.txt'
    dictionary_file = './dictionary_files/clean/factor_layouts_combined.csv'
    df = read_fixed_width_files(fwf_name, dictionary_file=dictionary_file, file_prefix='factorAplat')
    print(df)

# # OLD
# # Import Packages
# import polars as pl

# # Set File Name
# fwf_name = './data/raw/factorAplat_202001.txt'

# # Load the Factor Data
# df = pl.read_csv(
#     fwf_name,
#     has_header=False,
#     skip_rows=1,
#     new_columns=["full_str"]
# )

# # Read Dictionary File
# dictionary_file = './dictionary_files/tabula-factorAplat_layout.csv'
# formats = pl.read_csv(dictionary_file)
# column_names = formats.select(pl.col('Data Item')).to_series().to_list()
# widths = (formats.select(pl.col('End')) - formats.select(pl.col('Begin')) + 1).to_series().to_list()

# # Fix Column Names
# filler_count = 0
# final_column_names = []
# for column_name in column_names :
#     if column_name == 'Filler' :
#         column_name += f' {filler_count+1}'
#         filler_count += 1
#     final_column_names.append(column_name)

# # Calculate slice values from widths.
# slice_tuples = []
# offset = 0
# for i in widths:
#     slice_tuples.append((offset, i))
#     offset += i

# # Create Final DataFrame (and drop full string)
# df = df.with_columns(
#     [
#        pl.col("full_str").str.slice(slice_tuple[0], slice_tuple[1]).str.strip_chars().alias(col)
#        for slice_tuple, col in zip(slice_tuples, final_column_names)
#     ]
# ).drop("full_str")
