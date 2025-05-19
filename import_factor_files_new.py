# Import Packages
import zipfile
import pathlib
import os # Used in the example main block

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
            else:
                # Multiple files found. Try to find a specific text file.
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
def process_zip_files_in_folder(folder_path_str, file_prefix='', file_encoding='utf-8'):
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

    import glob
    zip_files = glob.glob(f'{folder_path}/{file_prefix}*.zip')
    print(zip_files)

    if not folder_path.is_dir():
        print(f"Error: Folder '{folder_path_str}' not found or is not a directory.")
        return {}

    print(f"Scanning for zip files in folder: {folder_path}\n")
    found_zip_files = False
    processed_data = {}

    # for item_path in folder_path.iterdir():
    for item_path in zip_files:
        item_path = pathlib.Path(item_path)
        if item_path.is_file() and item_path.suffix.lower() == '.zip':
            found_zip_files = True
            print(f"--- Processing Zip File: {item_path.name} ---")
            
            filename_in_zip, content = read_text_from_zip(item_path, encoding=file_encoding)
            print(content)
            stop
            
            if filename_in_zip: # A file was identified, even if content is None (due to read/decode error)
                full_key = f"{item_path.name}/{filename_in_zip}"
                processed_data[full_key] = content
                if content:
                    print(f"Successfully read '{filename_in_zip}'.")
                    # Displaying a preview of the content (e.g., first 5 lines)
                    print(f"Preview of '{filename_in_zip}' (first 5 lines or less):")
                    lines = content.splitlines()
                    for i, line in enumerate(lines[:5]):
                        print(line)
                    if len(lines) > 5:
                        print("...")
                # If content is None, an error message was already printed by read_text_from_zip
            else: # No file was identified within the zip by read_text_from_zip
                 print(f"Could not identify a text file to read from '{item_path.name}'.")

            print(f"--- Finished processing {item_path.name} ---\n")
    
    if not found_zip_files:
        print(f"No zip files found in '{folder_path_str}'.")
    
    return processed_data

## Main Routine
if __name__ == "__main__":

    # Configuration
    folder_to_scan = "./data/raw"
    text_file_encoding = 'utf-8'

    #
    all_extracted_data = process_zip_files_in_folder(folder_to_scan, file_prefix='factorA1_', file_encoding=text_file_encoding)

    if all_extracted_data:
        print("\n--- Summary of Extracted Data ---")
        for key, content_data in all_extracted_data.items():
            if content_data is not None:
                print(f"Data successfully read for: {key} ({len(content_data)} characters)")
            else:
                print(f"Failed to read/decode content for: {key}")
        print("--- End of Summary ---")
    else:
        print("No data was processed.")
