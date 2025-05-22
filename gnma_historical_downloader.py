# Import Packages
import requests
import datetime
import time
import os
import re
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
import yaml
import glob
import zipfile
import pandas as pd

# Get Date Format Function
def get_date_format(date_string: str) -> str:
    """
    Analyzes a date string and returns the corresponding strptime/strftime format.

    Supports common formats like:
    - YYYYMMDD ('20241025') -> '%Y%m%d'
    - YYYYMM ('202410') -> '%Y%m'
    - YYYY-MM-DD ('2024-10-25') -> '%Y-%m-%d'
    - YYYY/MM/DD ('2024/10/25') -> '%Y/%m/%d'
    - MM/DD/YYYY ('10/25/2024') -> '%m/%d/%Y'
    - MM-DD-YYYY ('10-25-2024') -> '%m-%d-%Y'
    - YYYYMMDDHHMMSS ('20241025153000') -> '%Y%m%d%H%M%S'
    # Add more patterns and formats here as needed

    Args:
        date_string: The string containing the date to analyze.

    Returns:
        The detected strptime/strftime format string.

    Raises:
        TypeError: If the input is not a string.
        ValueError: If the format of the input string is not recognized.

    Created w/ Google Gemini.
    """
    if not isinstance(date_string, str):
        raise TypeError("Input must be a string.")

    # --- Define patterns and their corresponding formats ---
    # Order matters: Check more specific patterns (like those with separators)
    # before more general ones (like all digits).
    patterns = {
        r'^\d{4}-\d{2}-\d{2}$': '%Y-%m-%d',  # YYYY-MM-DD
        r'^\d{4}/\d{2}/\d{2}$': '%Y/%m/%d',  # YYYY/MM/DD
        r'^\d{2}/\d{2}/\d{4}$': '%m/%d/%Y',  # MM/DD/YYYY
        r'^\d{2}-\d{2}-\d{4}$': '%m-%d-%Y',  # MM-DD-YYYY
        r'^\d{8}$': '%Y%m%d',              # YYYYMMDD
        r'^\d{6}$': '%Y%m',                # YYYYMM (as per original request)
        r'^\d{14}$': '%Y%m%d%H%M%S',       # YYYYMMDDHHMMSS
        # Add more complex patterns here if needed, e.g., for month names
        # r'^\d{2}-[A-Za-z]{3}-\d{4}$': '%d-%b-%Y', # DD-Mon-YYYY
    }

    for pattern, date_format in patterns.items():
        if re.match(pattern, date_string):
            # Optional: Add a try-except block here to validate the date value itself
            # try:
            #     datetime.datetime.strptime(date_string, date_format)
            #     return date_format
            # except ValueError:
            #     # Pattern matches, but the date value is invalid (e.g., month 13)
            #     # Continue checking other patterns or raise a specific error
            #     pass
            return date_format

    # If no pattern matched
    raise ValueError(f"Could not determine date format for input: '{date_string}'")

# Check Zip File Validity Function
def drop_invalid_zip_files(folder_path: str) :
    """
    Checks if files with a .zip extension in a given folder are valid zip files.

    Args:
        folder_path: The path to the folder containing the potential zip files.

    Returns:
        A dictionary where keys are the full paths of the .zip files found
        and values are booleans indicating whether the file is a valid zip (True)
        or not (False).
    """

    # Check that folder exists
    if not os.path.isdir(folder_path):
        print(f"Error: Folder not found or is not a directory: {folder_path}")
        return {}

    # Get all candidate zip files
    zip_files = glob.glob(os.path.join(folder_path, '*.zip'))

    if not zip_files:
        print(f"No files with a .zip extension found in {folder_path}")
        return {}

    print(f"Checking validity of {len(zip_files)} .zip files in {folder_path}...")

    # Drop Invalid Zip Files
    for file_path in zip_files:
        is_valid = zipfile.is_zipfile(file_path)
        if not is_valid :
            os.remove(file_path)
            print('Removing file:', file_path)

# Create Date Suffix
def create_date_suffix(current_date: datetime.datetime, date_format: str, frequency: str, firstlast: str='last') -> str:

    # Turn Current Date to Period
    if frequency == 'monthly' :
        date_period = pd.Series(current_date).dt.to_period('M')
    elif frequency == 'quarterly' :
        date_period = pd.Series(current_date).dt.to_period('Q')
    elif frequency == 'yearly' :
        date_period = pd.Series(current_date).dt.to_period('Y')

    # Get First of
    if firstlast == 'first' :
        key_date = date_period.dt.start_time[0]
    else :
        key_date = date_period.dt.end_time[0]

    # Get Date Suffix
    date_suffix = key_date.strftime(date_format)

    # Return Date Suffix
    return date_suffix

## Main Routine
if __name__=='__main__' :

    # Load Environment Variables
    load_dotenv()

    ## Set Up Cookie
    # Cookie Configuration
    cookie_name = "GMProfileInfo"
    email_value = os.getenv('email_value')  # Replace with the actual email
    id_value = os.getenv('id_value')       # Replace with the actual ID
    cookie_domain = "ginniemae.gov"
    cookie_path = "/"
    cookie_value = f"e={email_value}&i={id_value}"

    # Calculate Expiration - One Year From Now (Timestamp Format)
    expiration_datetime = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=365)
    expiration_timestamp = expiration_datetime.timestamp()

    # Create Cookie Jar and Set Cookie
    cookie_jar = requests.cookies.RequestsCookieJar()
    cookie_jar.set(
        name=cookie_name,
        value=cookie_value,
        domain=cookie_domain,
        path=cookie_path,
        expires=expiration_timestamp,
        secure=True,
    )

    ## Set Up Session
    # Create a session and attach the cookies
    session = requests.Session()
    session.cookies = cookie_jar

    # Set Headers
    headers = {"user-agent": os.getenv('user_agent')}

    ## Set Up Download Parameters
    # Set URL Prefix and Download Folder
    historical_url_prefix = 'https://bulk.ginniemae.gov/protectedfiledownload.aspx?dlfile=data_history_cons'
    download_folder = os.getenv('download_folder')

    # Load YAML file as dictionary
    with open("dictionary_files/prefix_dictionary.yaml", "r") as f:
        prefix_dict = yaml.safe_load(f)

    ## Download Data
    # Loop through Prefixes and Year-Month Ranges
    for prefix in prefix_dict.keys() :

        # Get Start and End Dates w/ Format Detection
        detected_format = get_date_format(prefix_dict[prefix]['min_date'])
        start_date = datetime.datetime.strptime(prefix_dict[prefix]['min_date'], detected_format)
        end_date = datetime.datetime.strptime(prefix_dict[prefix]['max_date'], detected_format) if prefix_dict[prefix]['max_date'] else datetime.datetime.now()

        # Loop through each month in the date range
        current_date = start_date
        while current_date <= end_date :

            # Get File Suffix
            date_format = prefix_dict[prefix]['date_format']
            frequency = prefix_dict[prefix]['frequency']
            file_date_suffix = create_date_suffix(current_date, date_format, frequency, firstlast='last')

            # Create File Name and URL
            file_name = f'{prefix}_{file_date_suffix}.{prefix_dict[prefix]["extension"]}'
            file_url = f'{historical_url_prefix}/{file_name}'
            download_file = f'{download_folder}/{file_name}'

            # Check if the file already exists, if not, download it
            if not os.path.exists(download_file) :
                r = session.request('GET', file_url, headers=headers)
                with open(download_file, 'wb') as f:
                    f.write(r.content)

                # Sleep for a few seconds to avoid overwhelming the server
                time.sleep(2)
            
            # Add one month to the current date
            current_date += relativedelta(months=1)

    # Drop Invalid Zip Files (If any)
    drop_invalid_zip_files(download_folder)
