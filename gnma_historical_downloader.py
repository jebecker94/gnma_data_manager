# Import Packages
import requests
import datetime
import time
import os
import re
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
import yaml

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

            # Create File Name and URL
            yearmonth = f'{current_date.year}{str(current_date.month).zfill(2)}'
            file_name = f'{prefix}_{yearmonth}.{prefix_dict[prefix]["extension"]}'
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
