# Import Packages
import os
import requests
from bs4 import BeautifulSoup
import re
from dotenv import load_dotenv
import datetime
import time
import yaml

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

    # Bad Texts to avoid downloading
    bad_texts = [
        'Supplemental Loan Level Forbearance File',
    ]

    # Create a directory to save PDFs
    download_folder = './dictionary_files/pdf_layouts'
    os.makedirs(download_folder, exist_ok=True)

    # Load YAML file as dictionary
    with open("dictionary_files/prefix_dictionary.yaml", "r") as f:
        prefix_dict = yaml.safe_load(f)

    ## Download Data
    # Loop through Prefixes and Year-Month Ranges
    for prefix in prefix_dict.keys() :

        # URL of the page to scrape for PDFs
        url = f'https://www.ginniemae.gov/data_and_reports/disclosure_data/pages/disclosurehistoryfiles.aspx?prefix={prefix}'

        # Send a GET request to the URL
        response = session.request('GET', url, headers=headers)
        response.raise_for_status()  # Raise an error for bad status codes

        # Parse the HTML content
        soup = BeautifulSoup(response.text, 'html.parser')

        # Find all anchor tags with href ending in '.pdf'
        pdf_links = soup.find_all('a', href=lambda href: href and href.lower().endswith('.pdf'))

        # Download each PDF
        for link in pdf_links:

            # Get the reference
            href = link['href']
            link_text = link.get_text(strip=True)

            # Check if Link Text is in Prohibited String List and skip
            if any(bad_text in link_text for bad_text in bad_texts):
                continue

            # Sanitize the link text to create a valid filename
            filename = prefix + '_' + re.sub(r'[\\/*?:"<>|]', "_", link_text) + '.pdf'
            file_path = os.path.join(download_folder, filename)

            # Check if the file already exists
            if not os.path.exists(file_path):

                # Construct the full URL if the href is relative
                if not href.startswith('http'):
                    href = 'https://www.ginniemae.gov' + href

                # Download the PDF
                try :

                    # Get PDF Content
                    pdf_response = session.request('GET', href, headers=headers)
                    pdf_response.raise_for_status()

                    # Save the PDF to the specified directory
                    with open(file_path, 'wb') as f:
                        f.write(pdf_response.content)

                    # Display Progress
                    print(f'Downloaded: {filename}')

                    # Courtesy Pause
                    time.sleep(2)

                # Skip errors but display that an error has occured
                except requests.HTTPError :
                    print(f'Failed to download file: {filename}')
