# gnma_data_manager
A tool to make it easier to use the publicly available Ginnie Mae (GNMA) data.

## Functionality
This program takes raw data from Ginnie Mae's public disclosure data files, cleans it, and combines it into a large research-ready dataset.

## How To Use
1. Download data from the GNMA Public Disclosure Data and History pages:
    - Set up .env File:
    - Historical Files: Use 'gnma_historical_downloader.py' to download the complete history for various series. If you want to download a subset of series, comment out the prefixes of the files you do not want in 'dictionary_files/prefix_dictionary.yaml'
    - Current Files: Forthcoming
2. Use the import scripts to load and clean the raw files.
3. Use the cleaned files in your personal research projects.
