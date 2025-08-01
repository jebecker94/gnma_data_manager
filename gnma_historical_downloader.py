"""
GNMA Data Downloader - Comprehensive Object-Oriented Version

This module provides a class-based approach to downloading both historical data files
and schema/layout PDF files from Ginnie Mae's website with improved error handling, 
logging, and configurability.

Key Features:
- Download historical data files (ZIP format) from protected file download endpoints
- Download schema/layout files (PDF format) from disclosure history pages
- Automatic organization of files into prefix-specific subfolders
- Robust error handling and logging
- Flexible configuration via DownloadConfig dataclass
- Support for selective downloads by prefix or date range
- Automatic validation and cleanup of invalid zip files
- Content filtering for unwanted files

File Organization:
By default, files are organized as:
- Data files: data/raw/prefix/filename.zip
- Schema files: dictionary_files/raw/prefix/filename.pdf
This can be customized via configuration options.

Usage:
- create_all_prefix_folders(): Create complete folder structure
- download_all_data(): Download historical data files
- download_all_schemas(): Download schema/PDF files  
- download_everything(): Download both data and schemas
"""

import requests
import datetime
import time
import os
import re
import logging
from pathlib import Path
from typing import Dict, Optional, Tuple, List
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
import yaml
import zipfile
import pandas as pd
from dataclasses import dataclass
from bs4 import BeautifulSoup


@dataclass
class DownloadConfig:
    """Configuration class for the GNMA downloader."""
    email_value: str
    id_value: str
    user_agent: str
    download_folder: str  # For data files (e.g., "data")
    schema_download_folder: str = "dictionary_files"  # For schema/PDF files
    dictionary_file: str = "dictionary_files/prefix_dictionary.yaml"
    base_url: str = "https://bulk.ginniemae.gov/protectedfiledownload.aspx?dlfile=data_history_cons"
    cookie_name: str = "GMProfileInfo"
    cookie_domain: str = "ginniemae.gov"
    cookie_path: str = "/"
    request_delay: float = 2.0
    cookie_expiry_days: int = 365
    use_prefix_subfolders: bool = True
    raw_folder_name: str = "raw"
    # Schema/PDF download settings
    schema_base_url: str = "https://www.ginniemae.gov/data_and_reports/disclosure_data/pages/disclosurehistoryfiles.aspx"
    schema_folder_name: str = "raw"  # Within dictionary_files, use "raw" subfolder
    bad_text_filters: List[str] = None


class DateFormatError(Exception):
    """Custom exception for date format detection errors."""
    pass


class DownloadError(Exception):
    """Custom exception for download errors."""
    pass


class GNMAHistoricalDownloader:
    """
    A class for downloading historical data files from Ginnie Mae's website.
    
    This class handles authentication, file discovery, and systematic downloading
    of historical data files based on configuration parameters.
    """
    
    def __init__(self, config: DownloadConfig):
        """
        Initialize the downloader with configuration.
        
        Args:
            config: DownloadConfig object containing all necessary parameters
        """
        self.config = config
        self.session = None
        self.prefix_dict = {}
        
        # Set default bad text filters if none provided
        if self.config.bad_text_filters is None:
            self.config.bad_text_filters = [
                'Supplemental Loan Level Forbearance File',
            ]
        
        # Set up logging
        self._setup_logging()
        
        # Load prefix dictionary
        self._load_prefix_dictionary()
        
        # Set up session
        self._setup_session()
    
    def _setup_logging(self) -> None:
        """Set up logging configuration."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('gnma_downloader.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def _load_prefix_dictionary(self) -> None:
        """Load the prefix dictionary from YAML file."""
        try:
            with open(self.config.dictionary_file, "r") as f:
                self.prefix_dict = yaml.safe_load(f)
            self.logger.info(f"Loaded {len(self.prefix_dict)} prefixes from {self.config.dictionary_file}")
        except FileNotFoundError:
            raise FileNotFoundError(f"Dictionary file not found: {self.config.dictionary_file}")
        except yaml.YAMLError as e:
            raise ValueError(f"Error parsing YAML file: {e}")
    
    def _setup_session(self) -> None:
        """Set up the requests session with authentication cookies."""
        # Create cookie value
        cookie_value = f"e={self.config.email_value}&i={self.config.id_value}"
        
        # Calculate expiration
        expiration_datetime = (
            datetime.datetime.now(datetime.timezone.utc) + 
            datetime.timedelta(days=self.config.cookie_expiry_days)
        )
        expiration_timestamp = expiration_datetime.timestamp()
        
        # Create cookie jar
        cookie_jar = requests.cookies.RequestsCookieJar()
        cookie_jar.set(
            name=self.config.cookie_name,
            value=cookie_value,
            domain=self.config.cookie_domain,
            path=self.config.cookie_path,
            expires=expiration_timestamp,
            secure=True,
        )
        
        # Create session
        self.session = requests.Session()
        self.session.cookies = cookie_jar
        self.session.headers.update({"user-agent": self.config.user_agent})
        
        self.logger.info("Session configured with authentication cookies")
    
    def _get_prefix_folder(self, prefix: str) -> Path:
        """
        Get the appropriate download folder for a given prefix.
        
        Args:
            prefix: The data prefix
            
        Returns:
            Path object for the prefix-specific download folder
        """
        base_path = Path(self.config.download_folder)
        
        if self.config.use_prefix_subfolders:
            # Create path: download_folder/raw/prefix
            prefix_path = base_path / self.config.raw_folder_name / prefix
        else:
            # Use the base download folder
            prefix_path = base_path
        
        # Create the directory if it doesn't exist
        prefix_path.mkdir(parents=True, exist_ok=True)
        
        return prefix_path
    
    def _get_schema_folder(self, prefix: str) -> Path:
        """
        Get the appropriate schema download folder for a given prefix.
        
        Args:
            prefix: The data prefix
            
        Returns:
            Path object for the prefix-specific schema folder
        """
        base_path = Path(self.config.schema_download_folder)
        
        if self.config.use_prefix_subfolders:
            # Create path: schema_download_folder/raw/prefix
            schema_path = base_path / self.config.schema_folder_name / prefix
        else:
            # Use the schema download folder with schema subfolder
            schema_path = base_path / self.config.schema_folder_name
        
        # Create the directory if it doesn't exist
        schema_path.mkdir(parents=True, exist_ok=True)
        
        return schema_path
    
    def get_all_prefix_folders(self) -> Dict[str, Path]:
        """
        Get all prefix folder paths that would be created for the current configuration.
        
        Returns:
            Dictionary mapping prefix names to their folder paths
        """
        prefix_folders = {}
        for prefix in self.prefix_dict.keys():
            prefix_folders[prefix] = self._get_prefix_folder(prefix)
        return prefix_folders
    
    @staticmethod
    def get_date_format(date_string: str) -> str:
        """
        Analyzes a date string and returns the corresponding strptime/strftime format.
        
        Args:
            date_string: The string containing the date to analyze
            
        Returns:
            The detected strptime/strftime format string
            
        Raises:
            DateFormatError: If the format cannot be determined
        """
        if not isinstance(date_string, str):
            raise TypeError("Input must be a string.")
        
        patterns = {
            r'^\d{4}-\d{2}-\d{2}$': '%Y-%m-%d',  # YYYY-MM-DD
            r'^\d{4}/\d{2}/\d{2}$': '%Y/%m/%d',  # YYYY/MM/DD
            r'^\d{2}/\d{2}/\d{4}$': '%m/%d/%Y',  # MM/DD/YYYY
            r'^\d{2}-\d{2}-\d{4}$': '%m-%d-%Y',  # MM-DD-YYYY
            r'^\d{8}$': '%Y%m%d',              # YYYYMMDD
            r'^\d{6}$': '%Y%m',                # YYYYMM
            r'^\d{14}$': '%Y%m%d%H%M%S',       # YYYYMMDDHHMMSS
        }
        
        for pattern, date_format in patterns.items():
            if re.match(pattern, date_string):
                # Validate the date
                try:
                    datetime.datetime.strptime(date_string, date_format)
                    return date_format
                except ValueError:
                    continue
        
        raise DateFormatError(f"Could not determine date format for input: '{date_string}'")
    
    @staticmethod
    def create_date_suffix(
        current_date: datetime.datetime,
        date_format: str,
        frequency: str,
        firstlast: str = 'last',
    ) -> str:
        """
        Create a date suffix based on the current date, format, frequency, and period.
        
        Args:
            current_date: The reference date
            date_format: The desired output format
            frequency: 'monthly', 'quarterly', or 'yearly'
            firstlast: 'first' or 'last' day of the period
            
        Returns:
            Formatted date string
        """
        # Convert to pandas period
        period_map = {
            'monthly': 'M',
            'quarterly': 'Q', 
            'yearly': 'Y'
        }
        
        if frequency not in period_map:
            raise ValueError(f"Unsupported frequency: {frequency}")
        
        date_period = pd.Series(current_date).dt.to_period(period_map[frequency])
        
        # Get appropriate date
        if firstlast == 'first':
            key_date = date_period.dt.start_time[0]
        else:
            key_date = date_period.dt.end_time[0]
        
        return key_date.strftime(date_format)
    
    def validate_zip_files(self, folder_path: Optional[str] = None, prefix: Optional[str] = None) -> Dict[str, bool]:
        """
        Check and remove invalid zip files from the download folder or prefix subfolder.
        
        Args:
            folder_path: Path to check (defaults to download folder or prefix subfolder)
            prefix: Specific prefix to check (if using prefix subfolders)
            
        Returns:
            Dictionary mapping file paths to validity status
        """
        if folder_path is None:
            if prefix:
                # Check specific prefix folder
                folder_path = self._get_prefix_folder(prefix)
            elif self.config.use_prefix_subfolders:
                # Check all prefix folders
                base_path = Path(self.config.download_folder) / self.config.raw_folder_name
                if not base_path.exists():
                    self.logger.warning(f"Raw folder not found: {base_path}")
                    return {}
                
                # Recursively find all zip files in all prefix subfolders
                results = {}
                for prefix_folder in base_path.iterdir():
                    if prefix_folder.is_dir():
                        prefix_results = self.validate_zip_files(str(prefix_folder))
                        results.update(prefix_results)
                return results
            else:
                # Use base download folder
                folder_path = Path(self.config.download_folder)
        else:
            folder_path = Path(folder_path)
        
        if not folder_path.exists():
            self.logger.warning(f"Folder not found: {folder_path}")
            return {}
        
        zip_files = list(folder_path.glob('*.zip'))
        
        if not zip_files:
            self.logger.debug(f"No zip files found in {folder_path}")
            return {}
        
        self.logger.info(f"Checking validity of {len(zip_files)} zip files in {folder_path}...")
        
        results = {}
        removed_count = 0
        
        for file_path in zip_files:
            is_valid = zipfile.is_zipfile(file_path)
            results[str(file_path)] = is_valid
            
            if not is_valid:
                try:
                    file_path.unlink()
                    self.logger.warning(f"Removed invalid zip file: {file_path}")
                    removed_count += 1
                except OSError as e:
                    self.logger.error(f"Failed to remove invalid zip file {file_path}: {e}")
        
        if removed_count > 0:
            self.logger.info(f"Removed {removed_count} invalid zip files from {folder_path}")
        
        return results
    
    def download_file(self, url: str, local_path: str, skip_existing: bool = True) -> bool:
        """
        Download a single file.
        
        Args:
            url: URL to download from
            local_path: Local path to save the file
            skip_existing: Skip download if file already exists
            
        Returns:
            True if file was downloaded, False if skipped or failed
        """
        local_path = Path(local_path)
        
        # Check if file exists
        if skip_existing and local_path.exists():
            self.logger.debug(f"File already exists, skipping: {local_path}")
            return False
        
        # Create directory if needed
        local_path.parent.mkdir(parents=True, exist_ok=True)
        
        try:
            self.logger.info(f"Downloading: {url}")
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            # Write file
            with open(local_path, 'wb') as f:
                f.write(response.content)
            
            self.logger.info(f"Downloaded: {local_path} ({len(response.content)} bytes)")
            
            # Add delay
            time.sleep(self.config.request_delay)
            
            return True
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to download {url}: {e}")
            # Clean up partial download
            if local_path.exists():
                local_path.unlink()
            return False
    
    def download_prefix_data(self, prefix: str, start_date: Optional[datetime.datetime] = None, 
                           end_date: Optional[datetime.datetime] = None) -> Tuple[int, int]:
        """
        Download all files for a specific prefix.
        
        Args:
            prefix: The data prefix to download
            start_date: Override start date (optional)
            end_date: Override end date (optional)
            
        Returns:
            Tuple of (successful_downloads, total_attempts)
        """
        if prefix not in self.prefix_dict:
            raise ValueError(f"Unknown prefix: {prefix}")
        
        prefix_config = self.prefix_dict[prefix]
        
        # Determine date range
        if start_date is None:
            detected_format = self.get_date_format(prefix_config['min_date'])
            start_date = datetime.datetime.strptime(prefix_config['min_date'], detected_format)
        
        if end_date is None:
            if prefix_config['max_date']:
                detected_format = self.get_date_format(prefix_config['max_date'])
                end_date = datetime.datetime.strptime(prefix_config['max_date'], detected_format)
            else:
                end_date = datetime.datetime.now()
        
        self.logger.info(f"Downloading {prefix} data from {start_date.date()} to {end_date.date()}")
        
        successful_downloads = 0
        total_attempts = 0
        current_date = start_date
        
        while current_date <= end_date:
            # Create file name
            date_suffix = self.create_date_suffix(
                current_date,
                prefix_config['date_format'],
                prefix_config['frequency'],
                firstlast='last'
            )
            
            file_name = f"{prefix}_{date_suffix}.{prefix_config['extension']}"
            file_url = f"{self.config.base_url}/{file_name}"
            prefix_folder = self._get_prefix_folder(prefix)
            local_path = prefix_folder / file_name
            
            # Download file
            total_attempts += 1
            if self.download_file(file_url, str(local_path)):
                successful_downloads += 1
            
            # Move to next period
            current_date += relativedelta(months=1)
        
        self.logger.info(f"Completed {prefix}: {successful_downloads}/{total_attempts} files downloaded")
        return successful_downloads, total_attempts
    
    def download_all_data(self, prefixes: Optional[List[str]] = None) -> Dict[str, Tuple[int, int]]:
        """
        Download data for all or specified prefixes.
        
        Args:
            prefixes: List of prefixes to download (None for all)
            
        Returns:
            Dictionary mapping prefix to (successful_downloads, total_attempts)
        """
        if prefixes is None:
            prefixes = list(self.prefix_dict.keys())
        
        results = {}
        total_successful = 0
        total_attempts = 0
        
        self.logger.info(f"Starting download for {len(prefixes)} prefixes")
        
        for prefix in prefixes:
            try:
                successful, attempts = self.download_prefix_data(prefix)
                results[prefix] = (successful, attempts)
                total_successful += successful
                total_attempts += attempts
            except Exception as e:
                self.logger.error(f"Error downloading {prefix}: {e}")
                results[prefix] = (0, 0)
        
        self.logger.info(f"Download complete: {total_successful}/{total_attempts} files downloaded")
        
        # Clean up invalid zip files
        self.validate_zip_files()
        
        return results
    
    def download_schema_file(self, url: str, local_path: str, skip_existing: bool = True) -> bool:
        """
        Download a single schema/PDF file.
        
        Args:
            url: URL to download from
            local_path: Local path to save the file
            skip_existing: Skip download if file already exists
            
        Returns:
            True if file was downloaded, False if skipped or failed
        """
        local_path = Path(local_path)
        
        # Check if file exists
        if skip_existing and local_path.exists():
            self.logger.debug(f"Schema file already exists, skipping: {local_path}")
            return False
        
        # Create directory if needed
        local_path.parent.mkdir(parents=True, exist_ok=True)
        
        try:
            self.logger.info(f"Downloading schema: {url}")
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            # Write file
            with open(local_path, 'wb') as f:
                f.write(response.content)
            
            self.logger.info(f"Downloaded schema: {local_path} ({len(response.content)} bytes)")
            
            # Add delay
            time.sleep(self.config.request_delay)
            
            return True
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to download schema {url}: {e}")
            # Clean up partial download
            if local_path.exists():
                local_path.unlink()
            return False
    
    def download_prefix_schemas(self, prefix: str) -> Tuple[int, int]:
        """
        Download all schema/PDF files for a specific prefix.
        
        Args:
            prefix: The data prefix to download schemas for
            
        Returns:
            Tuple of (successful_downloads, total_attempts)
        """
        if prefix not in self.prefix_dict:
            raise ValueError(f"Unknown prefix: {prefix}")
        
        self.logger.info(f"Downloading schemas for prefix: {prefix}")
        
        # Get the schema folder for this prefix
        schema_folder = self._get_schema_folder(prefix)
        
        # Construct the URL for this prefix
        url = f"{self.config.schema_base_url}?prefix={prefix}"
        
        try:
            # Send GET request to the schema page
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            # Parse the HTML content
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Find all anchor tags with href ending in '.pdf'
            pdf_links = soup.find_all('a', href=lambda href: href and href.lower().endswith('.pdf'))
            
            successful_downloads = 0
            total_attempts = 0
            
            # Download each PDF
            for link in pdf_links:
                # Get the reference and link text
                href = link['href']
                link_text = link.get_text(strip=True)
                
                # Check if link text contains any bad text filters
                if any(bad_text in link_text for bad_text in self.config.bad_text_filters):
                    self.logger.debug(f"Skipping filtered file: {link_text}")
                    continue
                
                # Sanitize the link text to create a valid filename
                sanitized_text = re.sub(r'[\\/*?:"<>|]', "_", link_text)
                filename = f"{prefix}_{sanitized_text}.pdf"
                file_path = schema_folder / filename
                
                # Construct the full URL if the href is relative
                if not href.startswith('http'):
                    href = 'https://www.ginniemae.gov' + href
                
                # Download the PDF
                total_attempts += 1
                if self.download_schema_file(href, str(file_path)):
                    successful_downloads += 1
            
            self.logger.info(f"Completed schema download for {prefix}: {successful_downloads}/{total_attempts} files downloaded")
            return successful_downloads, total_attempts
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to access schema page for {prefix}: {e}")
            return 0, 0
    
    def download_all_schemas(self, prefixes: Optional[List[str]] = None) -> Dict[str, Tuple[int, int]]:
        """
        Download schema/PDF files for all or specified prefixes.
        
        Args:
            prefixes: List of prefixes to download schemas for (None for all)
            
        Returns:
            Dictionary mapping prefix to (successful_downloads, total_attempts)
        """
        if prefixes is None:
            prefixes = list(self.prefix_dict.keys())
        
        results = {}
        total_successful = 0
        total_attempts = 0
        
        self.logger.info(f"Starting schema download for {len(prefixes)} prefixes")
        
        for prefix in prefixes:
            try:
                successful, attempts = self.download_prefix_schemas(prefix)
                results[prefix] = (successful, attempts)
                total_successful += successful
                total_attempts += attempts
            except Exception as e:
                self.logger.error(f"Error downloading schemas for {prefix}: {e}")
                results[prefix] = (0, 0)
        
        self.logger.info(f"Schema download complete: {total_successful}/{total_attempts} files downloaded")
        return results
    
    def download_everything(self, prefixes: Optional[List[str]] = None) -> Dict[str, Dict[str, Tuple[int, int]]]:
        """
        Download both historical data and schemas for specified prefixes.
        
        Args:
            prefixes: List of prefixes to download for (None for all)
            
        Returns:
            Dictionary with 'data' and 'schemas' keys, each containing download results
        """
        self.logger.info("Starting complete download (data + schemas)")
        
        results = {
            'data': self.download_all_data(prefixes),
            'schemas': self.download_all_schemas(prefixes)
        }
        
        # Summary
        data_total = sum(successful for successful, _ in results['data'].values())
        schema_total = sum(successful for successful, _ in results['schemas'].values())
        
        self.logger.info(f"Complete download finished: {data_total} data files, {schema_total} schema files")
        
        return results
    
    def create_all_prefix_folders(self, include_clean: bool = True) -> Dict[str, List[str]]:
        """
        Create all prefix folders upfront for organization (like create_prefix_folders.py).
        
        Args:
            include_clean: Whether to create 'clean' folders in addition to 'raw' folders
            
        Returns:
            Dictionary with 'created' and 'existing' folder lists
        """
        created_folders = []
        existing_folders = []
        
        # Define base directories
        base_dirs = [
            Path(self.config.download_folder) / self.config.raw_folder_name,  # data/raw
            Path(self.config.schema_download_folder) / self.config.schema_folder_name,  # dictionary_files/raw
        ]
        
        if include_clean:
            base_dirs.extend([
                Path(self.config.download_folder) / "clean",  # data/clean
                Path(self.config.schema_download_folder) / "clean",  # dictionary_files/clean
            ])
        
        # Create base directories first
        for base_dir in base_dirs:
            base_dir.mkdir(parents=True, exist_ok=True)
            self.logger.info(f"Ensured base directory exists: {base_dir}")
        
        # Create folders for each prefix
        prefixes = [key for key in self.prefix_dict.keys() if not key.startswith('#')]
        
        for base_dir in base_dirs:
            for prefix in prefixes:
                folder_path = base_dir / prefix
                
                if folder_path.exists():
                    existing_folders.append(str(folder_path))
                else:
                    folder_path.mkdir(parents=True, exist_ok=True)
                    created_folders.append(str(folder_path))
        
        # Log results
        self.logger.info(f"Created {len(created_folders)} new folders for {len(prefixes)} prefixes")
        self.logger.info(f"Skipped {len(existing_folders)} existing folders")
        
        return {
            'created': created_folders,
            'existing': existing_folders,
            'prefixes': prefixes,
            'base_dirs': [str(d) for d in base_dirs]
        }


def main():
    """Main function to run the downloader."""
    # Load environment variables
    load_dotenv()
    
    # Create configuration with separate folders for data and schemas
    config = DownloadConfig(
        email_value=os.getenv('email_value'),
        id_value=os.getenv('id_value'),
        user_agent=os.getenv('user_agent'),
        download_folder=os.getenv('download_folder', 'data'),  # Default to 'data'
        schema_download_folder='dictionary_files',  # Schemas go to dictionary_files
    )
    
    # Validate required environment variables
    required_vars = ['email_value', 'id_value', 'user_agent']
    missing_vars = [var for var in required_vars if not getattr(config, var)]
    
    if missing_vars:
        raise ValueError(f"Missing required environment variables: {missing_vars}")
    
    # Create downloader
    downloader = GNMAHistoricalDownloader(config)
    
    # Always create folder structure upfront (replaces create_prefix_folders.py)
    print("Creating complete folder structure...")
    folder_results = downloader.create_all_prefix_folders(include_clean=True)
    print(f"✓ Created folder structure for {len(folder_results['prefixes'])} prefixes")
    
    print("\nDownloading historical data files...")
    data_results = downloader.download_all_data()
    
    print("\nDownloading schema/PDF files...")
    schema_results = downloader.download_all_schemas()
    
    # Print summary
    print("\n" + "=" * 60)
    print("DOWNLOAD SUMMARY")
    print("=" * 60)
    
    print(f"\nData files saved to: {config.download_folder}/raw/[prefix]/")
    print("Data Download Results:")
    for prefix, (successful, total) in data_results.items():
        print(f"  {prefix}: {successful}/{total} files downloaded")
    
    print(f"\nSchema files saved to: {config.schema_download_folder}/raw/[prefix]/")
    print("Schema Download Results:")
    for prefix, (successful, total) in schema_results.items():
        print(f"  {prefix}: {successful}/{total} files downloaded")
    
    # Overall totals
    total_data_files = sum(successful for successful, _ in data_results.values())
    total_schema_files = sum(successful for successful, _ in schema_results.values())
    print(f"\nOverall: {total_data_files} data files, {total_schema_files} schema files downloaded")
    
    print(f"\n🎉 Process completed! Full folder structure available:")
    print(f"  - {config.download_folder}/raw/[prefix]/     (downloaded data)")
    print(f"  - {config.download_folder}/clean/[prefix]/   (for processed data)")
    print(f"  - {config.schema_download_folder}/raw/[prefix]/   (downloaded schemas)")
    print(f"  - {config.schema_download_folder}/clean/[prefix]/ (for processed schemas)")


if __name__ == '__main__':
    main() 