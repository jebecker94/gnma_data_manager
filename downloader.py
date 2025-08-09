#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
GNMA Historical Downloader

This module contains the GNMAHistoricalDownloader class for systematically
downloading historical data files from Ginnie Mae's protected website.

Features:
    - Authentication with Ginnie Mae's protected download system
    - Automatic file discovery from web pages
    - Systematic downloading with error handling
    - Organized folder structure for different data types
    - Schema/PDF file downloading

Classes:
    - GNMAHistoricalDownloader: Main downloader class

Dependencies:
    - config: DownloadConfig
    - types: Result classes and exceptions

Author: Jonathan E. Becker
"""

import os
import requests
import datetime
import time
import re
import logging
import pandas as pd
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Union
from dateutil.relativedelta import relativedelta
import yaml
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Imports from other modules
try:
    from .config_options import DownloadConfig
    from .types_and_errors import DownloadError, DateFormatError
except ImportError:
    from config_options import DownloadConfig
    from types_and_errors import DownloadError, DateFormatError

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
        # Ensure logs directory exists
        self.config.logs_folder.mkdir(parents=True, exist_ok=True)
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(self.config.logs_folder / 'gnma_downloader.log'),
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

        # Configure retries for robustness
        retry_strategy = Retry(
            total=3,
            backoff_factor=1.0,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)
        
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

    def download_data_file(self, url: str, local_path: Union[str, Path], skip_existing: bool = True) -> bool:
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

        # Construct the URL for this prefix
        url = f"{self.config.schema_base_url}?prefix={prefix}" # Note: This is the same page where schemas can be located

        # Get the data links
        try:
            # Send GET request to the schema page
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            # Parse the HTML content
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Get all links that start with the base URL and include the prefix
            data_links = soup.find_all('a', href=lambda href: href and prefix in href)

            # Add delay
            time.sleep(self.config.request_delay)

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to access data page for {prefix}: {e}")
            return 0, 0

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
        
        # Step size based on frequency to avoid duplicate attempts
        step_months = {
            'monthly': 1,
            'quarterly': 3,
            'yearly': 12,
        }.get(prefix_config.get('frequency', 'monthly'), 1)

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

            # Check if File is linked on page
            data_file = [x for x in data_links if file_name in x.get('href')]
            if not data_file:
                self.logger.warning(f"File {file_name} not found in data links")
                current_date += relativedelta(months=step_months)
                continue

            # Download file
            total_attempts += 1
            if self.download_data_file(file_url, str(local_path)):
                successful_downloads += 1
            
            # Move to next period
            current_date += relativedelta(months=step_months)
        
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
            prefixes = [k for k in self.prefix_dict.keys() if not str(k).startswith('#')]
        
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
        
        return results
    
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

    def download_schema_file(self, url: str, local_path: Union[str, Path], skip_existing: bool = True) -> bool:
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

            # Add delay
            time.sleep(self.config.request_delay)
            
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
            prefixes = [k for k in self.prefix_dict.keys() if not str(k).startswith('#')]
        
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

    def create_all_prefix_folders(self, include_clean: bool = True):
        """
        Create all prefix folders upfront for organization (like create_prefix_folders.py).
        
        Args:
            include_clean: Whether to create 'clean' folders in addition to 'raw' folders
            
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


## Main Routine
if __name__ == "__main__":

    # Load the environment variables
    load_dotenv()

    # Load the configuration
    cfg = DownloadConfig(
        email_value=os.getenv("email_value"),
        id_value=os.getenv("id_value"),
        user_agent=os.getenv("user_agent"),
        download_folder=os.getenv("download_folder"),
        schema_download_folder="dictionary_files",
        dictionary_file="dictionary_files/prefix_dictionary.yaml",
        request_delay=2.0,
        use_prefix_subfolders=True,
        raw_folder_name="raw",
    )

    # Initialize the downloader
    downloader = GNMAHistoricalDownloader(cfg)

    # Create all prefix folders
    downloader.create_all_prefix_folders(include_clean=True)

    # Download all data
    downloader.download_all_data()

    # Download all schemas
    downloader.download_all_schemas()
