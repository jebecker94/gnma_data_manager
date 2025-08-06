#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
GNMA Data Pipeline - Complete Data Processing Pipeline

This module contains the complete GNMA data processing pipeline with all core classes:
- GNMAHistoricalDownloader: Download historical data and schema files
- GNMADataFormatter: Convert ZIP files to Parquet format  
- GNMADataParser: Intelligent parsing with format detection
- GNMADataProcessor: Process raw data into structured datasets
- GNMASchemaReader: Read and process schema files

This represents the complete workflow: Download → Format → Parse → Process

Usage:
    from gnma_data_pipeline import (
        GNMAHistoricalDownloader, DownloadConfig,
        GNMADataFormatter, FormatterConfig,
        GNMADataParser,
        GNMADataProcessor, ProcessorConfig
        GNMASchemaReader, SchemaReaderConfig
    )
    
    # Complete pipeline
    downloader = GNMAHistoricalDownloader(download_config)
    formatter = GNMADataFormatter(formatter_config)
    parser = GNMADataParser(verbose=False)
    processor = GNMADataProcessor(processor_config)
    schema_reader = GNMASchemaReader(schema_reader_config)
"""

# ==========================================
# IMPORTS AND DEPENDENCIES
# ==========================================

import os
import zipfile
import tempfile
import subprocess
import requests
import datetime
import time
import re
import logging
import pandas as pd
import polars as pl
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Union, Any
from dataclasses import dataclass, field
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
import yaml
import numpy as np
from bs4 import BeautifulSoup
import config
from collections import defaultdict
import pymupdf


# ==========================================
# CONFIGURATION CLASSES
# ==========================================

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
    schema_folder_name: str = "raw"
    bad_text_filters: List[str] = None


@dataclass
class FormatterConfig:
    """Configuration class for the GNMA data formatter."""
    input_folder: str = "data/raw"
    output_folder: str = "data/raw"  # Same folder by default
    text_column_name: str = "text_content"
    dictionary_file: str = "dictionary_files/prefix_dictionary.yaml"
    encoding: str = "utf-8"
    fallback_encoding: str = "iso-8859-1"
    supported_extensions: List[str] = field(default_factory=lambda: ['.txt', '.dat', '.csv'])
    file_pattern: str = "*.zip"
    skip_existing: bool = True
    log_level: str = "INFO"
    batch_size: int = 100
    validate_conversions: bool = True
    use_prefix_subfolders: bool = True


@dataclass
class ProcessorConfig:
    """Configuration class for the GNMA data processor."""
    raw_folder: str = "data/raw"
    clean_folder: str = "data/clean"
    schema_folder: str = "dictionary_files/combined"
    dictionary_file: str = "dictionary_files/prefix_dictionary.yaml"
    skip_existing: bool = True
    log_level: str = "INFO"
    batch_size: int = 50
    validate_outputs: bool = True
    create_directories: bool = True
    default_delimiter: str = "|"
    date_format: str = "%Y%m"
    # Integration settings
    use_parser_for_format_detection: bool = True
    fallback_to_manual_processing: bool = True


@dataclass
class SchemaReaderConfig:
    """Configuration class for the GNMA schema reader."""
    input_folder: str = "data/raw"
    output_folder: str = "data/raw"  # Same folder by default
    text_column_name: str = "text_content"
    dictionary_file: str = "dictionary_files/prefix_dictionary.yaml"
    encoding: str = "utf-8"
    fallback_encoding: str = "iso-8859-1"
    supported_extensions: List[str] = field(default_factory=lambda: ['.txt', '.dat', '.csv'])
    file_pattern: str = "*.zip"
    skip_existing: bool = True
    log_level: str = "INFO"
    batch_size: int = 100
    validate_conversions: bool = True
    use_prefix_subfolders: bool = True
    verbose: bool = False
    overwrite: bool = False
    logging_folder: Path = Path('./logs')


# ==========================================
# RESULT CLASSES
# ==========================================

class ConversionResult:
    """Container for conversion operation results."""
    
    def __init__(self):
        self.successful = 0
        self.failed = 0
        self.skipped = 0
        self.total = 0
        self.errors: List[Dict] = []
        self.processed_files: List[str] = []
        self.skipped_files: List[str] = []
        
    @property
    def total_attempted(self) -> int:
        return self.successful + self.failed
    
    @property
    def success_rate(self) -> float:
        if self.total_attempted == 0:
            return 0.0
        return (self.successful / self.total_attempted) * 100
    
    def add_success(self, file_path: str):
        self.successful += 1
        self.processed_files.append(file_path)
    
    def add_failure(self, file_path: str, error: str):
        self.failed += 1
        self.errors.append({
            'file': file_path,
            'error': error,
            'timestamp': datetime.datetime.now().isoformat()
        })
    
    def add_skip(self, file_path: str, reason: str = "File already exists"):
        self.skipped += 1
        self.skipped_files.append(file_path)


class ProcessingResult:
    """Container for processing operation results."""
    
    def __init__(self):
        self.successful = 0
        self.failed = 0
        self.skipped = 0
        self.total_files = 0
        self.total_records_processed = 0
        self.records_by_type: Dict[str, int] = {}
        self.errors: List[Dict] = []
        self.processed_files: List[str] = []
        self.skipped_files: List[str] = []
        self.output_files: List[str] = []
        
    @property
    def total_attempted(self) -> int:
        return self.successful + self.failed
    
    @property
    def success_rate(self) -> float:
        if self.total_attempted == 0:
            return 0.0
        return (self.successful / self.total_attempted) * 100
    
    def add_success(self, file_path: str, output_path: str, record_count: int, record_type: str):
        self.successful += 1
        self.total_records_processed += record_count
        self.processed_files.append(file_path)
        self.output_files.append(output_path)
        
        if record_type in self.records_by_type:
            self.records_by_type[record_type] += record_count
        else:
            self.records_by_type[record_type] = record_count
    
    def add_failure(self, file_path: str, error: str):
        self.failed += 1
        self.errors.append({
            'file': file_path,
            'error': error,
            'timestamp': datetime.datetime.now().isoformat()
        })
    
    def add_skip(self, file_path: str, reason: str = "File already exists"):
        self.skipped += 1
        self.skipped_files.append(file_path)


# ==========================================
# EXCEPTION CLASSES
# ==========================================

class DateFormatError(Exception):
    """Custom exception for date format detection errors."""
    pass


class DownloadError(Exception):
    """Custom exception for download errors."""
    pass


# ==========================================
# CORE PIPELINE CLASSES
# ==========================================

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
                current_date += relativedelta(months=1)
                continue

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


class GNMADataFormatter:
    """
    A class for converting GNMA data files between different formats.
    
    This class handles conversion between various data formats commonly used in
    GNMA data processing, with a focus on ZIP to Parquet conversion for efficient
    downstream processing.
    """
    
    def __init__(self, config: FormatterConfig):
        """
        Initialize the formatter with configuration.
        
        Args:
            config: FormatterConfig object containing all necessary parameters
        """
        self.config = config
        self._setup_logging()
        self.logger.info("GNMA Data Formatter initialized")
        self.prefix_dict = {}
        
        # Load prefix dictionary
        self._load_prefix_dictionary()
    
    def _setup_logging(self) -> None:
        """Set up logging configuration."""
        log_level = getattr(logging, self.config.log_level.upper(), logging.INFO)
        
        logging.basicConfig(
            level=log_level,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('gnma_formatter.log'),
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

    def _get_output_path(self, input_path: Path, output_folder: Optional[Path] = None) -> Path:
        """
        Generate the output path for a converted file.
        
        Args:
            input_path: Path to the input file
            output_folder: Optional override for output folder
            
        Returns:
            Path object for the output file
        """
        if output_folder is None:
            output_folder = Path(self.config.output_folder)
        
        # Maintain the same relative structure
        if self.config.use_prefix_subfolders:
            # Extract prefix from path structure (assuming data/raw/prefix/file.zip)
            try:
                relative_parts = input_path.relative_to(Path(self.config.input_folder)).parts
                if len(relative_parts) >= 2:
                    # Maintain prefix subfolder structure
                    prefix_folder = output_folder / relative_parts[0]
                    output_file = prefix_folder / input_path.with_suffix('.parquet').name
                else:
                    output_file = output_folder / input_path.with_suffix('.parquet').name
            except ValueError:
                # Fallback if relative path doesn't work
                output_file = output_folder / input_path.with_suffix('.parquet').name
        else:
            output_file = output_folder / input_path.with_suffix('.parquet').name
        
        return output_file
    
    def convert_zip_to_parquet(
        self,
        zip_file_path: Union[str, Path],
        output_file_path: Optional[Union[str, Path]] = None,
        **kwargs
    ) -> bool:
        """
        Convert a single ZIP file containing text files to Parquet format.
        
        Args:
            zip_file_path: Path to the ZIP file
            output_file_path: Optional path for output file (auto-generated if not provided)
            **kwargs: Additional arguments (text_column_name, encoding, etc.)
            
        Returns:
            True if conversion successful, False otherwise
        """
        zip_path = Path(zip_file_path)
        
        # Generate output path if not provided
        if output_file_path is None:
            output_path = self._get_output_path(zip_path)
        else:
            output_path = Path(output_file_path)
        
        # Check if we should skip existing files
        if self.config.skip_existing and output_path.exists():
            self.logger.debug(f"Skipping existing file: {output_path}")
            return True
        
        # Extract configuration with overrides
        text_column_name = kwargs.get('text_column_name', self.config.text_column_name)
        encoding = kwargs.get('encoding', self.config.encoding)
        
        self.logger.debug(f"Converting: {zip_path} -> {output_path}")
        
        # Ensure output directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                # Find text files in the zip
                text_files = [
                    name for name in zip_ref.namelist() 
                    if any(name.lower().endswith(ext) for ext in self.config.supported_extensions)
                ]
                
                if not text_files:
                    self.logger.warning(f"No text files found in {zip_path}")
                    return False
                
                if len(text_files) > 1:
                    self.logger.debug(f"Multiple text files found, processing all: {text_files}")
                
                all_lines = []
                
                # Process each text file in the zip
                for text_file in text_files:
                    self.logger.debug(f"  Extracting: {text_file}")
                    
                    try:
                        # Read file content directly from zip
                        with zip_ref.open(text_file) as file:
                            content = file.read().decode(encoding)
                            lines = content.splitlines()
                            all_lines.extend(lines)
                            
                    except UnicodeDecodeError:
                        # Fallback to different encoding
                        self.logger.debug(f"  Encoding {encoding} failed, trying {self.config.fallback_encoding}...")
                        with zip_ref.open(text_file) as file:
                            content = file.read().decode(self.config.fallback_encoding)
                            lines = content.splitlines()
                            all_lines.extend(lines)
                
                # Create polars DataFrame and save as parquet
                if all_lines:
                    # Use polars lazy loading for efficiency
                    df = pl.LazyFrame({text_column_name: all_lines})
                    
                    # Write to parquet
                    df.sink_parquet(output_path)
                    
                    self.logger.debug(f"  Saved {len(all_lines)} lines to {output_path}")
                    return True
                else:
                    self.logger.warning(f"No content found in {zip_path}")
                    return False
                    
        except Exception as e:
            self.logger.error(f"Error converting {zip_path}: {e}")
            # Clean up partial output
            if output_path.exists():
                try:
                    output_path.unlink()
                except:
                    pass
            return False
    
    def convert_txt_to_parquet(
        self,
        txt_file_path: Union[str, Path],
        output_file_path: Optional[Union[str, Path]] = None,
        **kwargs
    ) -> bool:
        """
        Convert a single TXT file to Parquet format.
        
        Args:
            txt_file_path: Path to the TXT file
            output_file_path: Optional path for output file (auto-generated if not provided)
            **kwargs: Additional arguments (text_column_name, encoding, etc.)
            
        Returns:
            True if conversion successful, False otherwise
        """
        txt_path = Path(txt_file_path)
        
        # Generate output path if not provided
        if output_file_path is None:
            output_path = self._get_output_path(txt_path)
        else:
            output_path = Path(output_file_path)
        
        # Check if we should skip existing files
        if self.config.skip_existing and output_path.exists():
            self.logger.debug(f"Skipping existing file: {output_path}")
            return True
        
        # Extract configuration with overrides
        text_column_name = kwargs.get('text_column_name', self.config.text_column_name)
        encoding = kwargs.get('encoding', self.config.encoding)
        
        self.logger.debug(f"Converting: {txt_path} -> {output_path}")
        
        # Ensure output directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        try:
            with open(txt_path, 'r') as txt_file:
                self.logger.debug(f"  Extracting: {txt_path}")
                
                # Read file content directly from txt
                all_lines = []
                content = txt_file.read()
                lines = content.splitlines()
                all_lines.extend(lines)
            
            # Create polars DataFrame and save as parquet
            if all_lines:
                # Use polars lazy loading for efficiency
                df = pl.LazyFrame({text_column_name: all_lines})
                
                # Write to parquet
                df.sink_parquet(output_path)
                
                self.logger.debug(f"  Saved {len(all_lines)} lines to {output_path}")
                return True
            else:
                self.logger.warning(f"No content found in {txt_path}")
                return False
                    
        except Exception as e:
            self.logger.error(f"Error converting {txt_path}: {e}")
            # Clean up partial output
            if output_path.exists():
                try:
                    output_path.unlink()
                except:
                    pass
            return False

    def process_prefix_data(
        self, 
        prefix: str, 
        base_folder: Optional[Union[str, Path]] = None,
        show_progress: bool = True
    ) -> ConversionResult:
        """
        Process all ZIP files for a specific prefix.
        
        Args:
            prefix: Data prefix (e.g., 'dailyllmni', 'monthly')
            base_folder: Optional base folder override
            show_progress: Whether to show progress updates
            
        Returns:
            ConversionResult object with detailed results
        """
        if base_folder is None:
            base_folder = Path(self.config.input_folder)
        else:
            base_folder = Path(base_folder)
        
        # Determine prefix folder
        if self.config.use_prefix_subfolders:
            prefix_folder = base_folder / prefix
        else:
            prefix_folder = base_folder
        
        if not prefix_folder.exists():
            self.logger.error(f"Prefix folder not found: {prefix_folder}")
            return ConversionResult()
        
        self.logger.info(f"Processing prefix '{prefix}' from {prefix_folder}")
        
        # Find ZIP files
        zip_files = list(prefix_folder.glob(f"{prefix}_*.zip"))
        result = ConversionResult()
        result.total = len(zip_files)
        
        for zip_file in zip_files:
            output_file = self._get_output_path(zip_file)
            
            if self.config.skip_existing and output_file.exists():
                result.add_skip(str(zip_file))
                continue
            
            try:
                success = self.convert_zip_to_parquet(zip_file, output_file)
                if success:
                    result.add_success(str(zip_file))
                else:
                    result.add_failure(str(zip_file), "Conversion returned False")
            except Exception as e:
                result.add_failure(str(zip_file), str(e))

        # Find TXT files
        txt_files = list(set(list(prefix_folder.glob(f"{prefix}_*.txt"))+list(prefix_folder.glob(f"{prefix}_*.TXT"))))
        result.total += len(txt_files)
        
        for txt_file in txt_files:
            output_file = self._get_output_path(txt_file)
            
            if self.config.skip_existing and output_file.exists():
                result.add_skip(str(txt_file))
                continue
            
            try:
                success = self.convert_txt_to_parquet(txt_file, output_file)
                if success:
                    result.add_success(str(txt_file))
                else:
                    result.add_failure(str(txt_file), "Conversion returned False")
            except Exception as e:
                result.add_failure(str(txt_file), str(e))
        
        return result

    def process_all_prefixes(self, exclude_prefixes: Optional[List[str]] = None):
        """
        Process all prefixes in the configuration.
        """
        if exclude_prefixes is None:
            exclude_prefixes = []

        for prefix in self.prefix_dict.keys():
            if prefix not in exclude_prefixes:
                self.process_prefix_data(prefix)


class GNMADataParser:
    """
    Enhanced intelligent parser for GNMA data files that automatically detects format type
    and applies appropriate parsing strategy.
    
    This enhanced version includes core parsing functionality plus convenience methods
    for format detection and basic analysis.
    """
    
    def __init__(self, verbose: bool = False):
        self.verbose = verbose
        self.schema_cache = {}
        self._format_cache = {}  # Cache format detection results
        
    def detect_file_format(self, schema_df: pd.DataFrame) -> str:
        """
        Detect whether a file is delimited or fixed-width based on schema structure.
        
        Args:
            schema_df: DataFrame containing the schema definition
            
        Returns:
            str: 'DELIMITED', 'FIXED_WIDTH', or 'UNKNOWN'
        """
        columns = list(schema_df.columns)
        
        has_max_length = 'Max Length' in columns
        has_begin_end = 'Begin' in columns and 'End' in columns
        has_format = 'Format' in columns
        has_description = 'Description' in columns or 'Definition' in columns
        
        if has_max_length and has_format:
            return 'DELIMITED'
        elif has_begin_end:
            return 'FIXED_WIDTH'
        else:
            return 'UNKNOWN'
    
    def parse_data_file(self, data_file_path: Path, schema_file_path: Path) -> pd.DataFrame:
        """
        Parse a GNMA data file using its corresponding schema.
        
        Args:
            data_file_path: Path to the data file
            schema_file_path: Path to the schema CSV file
            
        Returns:
            pd.DataFrame: Parsed data
        """
        if self.verbose:
            print(f"\nParsing: {data_file_path.name}")
            print(f"   Schema: {schema_file_path.name}")
        
        try:
            # Load schema
            schema_df = pd.read_csv(schema_file_path)
            
            if schema_df.empty:
                if self.verbose:
                    print(f"   Error: Empty schema file")
                return pd.DataFrame()
            
            # Detect format
            file_format = self.detect_file_format(schema_df)
            
            if self.verbose:
                print(f"   Detected format: {file_format}")
            
            # For this combined version, we'll provide basic parsing
            # Full parsing logic would be implemented here
            # This is a simplified version for the combined module
            
            return pd.DataFrame()  # Placeholder
            
        except Exception as e:
            if self.verbose:
                print(f"   Error parsing file: {e}")
            return pd.DataFrame()
    
    def analyze_prefix_format(self, prefix: str, schema_folder: str = 'dictionary_files/combined') -> Dict:
        """
        Analyze format for a specific prefix and return detailed information.
        
        Args:
            prefix: File prefix to analyze
            schema_folder: Folder containing combined schema files
            
        Returns:
            Dict: Format analysis results
        """
        # Check cache first
        if prefix in self._format_cache:
            return self._format_cache[prefix]
        
        schema_file = Path(schema_folder) / f"{prefix}_combined_schema.csv"
        
        if not schema_file.exists():
            result = {
                'prefix': prefix,
                'format_type': 'ERROR',
                'error': f"Schema file not found: {schema_file}",
                'schema_exists': False
            }
            self._format_cache[prefix] = result
            return result
        
        try:
            schema_df = pd.read_csv(schema_file)
            
            if schema_df.empty:
                result = {
                    'prefix': prefix,
                    'format_type': 'ERROR',
                    'error': "Empty schema file",
                    'schema_exists': True
                }
                self._format_cache[prefix] = result
                return result
            
            # Detect format
            file_format = self.detect_file_format(schema_df)
            columns = list(schema_df.columns)
            
            # Analyze schema structure
            has_record_types = 'Record_Type' in schema_df.columns
            record_type_count = 0
            record_types = []
            if has_record_types:
                record_types = schema_df['Record_Type'].dropna().unique().tolist()
                record_type_count = len(record_types)
            
            result = {
                'prefix': prefix,
                'format_type': file_format,
                'schema_exists': True,
                'schema_rows': len(schema_df),
                'schema_columns': len(columns),
                'has_record_types': has_record_types,
                'record_type_count': record_type_count,
                'record_types': record_types,
                'column_names': columns,
                'error': None
            }
            
            # Cache the result
            self._format_cache[prefix] = result
            return result
            
        except Exception as e:
            result = {
                'prefix': prefix,
                'format_type': 'ERROR',
                'error': str(e),
                'schema_exists': True
            }
            self._format_cache[prefix] = result
            return result


class GNMADataProcessor:
    """
    A class for processing GNMA raw data files into structured datasets.
    
    This class handles the conversion of raw parquet files (from the formatter)
    into properly structured datasets using schema definitions. It supports both
    delimited and fixed-width formats with intelligent detection.
    """
    
    def __init__(self, config: ProcessorConfig):
        """
        Initialize the processor with configuration.
        
        Args:
            config: ProcessorConfig object containing all necessary parameters
        """
        self.config = config
        self._setup_logging()
        
        # Initialize data parser for format detection
        if self.config.use_parser_for_format_detection:
            self.parser = GNMADataParser(verbose=False)
        else:
            self.parser = None
        
        # Load prefix dictionary
        self._load_prefix_dictionary()
        
        # Create directories if needed
        if self.config.create_directories:
            self._ensure_directories_exist()
        
        self.logger.info("GNMA Data Processor initialized")
    
    def _setup_logging(self) -> None:
        """Set up logging configuration."""
        log_level = getattr(logging, self.config.log_level.upper(), logging.INFO)
        
        logging.basicConfig(
            level=log_level,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('gnma_processor.log'),
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
            self.logger.warning(f"Dictionary file not found: {self.config.dictionary_file}")
            self.prefix_dict = {}
        except yaml.YAMLError as e:
            self.logger.error(f"Error parsing YAML file: {e}")
            self.prefix_dict = {}
    
    def _ensure_directories_exist(self) -> None:
        """Create necessary directories."""
        for directory in [self.config.raw_folder, self.config.clean_folder]:
            os.makedirs(directory, exist_ok=True)
    
    def detect_file_format(self, prefix: str) -> str:
        """
        Detect whether files for a prefix should be processed as delimited or fixed-width.
        
        Args:
            prefix: File prefix to analyze
            
        Returns:
            'DELIMITED', 'FIXED_WIDTH', or 'UNKNOWN'
        """
        if self.parser:
            try:
                analysis = self.parser.analyze_prefix_format(prefix)
                return analysis['format_type']
            except Exception as e:
                self.logger.warning(f"Error detecting format for {prefix}: {e}")
        
        return 'UNKNOWN'
    
    def read_combined_schema(self, file_prefix: str) -> Dict[str, pd.DataFrame]:
        """
        Read the combined schema for a given file prefix.
        
        Args:
            file_prefix: The file prefix (e.g., 'dailyllmni')
            
        Returns:
            Dictionary with record types as keys and their schemas as DataFrames
        """
        schema_file = Path(self.config.schema_folder) / f"{file_prefix}_combined_schema.csv"
        
        if not schema_file.exists():
            raise FileNotFoundError(f"Schema file not found: {schema_file}")
        
        # Read the combined schema
        schema_df = pd.read_csv(schema_file)
        
        # Group by Record_Type
        schemas_by_record_type = {}
        for record_type in schema_df['Record_Type'].unique():
            if pd.notna(record_type):  # Skip NaN record types
                record_schema = schema_df[schema_df['Record_Type'] == record_type].copy()
                record_schema = record_schema.sort_values('Item')
                schemas_by_record_type[record_type] = record_schema
        
        return schemas_by_record_type

    # def process_prefix_data(
    #     self,
    #     prefix: str,
    #     record_types: Optional[List[str]] = None,
    #     show_progress: bool = True
    # ) -> ProcessingResult:
    #     """
    #     Process all parquet files for a specific prefix.
        
    #     Args:
    #         prefix: Data prefix to process
    #         record_types: Optional list of record types to process
    #         show_progress: Whether to show progress updates
            
    #     Returns:
    #         ProcessingResult object with detailed results
    #     """
    #     self.logger.info(f"Processing prefix: {prefix}")
        
    #     # Initialize result
    #     result = ProcessingResult()
        
    #     # Get parquet files for this prefix
    #     raw_folder = Path(self.config.raw_folder)
    #     parquet_files = list(raw_folder.rglob(f'{prefix}_*.parquet'))
        
    #     if not parquet_files:
    #         self.logger.warning(f"No parquet files found for prefix: {prefix}")
    #         return result
        
    #     result.total_files = len(parquet_files)
        
    #     # For this combined version, we'll provide basic processing
    #     # Full processing logic would be implemented here
    #     # This is a simplified version for the combined module
        
    #     self.logger.info(f"Found {len(parquet_files)} files for {prefix}")
        
    #     return result
    
    def process_prefix_data(
        self,
        prefix: str,
        record_types: Optional[List[str]] = None,
        show_progress: bool = True
    ) -> ProcessingResult:
        """
        Process all parquet files for a specific prefix.
        
        Args:
            prefix: Data prefix to process
            record_types: Optional list of record types to process
            show_progress: Whether to show progress updates
            
        Returns:
            ProcessingResult object with detailed results
        """
        self.logger.info(f"Processing prefix: {prefix}")
        
        # Initialize result
        result = ProcessingResult()
        
        # Get parquet files for this prefix
        raw_folder = Path(self.config.raw_folder)
        parquet_files = list(raw_folder.rglob(f'{prefix}_*.parquet'))
        
        if not parquet_files:
            self.logger.warning(f"No parquet files found for prefix: {prefix}")
            return result
        
        result.total_files = len(parquet_files)
        
        # Load schema to get available record types
        try:
            schemas = self.read_combined_schema(prefix)
            available_record_types = list(schemas.keys())
            
            # Use provided record types or all available
            if record_types is None:
                record_types = available_record_types
            else:
                # Validate requested record types
                invalid_types = [rt for rt in record_types if rt not in available_record_types]
                if invalid_types:
                    self.logger.warning(f"Invalid record types for {prefix}: {invalid_types}")
                    record_types = [rt for rt in record_types if rt in available_record_types]
            
        except Exception as e:
            self.logger.error(f"Error loading schema for {prefix}: {e}")
            return result
        
        # Detect file format once for the prefix
        file_format = self.detect_file_format(prefix)
        self.logger.info(f"Detected format for {prefix}: {file_format}")
        
        # Process each file and record type combination
        total_operations = len(parquet_files) * len(record_types)
        operation_count = 0
        
        for parquet_file in parquet_files:
            for record_type in record_types:
                operation_count += 1
                
                if show_progress and operation_count % 10 == 0:
                    self.logger.info(f"Progress: {operation_count}/{total_operations} operations")
                
                # Generate output file path
                ym = parquet_file.stem.split(f'{prefix}_')[1]
                clean_folder = Path(self.config.clean_folder)
                output_file = clean_folder / prefix / record_type / f'{prefix}_{ym}_{record_type}.parquet'
                
                # Skip if file exists and not overwriting
                if self.config.skip_existing and output_file.exists():
                    result.add_skip(str(parquet_file))
                    continue
                
                # Process the file
                success, record_count = self.process_single_file(
                    parquet_file, prefix, record_type, output_file, file_format
                )
                
                if success:
                    result.add_success(str(parquet_file), str(output_file), record_count, record_type)
                else:
                    result.add_failure(str(parquet_file), f"Failed to process record type {record_type}")
        
        self.logger.info(f"Completed {prefix}: {result.successful}/{result.total_attempted} successful, "
                        f"{result.total_records_processed} total records processed")
        
        return result
    
    def create_structured_dataset(
        self,
        prefix: str,
        record_types: Optional[List[str]] = None,
        include_formatting: bool = True
    ) -> ProcessingResult:
        """
        Complete end-to-end processing for a prefix (format + process if needed).
        
        Args:
            prefix: Data prefix to process
            record_types: Optional list of record types to process
            include_formatting: Whether to run formatting step first
            
        Returns:
            ProcessingResult from processing step
        """
        self.logger.info(f"Creating structured dataset for: {prefix}")
        
        # Step 1: Format raw data if requested
        if include_formatting:
            formatter_config = FormatterConfig(
                input_folder=self.config.raw_folder,
                output_folder=self.config.raw_folder,
                skip_existing=True,
                log_level=self.config.log_level
            )
            formatter = GNMADataFormatter(formatter_config)
            
            self.logger.info(f"Formatting raw data for {prefix}...")
            format_result = formatter.process_prefix_data(prefix, show_progress=False)
            self.logger.info(f"Formatting complete: {format_result.successful} files converted")
        
        # Step 2: Process formatted data into structured datasets
        return self.process_prefix_data(prefix, record_types, show_progress=True)

    def process_fixed_width_parquet(
        self,
        parquet_file: Path,
        schema: pd.DataFrame,
        record_type: str
    ) -> pl.LazyFrame:
        """
        Process a parquet file using fixed-width parsing logic.
        
        Args:
            parquet_file: Path to the parquet file
            schema: Schema DataFrame for the record type
            record_type: Record type being processed
            
        Returns:
            Processed Polars LazyFrame
        """
        # Load the parquet file
        df = pl.scan_parquet(parquet_file)
        
        # Get column information from schema
        data_item_column = self._get_data_column_name(schema)
        length_column = self._get_length_column_name(schema)
        
        final_column_names = schema[data_item_column].tolist()
        widths = schema[length_column].tolist()
        
        # Add number suffixes to final column names with "Filler"
        final_column_names = [
            x + f"_{i}" if "Filler" in x else x 
            for i, x in enumerate(final_column_names)
        ]
        
        # Calculate slice positions
        slice_tuples = []
        offset = 0
        for width in widths:
            slice_tuples.append((offset, width))
            offset += width
        
        # Create final DataFrame by slicing text content
        df = df.with_columns([
            pl.col("text_content")
            .str.slice(slice_tuple[0], slice_tuple[1])
            .str.strip_chars()
            .alias(col)
            for slice_tuple, col in zip(slice_tuples, final_column_names)
        ]).drop("text_content")
        
        # Filter by record type
        record_type_column = self._find_record_type_column(df.collect_schema().names())
        if record_type_column:
            df = df.filter(pl.col(record_type_column) == record_type)
        
        return df

    def process_delimited_parquet(
        self,
        parquet_file: Path,
        schema: pd.DataFrame,
        record_type: str,
        delimiter: str = None
    ) -> pl.LazyFrame:
        """
        Process a parquet file using delimited parsing logic.
        
        Args:
            parquet_file: Path to the parquet file
            schema: Schema DataFrame for the record type
            record_type: Record type being processed
            delimiter: Delimiter to use (defaults to config default)
            
        Returns:
            Processed Polars LazyFrame
        """
        if delimiter is None:
            delimiter = self.config.default_delimiter
        
        # Load the parquet file
        df = pl.read_parquet(parquet_file)
        
        # Get column names from schema
        data_element_column = self._get_data_column_name(schema)
        final_column_names = schema[data_element_column].tolist()
        
        if not final_column_names:
            raise ValueError(f"No column names found in schema for record type {record_type}")
        
        number_columns = len(final_column_names)
        
        # Split text content into columns
        df_record = df.with_columns(
            pl.col("text_content")
            .str.split_exact(delimiter, number_columns - 1)
            .alias("parts")
        ).unnest("parts").drop("text_content")
        
        # Rename columns using final_column_names
        df_record = df_record.rename({
            col: final_column_names[i]
            for i, col in enumerate(df_record.columns)
        })
        
        # Filter by record type
        record_type_column = self._find_record_type_column(df_record.columns)
        if record_type_column:
            df_record = df_record.filter(pl.col(record_type_column) == record_type)
        
        return df_record.lazy()

    def _get_data_column_name(self, schema: pd.DataFrame) -> str:
        """Get the appropriate data column name from schema."""
        if "Data Item" in schema.columns:
            return "Data Item"
        elif "Data Element" in schema.columns:
            return "Data Element"
        else:
            raise ValueError("Data Item or Data Element column not found in schema")
    
    def _get_length_column_name(self, schema: pd.DataFrame) -> str:
        """Get the appropriate length column name from schema."""
        if "Length" in schema.columns:
            return "Length"
        elif "Max Length" in schema.columns:
            return "Max Length"
        else:
            raise ValueError("Length or Max Length column not found in schema")
    
    def _find_record_type_column(self, columns: List[str]) -> Optional[str]:
        """Find the record type column in a list of column names."""
        for col in columns:
            if 'Record Type' in col:
                return col
        return None
    
    def process_single_file(
        self,
        parquet_file: Path,
        prefix: str,
        record_type: str,
        output_file: Path,
        file_format: str = None
    ) -> Tuple[bool, int]:
        """
        Process a single parquet file for a specific record type.
        
        Args:
            parquet_file: Path to input parquet file
            prefix: File prefix
            record_type: Record type to process
            output_file: Path for output file
            file_format: Optional format override ('DELIMITED' or 'FIXED_WIDTH')
            
        Returns:
            Tuple of (success, record_count)
        """
        try:
            # Load schema
            schemas = self.read_combined_schema(prefix)
            if record_type not in schemas:
                self.logger.warning(f"Record type '{record_type}' not found in schema for {prefix}")
                return False, 0
            
            schema = schemas[record_type]
            
            # Extract date from filename for schema filtering
            ym = parquet_file.stem.split(f'{prefix}_')[1]
            ym_dt = pd.to_datetime(ym, format=self.config.date_format).strftime('%Y-%m-%d')
            
            # Filter schema by date range
            file_schema = schema[
                (schema['Min_Date'] <= ym_dt) & (ym_dt <= schema['Max_Date'])
            ]
            
            if file_schema.empty:
                self.logger.warning(f"No schema found for {prefix} on {ym_dt}")
                return False, 0
            
            # Detect format if not provided
            if file_format is None:
                file_format = self.detect_file_format(prefix)
            
            # Process based on format
            if file_format == 'DELIMITED':
                df = self.process_delimited_parquet(parquet_file, file_schema, record_type)
            elif file_format == 'FIXED_WIDTH':
                df = self.process_fixed_width_parquet(parquet_file, file_schema, record_type)
            else:
                self.logger.error(f"Unknown file format '{file_format}' for {prefix}")
                return False, 0
            
            # Ensure output directory exists
            output_file.parent.mkdir(parents=True, exist_ok=True)
            
            # Save to clean directory
            df.sink_parquet(output_file)
            
            # Count records for reporting
            record_count = len(pl.read_parquet(output_file))
            
            self.logger.debug(f"Processed {parquet_file.name} -> {output_file.name} ({record_count} records)")
            
            return True, record_count
            
        except Exception as e:
            self.logger.error(f"Error processing {parquet_file}: {e}")
            return False, 0
    
    def process_multiple_prefixes(
        self,
        prefixes: List[str] | None = None,
        record_types: Optional[List[str]] = None,
        show_progress: bool = True
    ) -> Dict[str, ProcessingResult]:
        """
        Process multiple prefixes in batch.
        
        Args:
            prefixes: List of prefixes to process
            record_types: Optional list of record types to process for all prefixes
            show_progress: Whether to show progress updates
            
        Returns:
            Dictionary mapping prefix to ProcessingResult
        """

        if prefixes is None:
            prefixes = self.prefix_dict.keys()

        results = {}
        
        self.logger.info(f"Processing {len(prefixes)} prefixes: {prefixes}")
        
        for i, prefix in enumerate(prefixes, 1):
            if show_progress:
                self.logger.info(f"Processing prefix {i}/{len(prefixes)}: {prefix}")
            
            try:
                result = self.process_prefix_data(prefix, record_types, show_progress)
                results[prefix] = result
            except Exception as e:
                self.logger.error(f"Error processing prefix {prefix}: {e}")
                # Create empty result for failed prefix
                failed_result = ProcessingResult()
                failed_result.add_failure(prefix, str(e))
                results[prefix] = failed_result
        
        return results


class GNMASchemaReader:
    """
    GNMA Schema Reader

    This class provides functionality for extracting, cleaning, and analyzing GNMA (Government National 
    Mortgage Association) schema definitions from PDF documents. It supports:

    - PDF table extraction and conversion to CSV
    - Schema cleaning and standardization
    - Multi-version schema combination and reconciliation
    - Temporal coverage analysis across schema versions
    - Format detection (delimited vs fixed-width)
    - Field name standardization and analysis
    - COBOL format parsing for data type mapping

    Key Components:
    - PDF processing using PyMuPDF
    - Data cleaning and grouping logic
    - Record type extraction and validation
    - Analysis and reporting functions

    Usage:
        Run as script to process all schemas, or import functions for custom processing.
        Configuration is handled through config.py and command-line flags in main routine.
    """


    
    # Init
    def __init__(self, config: SchemaReaderConfig):
        self.config = config
        self.overwrite = config.overwrite
        self.verbose = config.verbose
        self.schema_cache = {}

        # Set up logging
        self._setup_logging()

        self._load_prefix_dictionary()

    def _setup_logging(self) -> None:
        """Set up logging configuration."""

        # Create Logging Folder
        self.config.logging_folder.mkdir(parents=True, exist_ok=True)

        # Create Logger
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(self.config.logging_folder / 'gnma_downloader.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    # Load Prefix Dictionary
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

    # Extract Dates Between Parentheses
    def extract_dates_between_parentheses(self, file_path: str | Path) -> List[datetime.datetime | None]:
        """
        Extract dates between parentheses from a file path.
        
        Returns:
            List containing [min_date, max_date] as datetime objects, or [None, None] if parsing fails
        """
        
        filename = Path(file_path).name
        
        # Check if file has parentheses
        if '(' not in filename or ')' not in filename:
            return [None, None]
        
        # Try to extract date range from file paths
        try:
            date_range = filename.split('(')[-1].split(')')[0]
            
            # Check if the content looks like a date range (should contain a hyphen)
            if '-' not in date_range:
                return [None, None]
                
            # Convert min and max dates to datetime
            min_date_str, max_date_str = date_range.split('-', 1)  # Split on first hyphen only
            min_date_str = min_date_str.strip()
            max_date_str = max_date_str.strip()
            
            # Check if we have non-empty strings
            if not min_date_str or not max_date_str:
                return [None, None]
            
            min_date = pd.to_datetime(min_date_str)
            
            if max_date_str.lower() == 'present':
                max_date = datetime.datetime(2099, 12, 31)
            else:
                max_date = pd.to_datetime(max_date_str)
                
            return [min_date, max_date]
            
        except Exception:
            return [None, None]

    # Extract Record Type Code
    def extract_record_type_code(self, value: str) -> str:
        """
        Extract the record type code from a record type string.
        
        Handles multiple patterns:
        - "Record Type = XX" format
        - "Record Type X = Description" format  
        - "Record Type (X = Description)" format
        - "Record Type (X)" format
        
        Args:
            value: String containing record type information
            
        Returns:
            Extracted code (e.g., 'H', 'PS', '01') or None if no code found
        """
        if not value or not isinstance(value, str):
            return None
            
        value = str(value).strip()
        
        # Skip if this is just the field name "Record Type"
        if value.lower().strip() == 'record type':
            return None
        
        # Pattern 1: "Record Type = XX" or "Record Type=XX"
        match = re.search(r'record\s*type\s*=\s*([A-Z0-9]+)', value, re.IGNORECASE)
        if match:
            return match.group(1).upper()
        
        # Pattern 2: "Record Type (X = Description)" 
        match = re.search(r'record\s*type\s*\(\s*([A-Z0-9]+)\s*=', value, re.IGNORECASE)
        if match:
            return match.group(1).upper()
        
        # Pattern 3: "Record Type X = Description"
        match = re.search(r'record\s*type\s+([A-Z0-9]+)\s*=', value, re.IGNORECASE)
        if match:
            return match.group(1).upper()
        
        # Pattern 4: "Record Type (X)"
        match = re.search(r'record\s*type\s*\(\s*([A-Z0-9]+)\s*\)', value, re.IGNORECASE)
        if match:
            return match.group(1).upper()
        
        return None

    # Add Record Type Column
    def add_record_type_column(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add a 'Record_Type' column to the DataFrame by extracting codes from 
        'Data Item' and 'Data Element' columns, then propagating to entire groups.
        
        Args:
            df: DataFrame with schema data
            
        Returns:
            DataFrame with added 'Record_Type' column propagated to entire groups
        """
        if df.empty:
            return df
        
        # Create a copy to avoid modifying the original
        df_copy = df.copy()
        
        # Initialize record_type column
        df_copy['Record_Type'] = None
        
        # Check both Data Item and Data Element columns
        data_cols = [col for col in df_copy.columns if col in ['Data Item', 'Data Element']]
        
        for col in data_cols:
            if col in df_copy.columns:
                # Extract record type codes
                df_copy['Record_Type'] = df_copy.apply(
                    lambda row: self.extract_record_type_code(row[col]) if pd.isna(row['Record_Type']) else row['Record_Type'],
                    axis=1
                )
        
        # If we have group_id column, propagate record types to entire groups
        if 'group_id' in df_copy.columns:
            df_copy = self.propagate_record_types_to_groups(df_copy)
        
        return df_copy

    # Propagate Record Types to Groups
    def propagate_record_types_to_groups(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Propagate record types to all rows within each group_id.
        
        First validates that each group has at most one unique record type,
        then fills all rows in each group with that record type.
        
        Args:
            df: DataFrame with group_id and record_type columns
            
        Returns:
            DataFrame with record_type propagated to entire groups
        """
        if 'group_id' not in df.columns or 'Record_Type' not in df.columns:
            return df
        
        df_copy = df.copy()
        
        # Check each group for record type consistency
        validation_results = []
        groups_with_conflicts = []
        
        for group_id in df_copy['group_id'].unique():
            group_data = df_copy[df_copy['group_id'] == group_id]
            
            # Get non-null record types in this group
            group_record_types = group_data['Record_Type'].dropna().unique()
            
            validation_results.append({
                'group_id': group_id,
                'record_types': list(group_record_types),
                'count': len(group_record_types),
                'rows_in_group': len(group_data)
            })
            
            # Check for conflicts (more than one record type per group)
            if len(group_record_types) > 1:
                groups_with_conflicts.append({
                    'group_id': group_id,
                    'record_types': list(group_record_types),
                    'rows': len(group_data)
                })
        
        # Report validation results
        total_groups = len(validation_results)
        groups_with_records = sum(1 for r in validation_results if r['count'] > 0)
        groups_with_conflicts_count = len(groups_with_conflicts)
        
        print(f"Record type validation:")
        print(f"  Total groups: {total_groups}")
        print(f"  Groups with record types: {groups_with_records}")
        print(f"  Groups with conflicts: {groups_with_conflicts_count}")
        
        if groups_with_conflicts:
            print(f"Groups with multiple record types:")
            for conflict in groups_with_conflicts[:5]:  # Show first 5 conflicts
                print(f"    Group {conflict['group_id']}: {conflict['record_types']} ({conflict['rows']} rows)")
            if len(groups_with_conflicts) > 5:
                print(f"    ... and {len(groups_with_conflicts) - 5} more conflicts")
        
        # Propagate record types to all rows in each group
        for group_id in df_copy['group_id'].unique():
            group_mask = df_copy['group_id'] == group_id
            group_record_types = df_copy[group_mask]['Record_Type'].dropna().unique()
            
            if len(group_record_types) == 1:
                # Single record type - propagate to all rows in group
                df_copy.loc[group_mask, 'Record_Type'] = group_record_types[0]
            elif len(group_record_types) > 1:
                # Multiple record types - use the first one found (could be improved)
                df_copy.loc[group_mask, 'Record_Type'] = group_record_types[0]
                # Note: In practice, you might want different logic here
        
        return df_copy

    # Clean Individual DataFrame
    def clean_extracted_dataframe(
        self,
        df: pd.DataFrame, 
        filename: str,
        add_record_types: bool = True,
        verbose: bool = False
    ) -> pd.DataFrame:
        """
        Apply cleaning operations to a single extracted DataFrame.
        
        This includes:
        - Column name standardization
        - Grouping by Item resets
        - Filtering unwanted rows and groups
        - Record type extraction
        
        Args:
            df: Raw DataFrame from PDF extraction
            filename: Source filename for context
            add_record_types: Whether to extract record types
            verbose: Whether to print detailed output
            
        Returns:
            Cleaned DataFrame ready for saving
        """
        if df.empty:
            return df
        
        if verbose:
            print(f"    Cleaning extracted data from {filename}")
            print(f"    Initial rows: {len(df)}")
        
        # Note: Column names already standardized during individual table extraction
        
        # Step 1: Add grouping logic for when Item field resets to 1
        if 'Item' in df.columns:
            # Show sample of Item values for debugging
            if verbose:
                sample_items = df['Item'].dropna().head(5).tolist()
                print(f"    Sample Item values: {sample_items}")
            
            # Extract numeric part from Item column (handles cases like "P1", "P2", etc.)
            # This regex finds the first sequence of digits in each Item value
            df['Item_numeric'] = df['Item'].astype(str).str.extract(r'(\d+)', expand=False)
            df['Item_numeric'] = pd.to_numeric(df['Item_numeric'], errors='coerce')
            
            # Filter out rows where we couldn't extract a number
            valid_item_mask = df['Item_numeric'].notna()
            df = df[valid_item_mask]
            
            if verbose and not valid_item_mask.all():
                removed_non_numeric = (~valid_item_mask).sum()
                print(f"    Removed {removed_non_numeric} rows with non-numeric Item values")
            
            if len(df) > 0:
                # Show range of extracted numeric values
                if verbose:
                    min_item = df['Item_numeric'].min()
                    max_item = df['Item_numeric'].max()
                    print(f"    Extracted Item numbers range: {min_item} to {max_item}")
                
                # Create groups based on Item resets (vectorized approach)
                item_resets = (df['Item_numeric'] == 1) & (df['Item_numeric'].shift(1, fill_value=1) != 1)
                df['group_id'] = item_resets.cumsum() - 1  # Start from 0
                
                # Clean up temporary column
                df = df.drop('Item_numeric', axis=1)
                
                if verbose:
                    group_counts = df['group_id'].value_counts().sort_index()
                    print(f"    Created {len(group_counts)} groups based on Item field resets")
        
        # Step 2: Filter out unwanted rows
        initial_row_count = len(df)
        
        # Remove rows where 'Data Item' = 'Length of Record'
        if 'Data Item' in df.columns:
            mask = df['Data Item'] != 'Length of Record'
            df = df[mask]
            removed_length_records = initial_row_count - len(df)
            if verbose and removed_length_records > 0:
                print(f"    Removed {removed_length_records} 'Length of Record' rows")
        
        # Remove rows where all original schema fields are missing
        original_schema_columns = [col for col in df.columns 
                                if col not in ['File', 'Min_Date', 'Max_Date', 'group_id']]
        if original_schema_columns:
            mask = df[original_schema_columns].notna().any(axis=1)
            rows_before = len(df)
            df = df[mask]
            removed_empty_rows = rows_before - len(df)
            if verbose and removed_empty_rows > 0:
                print(f"    Removed {removed_empty_rows} rows with all original fields missing")
        
        # Step 3: Filter out groups without proper 'record type' references
        if 'group_id' in df.columns and len(df) > 0:
            groups_before = df['group_id'].nunique()
            
            # Check for 'record type' substring in 'Data Item' and 'Data Element' columns
            # but exclude rows where it's exactly "Record Type" with no additional code
            record_type_mask = pd.Series(False, index=df.index)
            
            for col in ['Data Item', 'Data Element']:
                if col in df.columns:
                    col_str = df[col].astype(str).str.lower()
                    # Find rows where column contains "record type" but is not exactly "record type"
                    contains_rt = col_str.str.contains('record type', na=False)
                    not_exact_rt = col_str.str.strip() != 'record type'
                    record_type_mask = record_type_mask | (contains_rt & not_exact_rt)
            
            # Check if ANY rows have record type references
            total_record_type_rows = record_type_mask.sum()
            
            if total_record_type_rows > 0:
                # PDF has record type references - apply filtering
                valid_groups = df[record_type_mask]['group_id'].unique()
                df = df[df['group_id'].isin(valid_groups)]
                
                groups_after = df['group_id'].nunique() if len(df) > 0 else 0
                groups_removed = groups_before - groups_after
                
                if verbose and groups_removed > 0:
                    print(f"    Removed {groups_removed} groups without proper 'record type' references (excluding exact 'Record Type' matches)")
            else:
                # PDF has no record type references - keep all groups (single record type PDF)
                if verbose:
                    print(f"    No 'record type' references found - keeping all {groups_before} groups (single record type PDF)")
        
        # Step 4: Add record types if requested (only for multi-record type PDFs)
        has_record_type_references = False
        if 'group_id' in df.columns and len(df) > 0:
            # Check if this PDF has record type references
            record_type_check_mask = pd.Series(False, index=df.index)
            for col in ['Data Item', 'Data Element']:
                if col in df.columns:
                    col_str = df[col].astype(str).str.lower()
                    contains_rt = col_str.str.contains('record type', na=False)
                    not_exact_rt = col_str.str.strip() != 'record type'
                    record_type_check_mask = record_type_check_mask | (contains_rt & not_exact_rt)
            has_record_type_references = record_type_check_mask.sum() > 0
        
        if add_record_types and not df.empty and has_record_type_references:
            # Only add record types for multi-record type PDFs
            df = self.add_record_type_column(df)
            
            if verbose and 'Record_Type' in df.columns:
                record_type_counts = df['Record_Type'].value_counts()
                print(f"    Extracted record types: {len(record_type_counts)} unique types")
        elif add_record_types and not df.empty and not has_record_type_references:
            if verbose:
                print(f"    Skipping record type extraction for single record type PDF")
        
        # Step 5: Validate and clean up group_id column
        if 'group_id' in df.columns and not df.empty:
            unique_groups = df['group_id'].nunique()
            
            if unique_groups == 1:
                # Only one group - group_id is redundant, drop it
                df = df.drop('group_id', axis=1)
                if verbose:
                    print(f"    Only one group found, dropped group_id column")
            elif 'Record_Type' in df.columns:
                # Multiple groups with record types - validate mapping
                record_type_to_groups = df.groupby('Record_Type')['group_id'].nunique()
                groups_to_record_types = df.groupby('group_id')['Record_Type'].nunique()
                
                # Validation: each record type should map to exactly one group, and vice versa
                max_groups_per_record_type = record_type_to_groups.max()
                max_record_types_per_group = groups_to_record_types.max()
                
                if max_groups_per_record_type == 1 and max_record_types_per_group == 1:
                    # Perfect 1:1 mapping - we can safely drop group_id
                    df = df.drop('group_id', axis=1)
                    if verbose:
                        print(f"    Validated 1:1 record_type↔group_id mapping, dropped group_id column")
                else:
                    # Keep group_id for debugging/analysis
                    if verbose:
                        print(f"    Complex record_type↔group_id mapping detected:")
                        print(f"      Max groups per record type: {max_groups_per_record_type}")
                        print(f"      Max record types per group: {max_record_types_per_group}")
                        print(f"      Keeping group_id column for analysis")
            else:
                # Multiple groups but no record types - keep group_id for structure
                if verbose:
                    print(f"    Multiple groups ({unique_groups}) but no record types - keeping group_id column")
        
        if verbose:
            print(f"    Final rows: {len(df)} (removed {initial_row_count - len(df)} total)")
        
        return df

    # Extract Tables from PDF
    def extract_tables_from_pdf(
        self, 
        pdf_path: str,
        prefix: str,
        save_document_data: bool = False,
        overwrite: bool = True,
        verbose: bool = False,
    ) :
        """
        Extract tables from PDF using PyMuPDF.
        """

        pdf_path = Path(pdf_path)
        
        if verbose:
            print(f"Processing PDF: {pdf_path.name}")

        # Early check: if we're saving data and file exists and not overwriting, skip processing
        if save_document_data:
            save_file = DICTIONARY_DIR / f'clean/{prefix}' / pdf_path.with_suffix('.csv').name
            if save_file.exists() and not overwrite:
                if verbose:
                    print(f"  Skipping - CSV already exists: {save_file.name}")
                return []  # Return empty list since we're not processing

        # PyMuPDF
        schema: List[pd.DataFrame] = []
        previous_page_has_item = False
        
        with pymupdf.open(pdf_path) as pdf:
            for page_num in range(len(pdf)):
                page = pdf[page_num]
                table_list = list(page.find_tables(vertical_strategy='lines_strict', horizontal_strategy='lines_strict'))
                
                for table in table_list:
                    df = table.to_pandas()

                    # Standardize column names immediately after extraction
                    df = self.standardize_column_names(df, verbose=False)
                    
                    # Track table types for reporting
                    has_item = "item" in df.columns.str.lower().values
                    if has_item:
                        table_columns = df.columns
                        previous_page_has_item = True
                        schema.append(df)
                    else:
                        if previous_page_has_item:
                            if (len(df.columns)==len(table_columns)):
                                df.loc[-1] = df.columns
                                df.index = df.index + 1
                                df = df.sort_index()
                                df.columns = table_columns
                                schema.append(df)
                            else:
                                previous_page_has_item = False
                                table_columns = []
                    
                    # KEEP ALL TABLES - don't filter by Item column presence
                    # This fixes the multi-page table issue where continuation tables lack headers
                    # NOTE: Eventually we need to implement custom logic to ensure that tables spanning multiple pages are combined appropriately
                    # schema.append(df)

        # Save Document Data to Separate CSV File
        if save_document_data and schema:
            df = pd.concat(schema, ignore_index=True)
            df['File'] = pdf_path.name
            min_date, max_date = self.extract_dates_between_parentheses(pdf_path)
            df['Min_Date'] = min_date
            df['Max_Date'] = max_date
            
            # Apply cleaning operations to the extracted data
            df = self.clean_extracted_dataframe(
                df, 
                filename=pdf_path.name,
                add_record_types=True,
                verbose=verbose
            )
            
            # Only save if we have data after cleaning
            if not df.empty:
                save_file = DICTIONARY_DIR / f'clean/{prefix}' / pdf_path.with_suffix('.csv').name
                
                # Create directory if it doesn't exist
                save_file.parent.mkdir(parents=True, exist_ok=True)
                df.to_csv(save_file, index=False)
                if verbose:
                    print(f"  Saved cleaned data to: {save_file.name}")
            else:
                if verbose:
                    print(f"  No data remaining after cleaning - skipping save")

    # Reconcile Schemas
    def reconcile_schemas(self, schema_list: List[Tuple[str, List[Dict[str, Any]]]]) -> Dict[Tuple[int, int], Dict[str, Any]]:
        """
        Reconcile schemas from different versions of the same document.
        """

        reconciled: Dict[Tuple[int, int], Dict[str, Any]] = defaultdict(dict)
        for version, schema in schema_list:
            for field in schema:
                key = (field['begin'], field['end'])  # or item name
                reconciled[key].update({
                    "field_name": field['field_name'],
                    f"version_{version}": True # type: ignore # Dynamic key name
                })
        return reconciled

    # Extract Schemas from PDFs to CSV Files
    def extract_schemas_to_csv(
        self, 
        prefix: str, 
        overwrite: bool = True,
        verbose: bool = False,
    ) :
        """
        Extract tables from PDF files and save as individual CSV files in the clean directory.
        """
        
        if verbose:
            print(f"\n=== Extracting schemas for prefix: {prefix} ===")

        # Get All Layout Files
        pdf_directory = DICTIONARY_DIR / f'raw/{prefix}'
        pdf_paths = list(pdf_directory.glob(f'{prefix}_*.pdf'))
        
        if verbose:
            print(f"Found {len(pdf_paths)} PDF files")
        
        # Process each PDF file
        for i, pdf_path in enumerate(pdf_paths, 1):
            if verbose:
                print(f"\n[{i}/{len(pdf_paths)}] Processing: {pdf_path.name}")
            
            # Extract tables and save to CSV
            self.extract_tables_from_pdf(
                pdf_path, 
                prefix, 
                save_document_data=True, 
                overwrite=overwrite, 
                verbose=verbose
            )

    # Standardize Column Names
    def standardize_column_names(
        self, 
        df: pd.DataFrame,
        verbose: bool = False,
    ) -> pd.DataFrame:
        """
        Standardize column names to handle inconsistencies.
        
        Fixes issues like:
        - "Max\nLength" → "Max Length"
        - Extra whitespace
        - Other formatting inconsistencies
        - Handles potential duplicate columns after standardization
        
        Args:
            df: DataFrame with potentially inconsistent column names
            verbose: Whether to print standardization messages
            
        Returns:
            DataFrame with standardized column names
        """
        if df.empty:
            return df
        
        # Create a copy to avoid modifying the original
        df_copy = df.copy()
        
        # Create mapping of old column names to new standardized names
        column_mapping = {}
        standardized_names = []
        
        for col in df_copy.columns:
            # Start with the original column name
            new_col = str(col)
            
            # Remove newlines and extra whitespace
            new_col = new_col.replace('\n', ' ').replace('\r', ' ')
            
            # Normalize multiple spaces to single space
            new_col = ' '.join(new_col.split())
            
            # Strip leading/trailing whitespace
            new_col = new_col.strip()
            
            # Handle potential duplicates by adding suffix
            original_new_col = new_col
            counter = 1
            while new_col in standardized_names:
                new_col = f"{original_new_col}_{counter}"
                counter += 1
            
            standardized_names.append(new_col)
            
            # Store mapping if different
            if new_col != col:
                column_mapping[col] = new_col
        
        # Apply the column name changes
        if column_mapping:
            df_copy = df_copy.rename(columns=column_mapping)
            
            # Log the changes if verbose mode is enabled
            if verbose:
                print(f"  Standardized {len(column_mapping)} column names:")
                for old_col, new_col in column_mapping.items():
                    print(f"    '{old_col}' → '{new_col}'")
        
        return df_copy

    # Combine Schemas from Clean CSV Files
    def combine_schemas(
        self, 
        prefix: str, 
        save_combined_schema: bool = False,
    ) -> pd.DataFrame:
        """
        Combine existing cleaned CSV files from the clean directory into a single DataFrame.
        
        Note: CSV files should already be cleaned by the extract_tables_from_pdf process.
        This function simply concatenates the pre-cleaned files.
        
        Args:
            prefix: File prefix to process
            verbose: Whether to print detailed output
            
        Returns:
            DataFrame with combined schema data
        """

        if self.config.verbose:
            print(f"\n=== Combining clean CSV files for prefix: {prefix} ===")

        # Get All Clean CSV Files
        csv_directory = DICTIONARY_DIR / f'clean/{prefix}'
        
        if not csv_directory.exists():
            if self.config.verbose:
                print(f"Clean directory does not exist: {csv_directory}")
            return pd.DataFrame()
        
        csv_paths = list(csv_directory.glob(f'{prefix}_*.csv'))
        
        if self.config.verbose:
            print(f"Found {len(csv_paths)} CSV files")
        
        if not csv_paths:
            if self.config.verbose:
                print(f"No CSV files found for prefix: {prefix}")
            return pd.DataFrame()
        
        # Read and combine all CSV files
        df_list = []
        for i, csv_path in enumerate(csv_paths, 1):
            if self.config.verbose:
                print(f"[{i}/{len(csv_paths)}] Reading: {csv_path.name}")
            
            try:
                df = pd.read_csv(csv_path)
                if not df.empty:
                    df_list.append(df)
                    if self.config.verbose:
                        print(f"  Added {len(df)} rows")
                else:
                    if self.config.verbose:
                        print(f"  File is empty")
            except Exception as e:
                if self.config.verbose:
                    print(f"  Error reading file: {e}")
        
        # Combine all results (CSV files are already cleaned individually)
        if df_list:
            combined_df = pd.concat(df_list, ignore_index=True)
            
            if self.config.verbose:
                print(f"\nCombined dataset: {len(combined_df)} total rows from {len(df_list)} files")
                
                # Show group distribution if available
                if 'group_id' in combined_df.columns:
                    group_counts = combined_df['group_id'].value_counts().sort_index()
                    print(f"Group distribution: {len(group_counts)} total groups")
                    # Show first few groups as sample
                    for group_id, count in list(group_counts.items())[:5]:
                        print(f"  Group {group_id}: {count} rows")
                    if len(group_counts) > 5:
                        print(f"  ... and {len(group_counts) - 5} more groups")
                
                # Show record type distribution if available
                if 'Record_Type' in combined_df.columns:
                    record_type_counts = combined_df['Record_Type'].value_counts()
                    print(f"\nRecord type distribution:")
                    for rt, count in record_type_counts.items():
                        print(f"  {rt}: {count} rows")
 
            # Save Combined DataFrame to CSV
            if save_combined_schema:
                combined_df.to_csv(DICTIONARY_DIR / f'combined/{prefix}_combined_schema.csv', index=False)

            return combined_df

        else:
            if self.config.verbose:
                print(f"\nNo data found for prefix: {prefix}")
            return pd.DataFrame()

    # Organize Schemas by Record Type
    def organize_schemas_by_record_type(self) :
        """
        Create subfolders for each Record_Type within data/clean/prefix folders.
        Only processes prefixes where the combined schema file has a Record_Type column.
        
        Args:
            verbose: Whether to print detailed output
        """
        
        if self.config.verbose:
            print(f"\n=== Organizing schemas by Record_Type ===")
        
        # Process each prefix
        for prefix in self.config.prefix_dict.keys():
            if self.config.verbose:
                print(f"\nProcessing prefix: {prefix}")
            
            # Check if combined schema file exists
            combined_file = DICTIONARY_DIR / f'combined/{prefix}_combined_schema.csv'
            if not combined_file.exists():
                if self.config.verbose:
                    print(f"  No combined schema file found: {combined_file.name}")
                continue
            
            try:
                # Load combined schema file
                df = pd.read_csv(combined_file)
                
                if df.empty:
                    if self.config.verbose:
                        print(f"  Combined schema file is empty")
                    continue
                
                # Check if Record_Type column exists
                if 'Record_Type' not in df.columns:
                    if self.config.verbose:
                        print(f"  No Record_Type column found - skipping organization")
                    continue
                
                # Get unique record types
                record_types = df['Record_Type'].dropna().unique()
                
                if len(record_types) == 0:
                    if self.config.verbose:
                        print(f"  No record types found in Record_Type column")
                    continue
                
                if self.config.verbose:
                    print(f"  Found {len(record_types)} record types: {list(record_types)}")
                
                # Create subfolders for each record type
                base_clean_dir = DATA_DIR / f'clean/{prefix}'
                
                for record_type in record_types:
                    record_type_dir = base_clean_dir / str(record_type)
                    record_type_dir.mkdir(parents=True, exist_ok=True)
                    
                    if self.config.verbose:
                        print(f"    Created folder: {record_type_dir.relative_to(Path('.'))}")
                
                if self.config.verbose:
                    print(f"  Success: Organized {prefix} by {len(record_types)} record types")
                    
            except Exception as e:
                if self.config.verbose:
                    print(f"  Error processing {prefix}: {e}")
        
        if self.config.verbose:
            print(f"\nCompleted schema organization by Record_Type")

    def analyze_temporal_coverage(self) -> pd.DataFrame:
        """
        Analyze temporal coverage across all combined schema files to identify monthly gaps.
        
        Returns:
            pd.DataFrame: Summary of coverage with gaps identified for each prefix
        """
        combined_dir = DICTIONARY_DIR / 'combined'
        
        if not combined_dir.exists():
            print(f"Combined schemas directory not found: {combined_dir}")
            return pd.DataFrame()
        
        # Find all combined schema files
        schema_files = list(combined_dir.glob('*_combined_schema.csv'))
        
        if not schema_files:
            print(f"No combined schema files found in {combined_dir}")
            return pd.DataFrame()
        
        coverage_results = []
        
        print(f"Analyzing temporal coverage for {len(schema_files)} prefixes...")
        
        for schema_file in schema_files:
            prefix = schema_file.stem.replace('_combined_schema', '')
            
            try:
                # Load the schema file
                df = pd.read_csv(schema_file)
                
                if 'Min_Date' not in df.columns or 'Max_Date' not in df.columns:
                    print(f"Warning: {prefix}: Missing date columns")
                    continue
                    
                # Convert date columns to datetime
                df['Min_Date'] = pd.to_datetime(df['Min_Date'])
                df['Max_Date'] = pd.to_datetime(df['Max_Date'])
                
                # Replace far future dates (2099) with current date for analysis
                current_date = pd.Timestamp.now().normalize()
                df['Max_Date_Analysis'] = df['Max_Date'].where(
                    df['Max_Date'] < pd.Timestamp('2099-01-01'), 
                    current_date
                )
                
                # Get unique date ranges from files
                date_ranges = df[['Min_Date', 'Max_Date_Analysis']].drop_duplicates().sort_values('Min_Date')
                
                if len(date_ranges) == 0:
                    continue
                    
                # Calculate overall coverage
                overall_min = date_ranges['Min_Date'].min()
                overall_max = date_ranges['Max_Date_Analysis'].max()
                
                # Generate expected monthly range
                expected_months = pd.date_range(
                    start=overall_min.replace(day=1), 
                    end=overall_max.replace(day=1), 
                    freq='MS'  # Month start
                )
                
                # Generate covered months from all date ranges
                covered_months = set()
                for _, row in date_ranges.iterrows():
                    range_months = pd.date_range(
                        start=row['Min_Date'].replace(day=1),
                        end=row['Max_Date_Analysis'].replace(day=1),
                        freq='MS'
                    )
                    covered_months.update(range_months)
                
                # Find gaps
                missing_months = []
                for month in expected_months:
                    if month not in covered_months:
                        missing_months.append(month)
                
                # Calculate gap details
                gap_periods = []
                if missing_months:
                    missing_months.sort()
                    gap_start = missing_months[0]
                    gap_end = missing_months[0]
                    
                    for i in range(1, len(missing_months)):
                        # Check if this month is consecutive to the previous
                        if missing_months[i] == missing_months[i-1] + pd.DateOffset(months=1):
                            gap_end = missing_months[i]
                        else:
                            # End current gap and start new one
                            gap_periods.append((gap_start, gap_end))
                            gap_start = missing_months[i]
                            gap_end = missing_months[i]
                    
                    # Add the last gap
                    gap_periods.append((gap_start, gap_end))
                
                # Create summary
                total_months_expected = len(expected_months)
                total_months_covered = len(covered_months)
                total_months_missing = len(missing_months)
                coverage_pct = (total_months_covered / total_months_expected) * 100 if total_months_expected > 0 else 0
                
                # Format gap periods for display
                gap_summary = []
                for gap_start, gap_end in gap_periods:
                    if gap_start == gap_end:
                        gap_summary.append(gap_start.strftime('%Y-%m'))
                    else:
                        gap_summary.append(f"{gap_start.strftime('%Y-%m')} to {gap_end.strftime('%Y-%m')}")
                
                coverage_results.append({
                    'Prefix': prefix,
                    'Files_Count': len(date_ranges),
                    'Coverage_Start': overall_min.strftime('%Y-%m-%d'),
                    'Coverage_End': overall_max.strftime('%Y-%m-%d'),
                    'Total_Months_Expected': total_months_expected,
                    'Total_Months_Covered': total_months_covered,
                    'Total_Months_Missing': total_months_missing,
                    'Coverage_Percentage': round(coverage_pct, 1),
                    'Gap_Periods': '; '.join(gap_summary) if gap_summary else 'None',
                    'Has_Gaps': len(missing_months) > 0
                })
                
                if self.config.verbose:
                    print(f"{prefix}:")
                    print(f"   Coverage: {overall_min.strftime('%Y-%m')} to {overall_max.strftime('%Y-%m')}")
                    print(f"   Files: {len(date_ranges)}")
                    print(f"   Coverage: {total_months_covered}/{total_months_expected} months ({coverage_pct:.1f}%)")
                    if gap_summary:
                        print(f"   Gaps: {'; '.join(gap_summary)}")
                    else:
                        print(f"Complete coverage")
                        
            except Exception as e:
                print(f"Error analyzing {prefix}: {str(e)}")
                continue
        
        # Convert to DataFrame and sort by coverage percentage
        results_df = pd.DataFrame(coverage_results)
        if not results_df.empty:
            results_df = results_df.sort_values(['Has_Gaps', 'Coverage_Percentage'], ascending=[True, False])
        
        return results_df

    def print_coverage_summary(self, coverage_df: pd.DataFrame) :
        """
        Print a formatted summary of temporal coverage analysis.
        """
        if coverage_df.empty:
            print("No coverage data to summarize")
            return
        
        total_prefixes = len(coverage_df)
        prefixes_with_gaps = coverage_df['Has_Gaps'].sum()
        prefixes_complete = total_prefixes - prefixes_with_gaps
        
        print(f"\n" + "="*80)
        print(f"TEMPORAL COVERAGE SUMMARY")
        print(f"="*80)
        print(f"Total Prefixes Analyzed: {total_prefixes}")
        print(f"Complete Coverage: {prefixes_complete}")
        print(f"With Gaps: {prefixes_with_gaps}")
        print(f"="*80)
        
        if prefixes_with_gaps > 0:
            print(f"PREFIXES WITH GAPS:")
            print(f"{'-'*80}")
            gap_df = coverage_df[coverage_df['Has_Gaps']].copy()
            for _, row in gap_df.iterrows():
                print(f"{row['Prefix']:<20} | {row['Coverage_Percentage']:>5.1f}% | {row['Gap_Periods']}")
        
        if prefixes_complete > 0:
            print(f"PREFIXES WITH COMPLETE COVERAGE:")
            print(f"{'-'*80}")
            complete_df = coverage_df[~coverage_df['Has_Gaps']].copy()
            for _, row in complete_df.iterrows():
                print(f"{row['Prefix']:<20} | {row['Coverage_Start']} to {row['Coverage_End']}")
        
        print(f"\n" + "="*80)

    def analyze_schema_formats(self) -> pd.DataFrame:
        """
        Analyze schema files to identify potential delimited vs fixed-width file formats
        based on column structure patterns.
        
        Returns:
            pd.DataFrame: Analysis of schema formats with file type predictions
        """
        combined_dir = DICTIONARY_DIR / 'combined'
        
        if not combined_dir.exists():
            print(f"Combined schemas directory not found: {combined_dir}")
            return pd.DataFrame()
        
        # Find all combined schema files
        schema_files = list(combined_dir.glob('*_combined_schema.csv'))
        
        if not schema_files:
            print(f"No combined schema files found in {combined_dir}")
            return pd.DataFrame()
        
        format_results = []
        
        print(f"Analyzing schema formats for {len(schema_files)} prefixes...")
        
        for schema_file in schema_files:
            prefix = schema_file.stem.replace('_combined_schema', '')
            
            try:
                # Read just the header to analyze column structure
                df_header = pd.read_csv(schema_file, nrows=0)  # Just get column names
                columns = list(df_header.columns)
                
                # Analyze column patterns
                has_max_length = 'Max Length' in columns
                has_length = 'Length' in columns
                has_begin_end = 'Begin' in columns and 'End' in columns
                has_format = 'Format' in columns
                has_description = 'Description' in columns
                has_definition = 'Definition' in columns
                has_remarks = 'Remarks' in columns
                has_data_element = 'Data Element' in columns
                has_data_item = 'Data Item' in columns
                
                # Read a few rows to check for additional patterns
                df_sample = pd.read_csv(schema_file, nrows=10)
                
                # Count total columns
                total_columns = len(columns)
                
                # Predict file format based on patterns
                format_indicators = []
                confidence_score = 0
                
                if has_max_length:
                    format_indicators.append("Max Length column")
                    confidence_score += 3
                
                if has_begin_end:
                    format_indicators.append("Begin/End columns")  
                    confidence_score -= 3  # This suggests fixed-width
                
                if has_format:
                    format_indicators.append("Format column")
                    confidence_score += 2
                
                if has_description and has_definition:
                    format_indicators.append("Description/Definition columns")
                    confidence_score += 1
                
                if has_remarks and not (has_description or has_definition):
                    format_indicators.append("Remarks column only")
                    confidence_score -= 1  # Suggests older/simpler fixed-width format
                
                if has_data_element:
                    format_indicators.append("Data Element naming")
                    confidence_score += 1
                elif has_data_item:
                    format_indicators.append("Data Item naming")
                    confidence_score -= 1
                
                # Make prediction
                if confidence_score >= 3:
                    predicted_format = "DELIMITED"
                elif confidence_score <= -2:
                    predicted_format = "FIXED-WIDTH"
                else:
                    predicted_format = "UNCERTAIN"
                
                # Check for any explicit delimiter indicators in the data
                delimiter_hints = []
                if not df_sample.empty:
                    for col in ['Format', 'Description', 'Definition', 'Remarks']:
                        if col in df_sample.columns:
                            sample_text = ' '.join(df_sample[col].dropna().astype(str).head(10))
                            if 'delimit' in sample_text.lower():
                                delimiter_hints.append(f"'{col}' mentions delimiter")
                            if 'fixed' in sample_text.lower() and 'width' in sample_text.lower():
                                delimiter_hints.append(f"'{col}' mentions fixed width")
                            if 'pipe' in sample_text.lower() or '|' in sample_text:
                                delimiter_hints.append(f"'{col}' mentions pipe delimiter")
                            if 'comma' in sample_text.lower() and 'separat' in sample_text.lower():
                                delimiter_hints.append(f"'{col}' mentions comma separator")
                
                format_results.append({
                    'Prefix': prefix,
                    'Total_Columns': total_columns,
                    'Has_Max_Length': has_max_length,
                    'Has_Length': has_length,
                    'Has_Begin_End': has_begin_end,
                    'Has_Format': has_format,
                    'Has_Description': has_description,
                    'Has_Definition': has_definition,
                    'Has_Remarks': has_remarks,
                    'Has_Data_Element': has_data_element,
                    'Has_Data_Item': has_data_item,
                    'Confidence_Score': confidence_score,
                    'Predicted_Format': predicted_format,
                    'Format_Indicators': '; '.join(format_indicators),
                    'Delimiter_Hints': '; '.join(delimiter_hints) if delimiter_hints else 'None',
                    'Column_Structure': ', '.join(columns[:6])  # First 6 columns for reference
                })
                
                if self.config.verbose:
                    print(f"{prefix}:")
                    print(f"   Predicted Format: {predicted_format} (score: {confidence_score})")
                    print(f"   Key Indicators: {'; '.join(format_indicators)}")
                    if delimiter_hints:
                        print(f"   Delimiter Hints: {'; '.join(delimiter_hints)}")
                        
            except Exception as e:
                print(f"Error analyzing {prefix}: {str(e)}")
                continue
        
        # Convert to DataFrame and sort by prediction confidence
        results_df = pd.DataFrame(format_results)
        if not results_df.empty:
            results_df = results_df.sort_values(['Predicted_Format', 'Confidence_Score'], 
                                            ascending=[True, False])
        
        return results_df

    def print_format_analysis_summary(self, format_df: pd.DataFrame) :
        """
        Print a formatted summary of schema format analysis.
        """
        if format_df.empty:
            print("No format analysis data to summarize")
            return
        
        total_prefixes = len(format_df)
        delimited_count = (format_df['Predicted_Format'] == 'DELIMITED').sum()
        fixed_width_count = (format_df['Predicted_Format'] == 'FIXED-WIDTH').sum()
        uncertain_count = (format_df['Predicted_Format'] == 'UNCERTAIN').sum()
        
        print(f"\n" + "="*80)
        print(f"SCHEMA FORMAT ANALYSIS SUMMARY")
        print(f"="*80)
        print(f"Total Prefixes Analyzed: {total_prefixes}")
        print(f"Predicted Delimited: {delimited_count}")
        print(f"Predicted Fixed-Width: {fixed_width_count}")
        print(f"Uncertain: {uncertain_count}")
        print(f"="*80)
        
        if delimited_count > 0:
            print(f"LIKELY DELIMITED FILES:")
            print(f"{'-'*80}")
            delimited_df = format_df[format_df['Predicted_Format'] == 'DELIMITED'].copy()
            for _, row in delimited_df.iterrows():
                print(f"{row['Prefix']:<20} | Score: {row['Confidence_Score']:>2} | {row['Format_Indicators']}")
        
        if fixed_width_count > 0:
            print(f"LIKELY FIXED-WIDTH FILES:")
            print(f"{'-'*80}")
            fixed_df = format_df[format_df['Predicted_Format'] == 'FIXED-WIDTH'].copy()
            for _, row in fixed_df.iterrows():
                print(f"{row['Prefix']:<20} | Score: {row['Confidence_Score']:>2} | {row['Format_Indicators']}")
        
        if uncertain_count > 0:
            print(f"UNCERTAIN FORMAT:")
            print(f"{'-'*80}")
            uncertain_df = format_df[format_df['Predicted_Format'] == 'UNCERTAIN'].copy()
            for _, row in uncertain_df.iterrows():
                print(f"{row['Prefix']:<20} | Score: {row['Confidence_Score']:>2} | {row['Format_Indicators']}")
        
        print(f"\n" + "="*80)

    def count_field_names_from_clean_schemas(
        self,
        clean_dir: str | Path = 'dictionary_files/clean', 
        output_dir: str | Path = 'output',
    ):
        """
        Extract and count all Data Item and Data Element values from individual CSV schemas.
        
        Args:
            clean_dir: Directory containing cleaned schema CSV files (default: 'dictionary_files/clean')
            output_dir: Directory to save results (default: 'output')
            
        Returns:
            pd.DataFrame: Field name counts sorted by frequency
            
        Saves results to {output_dir}/field_name_counts.csv.
        """
        clean_dir = Path(clean_dir)
        output_dir = Path(output_dir)
        all_field_names = []
        
        # Find all CSV files in clean directory and subdirectories
        csv_files = list(clean_dir.rglob('*.csv'))
        
        for csv_file in csv_files:
            try:
                df = pd.read_csv(csv_file)
                
                # Extract Data Item values
                if 'Data Item' in df.columns:
                    data_items = df['Data Item'].dropna().astype(str)
                    all_field_names.extend([item.strip() for item in data_items if item and item != 'nan'])
                
                # Extract Data Element values
                if 'Data Element' in df.columns:
                    data_elements = df['Data Element'].dropna().astype(str)
                    all_field_names.extend([element.strip() for element in data_elements if element and element != 'nan'])
                    
            except Exception:
                continue
        
        # Count occurrences
        field_counts = {}
        for name in all_field_names:
            field_counts[name] = field_counts.get(name, 0) + 1
        
        # Convert to DataFrame and save
        results_df = pd.DataFrame([
            {'Field_Name': name, 'Count': count} 
            for name, count in field_counts.items()
        ]).sort_values('Count', ascending=False)
        
        # Save to output directory
        output_dir.mkdir(parents=True, exist_ok=True)
        results_df.to_csv(output_dir / 'field_name_counts.csv', index=False)
        
        return results_df

    def standardize_field_names(
        self,
        input_dir: str | Path = 'output',
        output_dir: str | Path = 'output',
    ):
        """
        Standardize field names by removing newlines and parenthetical text.
        
        Args:
            input_dir: Directory containing field_name_counts.csv (default: 'output')
            output_dir: Directory to save standardized results (default: 'output')
            
        Returns:
            pd.DataFrame: Standardized field names with change tracking
            
        Reads from {input_dir}/field_name_counts.csv and saves to {output_dir}/field_name_counts_w_new.csv
        """
        # Read the field name counts from input directory
        input_dir = Path(input_dir)
        output_dir = Path(output_dir)
        df = pd.read_csv(input_dir / 'field_name_counts.csv')
        
        # Apply standardization rules
        df['Standardized_Field_Name'] = df['Field_Name'].apply(standardize_single_name)
        
        # Create aggregated counts for standardized names
        standardized_counts = df.groupby('Standardized_Field_Name')['Count'].sum().reset_index()
        standardized_counts.columns = ['Standardized_Field_Name', 'Standardized_Count']
        
        # Merge back to original data
        result_df = df.merge(standardized_counts, on='Standardized_Field_Name')
        
        # Add changed flag
        result_df['Changed'] = result_df['Field_Name'] != result_df['Standardized_Field_Name']
        
        # Reorder columns
        result_df = result_df[['Field_Name', 'Count', 'Standardized_Field_Name', 'Standardized_Count', 'Changed']]
        
        # Sort by original count descending
        result_df = result_df.sort_values('Count', ascending=False)
        
        # Save results to output directory
        output_dir.mkdir(parents=True, exist_ok=True)
        result_df.to_csv(output_dir / 'field_name_counts_w_new.csv', index=False)
        
        return result_df

    def standardize_single_name(self, name):
        """
        Apply standardization rules to a single field name.
        """
        if not isinstance(name, str):
            return str(name)
        
        # Step 1: Remove newline characters
        standardized = name.replace('\n', ' ').replace('\r', ' ')
        
        # Step 2: Handle "Filler" special case - replace anything with "filler" to just "Filler"
        if 'filler' in standardized.lower():
            standardized = 'Filler'
            return standardized
        
        # Step 3: Handle "Record Type" special case - any field starting with "Record Type" becomes "Record Type"
        if standardized.startswith('Record Type'):
            standardized = 'Record Type'
            return standardized
        
        # Step 4: Handle specific parentheses cases before general removal
        standardized = standardized.replace('WAGM (ARM pools only)', 'WAGM_ARM')
        standardized = standardized.replace('WAGM (AR pools only)', 'WAGM_AR')
        standardized = standardized.replace('Issuer Name (Long Name)', 'Issuer_Name_Long')
        standardized = standardized.replace('Issuer Name (Short Name)', 'Issuer_Name_Short')
        
        # Step 5: Remove text within parentheses (including the parentheses)
        standardized = re.sub(r'\([^)]*\)', '', standardized)
        
        # Step 6: Replace special characters
        standardized = standardized.replace('%', 'Percent')
        standardized = standardized.replace('/', '_or_')
        standardized = standardized.replace('=', '_equals_')
        standardized = standardized.replace('-', '_')
        standardized = standardized.replace('–', '_')  # em dash
        standardized = standardized.replace('+', '_Plus')
        standardized = standardized.replace('*', '')  # Remove asterisks
        standardized = standardized.replace(',', '')  # Remove commas
        standardized = standardized.replace("'", '')  # Remove apostrophes
        standardized = standardized.replace('"', '')  # Remove quotes
        standardized = standardized.replace('"', '')  # Remove smart quotes

        # Step 7: Replace spaces with underscores
        standardized = standardized.replace(' ', '_')
        
        # Step 8: Handle fields that start with a number
        if standardized and standardized[0].isdigit():
            standardized = f"Field_{standardized}"
        
        # Clean up multiple underscores and trim
        standardized = re.sub(r'_+', '_', standardized)  # Remove multiple underscores
        standardized = standardized.strip('_')  # Remove leading/trailing underscores
        
        return standardized

    def parse_cobol_format(self, cobol_notation):
        """
        Parse COBOL format notation into Python data types and processing instructions.
        
        Args:
            cobol_notation (str): COBOL format string like 'X(9)', '9(6)', '9(13)v9(2)'
            
        Returns:
            dict: Contains 'python_type', 'pandas_dtype', 'polars_dtype', 'decimal_places', 'total_length', 'validation_pattern'
            
        Examples:
            >>> parse_cobol_format('X(9)')
            {'python_type': str, 'pandas_dtype': 'string', 'polars_dtype': 'pl.Utf8', 'decimal_places': 0, 'total_length': 9, 'validation_pattern': r'^.{9}$'}
            
            >>> parse_cobol_format('9(6)')
            {'python_type': int, 'pandas_dtype': 'Int64', 'polars_dtype': 'pl.Int64', 'decimal_places': 0, 'total_length': 6, 'validation_pattern': r'^\\d{6}$'}
            
            >>> parse_cobol_format('9(13)v9(2)')
            {'python_type': float, 'pandas_dtype': 'float64', 'polars_dtype': 'pl.Float64', 'decimal_places': 2, 'total_length': 15, 'validation_pattern': r'^\\d{15}$'}
        """
        if not isinstance(cobol_notation, str):
            return None
        
        cobol_notation = cobol_notation.strip()
        
        # Handle alphanumeric fields: X(n)
        x_match = re.match(r'^X\((\d+)\)$', cobol_notation)
        if x_match:
            length = int(x_match.group(1))
            result = {
                'python_type': str,
                'pandas_dtype': 'string',
                'decimal_places': 0,
                'total_length': length,
                'validation_pattern': f'^.{{{length}}}$',
                'cobol_type': 'alphanumeric'
            }
            # Add Polars dtype if available
            if POLARS_AVAILABLE:
                result['polars_dtype'] = pl.Utf8
            else:
                result['polars_dtype'] = 'pl.Utf8'  # String representation for reference
            return result
        
        # Handle numeric fields with implied decimal: 9(m)v9(n)
        decimal_match = re.match(r'^9\((\d+)\)v9\((\d+)\)$', cobol_notation)
        if decimal_match:
            before_decimal = int(decimal_match.group(1))
            after_decimal = int(decimal_match.group(2))
            total_length = before_decimal + after_decimal
            result = {
                'python_type': float,
                'pandas_dtype': 'float64',
                'decimal_places': after_decimal,
                'total_length': total_length,
                'validation_pattern': f'^\\d{{{total_length}}}$',
                'cobol_type': 'numeric_decimal'
            }
            # Add Polars dtype if available
            result['polars_dtype'] = pl.Float64
            return result
        
        # Handle integer numeric fields: 9(n)
        int_match = re.match(r'^9\((\d+)\)$', cobol_notation)
        if int_match:
            length = int(int_match.group(1))
            result = {
                'python_type': int,
                'pandas_dtype': 'Int64',  # Nullable integer
                'decimal_places': 0,
                'total_length': length,
                'validation_pattern': f'^\\d{{{length}}}$',
                'cobol_type': 'numeric_integer'
            }
            result['polars_dtype'] = pl.Int64
            return result
        
        # If no pattern matches, return None
        return None

    def convert_cobol_value(self, raw_value, cobol_format_info):
        """
        Convert a raw string value from a fixed-width file using COBOL format information.
        
        Args:
            raw_value (str): Raw string value from file
            cobol_format_info (dict): Output from parse_cobol_format()
            
        Returns:
            Converted value in appropriate Python type, or None if conversion fails
            
        Examples:
            >>> info = parse_cobol_format('9(13)v9(2)')
            >>> convert_cobol_value('000012345000', info)
            123450.0
            
            >>> info = parse_cobol_format('X(9)')
            >>> convert_cobol_value('ABC123   ', info)
            'ABC123   '
            
            >>> info = parse_cobol_format('9(6)')
            >>> convert_cobol_value('202401', info)
            202401
        """
        if not cobol_format_info or not isinstance(raw_value, str):
            return raw_value
        
        try:
            # Handle alphanumeric fields - return as-is (but could trim if needed)
            if cobol_format_info['cobol_type'] == 'alphanumeric':
                return raw_value
            
            # Handle numeric fields
            elif cobol_format_info['cobol_type'] == 'numeric_integer':
                # Convert to integer, handling leading zeros
                return int(raw_value)
            
            # Handle decimal fields with implied decimal point
            elif cobol_format_info['cobol_type'] == 'numeric_decimal':
                decimal_places = cobol_format_info['decimal_places']
                # Convert to integer first, then divide by 10^decimal_places
                int_value = int(raw_value)
                float_value = int_value / (10 ** decimal_places)
                return float_value
            
            else:
                return raw_value
                
        except (ValueError, TypeError):
            return None

    def validate_cobol_value(self, raw_value, cobol_format_info):
        """
        Validate a raw string value against COBOL format requirements.
        
        Args:
            raw_value (str): Raw string value from file
            cobol_format_info (dict): Output from parse_cobol_format()
            
        Returns:
            dict: Contains 'is_valid' (bool) and 'error_message' (str)
            
        Examples:
            >>> info = parse_cobol_format('9(6)')
            >>> validate_cobol_value('202401', info)
            {'is_valid': True, 'error_message': None}
            
            >>> validate_cobol_value('20240A', info)
            {'is_valid': False, 'error_message': 'Value contains non-numeric characters'}
        """
        if not cobol_format_info:
            return {'is_valid': False, 'error_message': 'No format information provided'}
        
        if not isinstance(raw_value, str):
            return {'is_valid': False, 'error_message': 'Value must be a string'}
        
        # Check length
        expected_length = cobol_format_info['total_length']
        if len(raw_value) != expected_length:
            return {
                'is_valid': False, 
                'error_message': f'Expected length {expected_length}, got {len(raw_value)}'
            }
        
        # Check pattern
        pattern = cobol_format_info['validation_pattern']
        if not re.match(pattern, raw_value):
            cobol_type = cobol_format_info['cobol_type']
            if 'numeric' in cobol_type:
                return {'is_valid': False, 'error_message': 'Value contains non-numeric characters'}
            else:
                return {'is_valid': False, 'error_message': 'Value does not match expected pattern'}
        
        return {'is_valid': True, 'error_message': None}

    def create_pandas_dtype_mapping(self, schema_df, remarks_column='Remarks'):
        """
        Create a pandas dtype mapping from a schema DataFrame containing COBOL format notation.
        
        Args:
            schema_df (pd.DataFrame): Schema with COBOL format notation in remarks column
            remarks_column (str): Name of column containing COBOL notation
            
        Returns:
            dict: Mapping of field names to pandas dtypes
            
        Example:
            >>> schema = pd.DataFrame({
            ...     'Data Item': ['Pool Number', 'Issue Date', 'Interest Rate'],
            ...     'Remarks': ['X(6)', '9(8)', '9(2)v9(3)']
            ... })
            >>> create_pandas_dtype_mapping(schema)
            {'Pool Number': 'string', 'Issue Date': 'Int64', 'Interest Rate': 'float64'}
        """
        dtype_mapping = {}
        
        for _, row in schema_df.iterrows():
            field_name = row.get('Data Item') or row.get('Data Element')
            if not field_name or pd.isna(field_name):
                continue
                
            cobol_notation = row.get(remarks_column)
            if not cobol_notation or pd.isna(cobol_notation):
                continue
                
            format_info = parse_cobol_format(cobol_notation)
            if format_info:
                dtype_mapping[field_name] = format_info['pandas_dtype']
        
        return dtype_mapping

    def create_polars_dtype_mapping(self, schema_df, remarks_column='Remarks'):
        """
        Create a Polars dtype mapping from a schema DataFrame containing COBOL format notation.
        
        Args:
            schema_df (pd.DataFrame): Schema with COBOL format notation in remarks column
            remarks_column (str): Name of column containing COBOL notation
            
        Returns:
            dict: Mapping of field names to Polars dtypes
            
        Example:
            >>> schema = pd.DataFrame({
            ...     'Data Item': ['Pool Number', 'Issue Date', 'Interest Rate'],
            ...     'Remarks': ['X(6)', '9(8)', '9(2)v9(3)']
            ... })
            >>> create_polars_dtype_mapping(schema)
            {'Pool Number': pl.Utf8, 'Issue Date': pl.Int64, 'Interest Rate': pl.Float64}
        
        Note:
            Requires Polars to be installed. If Polars is not available, returns string representations.
        """
        
        dtype_mapping = {}
        
        for _, row in schema_df.iterrows():
            field_name = row.get('Data Item') or row.get('Data Element')
            if not field_name or pd.isna(field_name):
                continue
                
            cobol_notation = row.get(remarks_column)
            if not cobol_notation or pd.isna(cobol_notation):
                continue
                
            format_info = parse_cobol_format(cobol_notation)
            if format_info:
                dtype_mapping[field_name] = format_info['polars_dtype']
        
        return dtype_mapping

    def create_polars_schema(self, schema_df, remarks_column='Remarks'):
        """
        Create a Polars schema dictionary from a schema DataFrame containing COBOL format notation.
        This is the preferred format for Polars DataFrame creation with explicit schemas.
        
        Args:
            schema_df (pd.DataFrame): Schema with COBOL format notation in remarks column
            remarks_column (str): Name of column containing COBOL notation
            
        Returns:
            dict: Polars schema mapping {column_name: polars_dtype}
            
        Example:
            >>> schema = pd.DataFrame({
            ...     'Data Item': ['Pool Number', 'Issue Date', 'Interest Rate'],
            ...     'Remarks': ['X(6)', '9(8)', '9(2)v9(3)']
            ... })
            >>> polars_schema = create_polars_schema(schema)
            >>> df = pl.DataFrame(data, schema=polars_schema)
        
        Note:
            Requires Polars to be installed.
        """
        return create_polars_dtype_mapping(schema_df, remarks_column)


# ==========================================
# PIPELINE ORCHESTRATOR CLASS
# ==========================================

class GNMAPipeline:
    """
    Complete GNMA data processing pipeline orchestrator.
    
    This class coordinates the entire workflow:
    1. Download raw data files
    2. Format ZIP files to Parquet  
    3. Parse with schema detection
    4. Process into structured datasets
    """
    
    def __init__(
        self,
        download_config: DownloadConfig,
        formatter_config: FormatterConfig,
        processor_config: ProcessorConfig,
        verbose: bool = True
    ):
        """
        Initialize the complete pipeline.
        
        Args:
            download_config: Configuration for downloader
            formatter_config: Configuration for formatter
            processor_config: Configuration for processor
            verbose: Whether to show detailed progress
        """
        self.verbose = verbose
        self._setup_logging()
        
        # Initialize all components
        self.downloader = GNMAHistoricalDownloader(download_config)
        self.formatter = GNMADataFormatter(formatter_config)
        self.parser = GNMADataParser(verbose=False)
        self.processor = GNMADataProcessor(processor_config)
        
        self.logger.info("GNMA Pipeline initialized with all components")
    
    def _setup_logging(self) -> None:
        """Set up pipeline logging."""
        logging.basicConfig(
            level=logging.INFO if self.verbose else logging.WARNING,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
    
    def process_complete_workflow(
        self,
        prefix: str,
        download: bool = True,
        format_data: bool = True,
        process_data: bool = True
    ) -> Dict[str, Any]:
        """
        Execute complete workflow for a prefix.
        
        Args:
            prefix: Data prefix to process
            download: Whether to download data
            format_data: Whether to format data
            process_data: Whether to process data
            
        Returns:
            Dict: Results from each step
        """
        results = {'prefix': prefix}
        
        self.logger.info(f"Starting complete workflow for: {prefix}")
        
        # Step 1: Download
        if download:
            self.logger.info(f"Step 1: Downloading data for {prefix}")
            try:
                download_result = self.downloader.download_prefix_data(prefix)
                results['download'] = {
                    'successful': download_result[0],
                    'total': download_result[1],
                    'status': 'completed'
                }
            except Exception as e:
                self.logger.error(f"Download failed for {prefix}: {e}")
                results['download'] = {'status': 'failed', 'error': str(e)}
                return results
        
        # Step 2: Format
        if format_data:
            self.logger.info(f"Step 2: Formatting data for {prefix}")
            try:
                format_result = self.formatter.process_prefix_data(prefix)
                results['format'] = {
                    'successful': format_result.successful,
                    'total': format_result.total,
                    'status': 'completed'
                }
            except Exception as e:
                self.logger.error(f"Formatting failed for {prefix}: {e}")
                results['format'] = {'status': 'failed', 'error': str(e)}
                return results
        
        # Step 3: Process
        if process_data:
            self.logger.info(f"Step 3: Processing data for {prefix}")
            try:
                process_result = self.processor.process_prefix_data(prefix)
                results['process'] = {
                    'successful': process_result.successful,
                    'total_records': process_result.total_records_processed,
                    'records_by_type': process_result.records_by_type,
                    'status': 'completed'
                }
            except Exception as e:
                self.logger.error(f"Processing failed for {prefix}: {e}")
                results['process'] = {'status': 'failed', 'error': str(e)}
        
        self.logger.info(f"Completed workflow for {prefix}")
        return results
    
    def get_pipeline_status(self) -> Dict[str, str]:
        """Get status of all pipeline components."""
        return {
            'downloader': 'ready',
            'formatter': 'ready', 
            'parser': 'ready',
            'processor': 'ready',
            'pipeline': 'ready'
        }


# ==========================================
# CONVENIENCE FUNCTIONS
# ==========================================

def create_default_configs(
    email_value: str,
    id_value: str,
    user_agent: str,
    base_data_folder: str = "data",
    base_schema_folder: str = "dictionary_files"
) -> Tuple[DownloadConfig, FormatterConfig, ProcessorConfig]:
    """
    Create default configurations for all pipeline components.
    
    Args:
        email_value: Email for GNMA authentication
        id_value: ID for GNMA authentication  
        user_agent: User agent string
        base_data_folder: Base folder for data files
        base_schema_folder: Base folder for schema files
        
    Returns:
        Tuple of (DownloadConfig, FormatterConfig, ProcessorConfig)
    """
    download_config = DownloadConfig(
        email_value=email_value,
        id_value=id_value,
        user_agent=user_agent,
        download_folder=base_data_folder,
        schema_download_folder=base_schema_folder
    )
    
    formatter_config = FormatterConfig(
        input_folder=f"{base_data_folder}/raw",
        output_folder=f"{base_data_folder}/raw"
    )
    
    processor_config = ProcessorConfig(
        raw_folder=f"{base_data_folder}/raw",
        clean_folder=f"{base_data_folder}/clean",
        schema_folder=f"{base_schema_folder}/combined"
    )
    
    return download_config, formatter_config, processor_config


def create_pipeline_from_env() -> GNMAPipeline:
    """
    Create a complete pipeline using environment variables.
    
    Requires these environment variables:
    - email_value: GNMA email
    - id_value: GNMA ID
    - user_agent: User agent string
    
    Returns:
        Configured GNMAPipeline instance
    """
    load_dotenv()
    
    required_vars = ['email_value', 'id_value', 'user_agent']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        raise ValueError(f"Missing required environment variables: {missing_vars}")
    
    download_config, formatter_config, processor_config = create_default_configs(
        email_value=os.getenv('email_value'),
        id_value=os.getenv('id_value'),
        user_agent=os.getenv('user_agent')
    )
    
    return GNMAPipeline(download_config, formatter_config, processor_config)


# ==========================================
# MAIN EXECUTION
# ==========================================

def main():
    """Main execution function for pipeline testing."""
    print("GNMA Data Pipeline - Combined Module")
    print("="*50)
    
    # Example usage
    try:
        pipeline = create_pipeline_from_env()
        status = pipeline.get_pipeline_status()
        
        print("Pipeline Status:")
        for component, state in status.items():
            print(f"  {component}: {state}")
            
        print("\nPipeline ready for use!")
        print("Example usage:")
        print("  result = pipeline.process_complete_workflow('dailyllmni')")
        
    except Exception as e:
        print(f"Error initializing pipeline: {e}")
        print("Make sure environment variables are set:")
        print("  - email_value")
        print("  - id_value") 
        print("  - user_agent")


if __name__ == "__main__":
    main()