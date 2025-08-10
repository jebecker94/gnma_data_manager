#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
GNMA Data Manager - Configuration Options

This module contains all configuration dataclasses for the GNMA data processing pipeline.
Renamed to config_options.py to avoid conflicts with the existing config.py file.
Separated from main pipeline for better organization and easier testing.

Classes:
    - DownloadConfig: Configuration for GNMAHistoricalDownloader
    - FormatterConfig: Configuration for GNMADataFormatter  
    - ProcessorConfig: Configuration for GNMADataProcessor
    - SchemaReaderConfig: Configuration for GNMASchemaReader

Author: Jonathan E. Becker
"""

from pathlib import Path
from typing import List, Union
from dataclasses import dataclass, field

# ==========================================
# CONFIGURATION CLASSES
# ==========================================

@dataclass
class DownloadConfig:
    """Configuration class for the GNMA downloader."""
    # GNMA credentials and Page Settings
    email_value: str
    id_value: str
    user_agent: str
    base_url: str = "https://bulk.ginniemae.gov/protectedfiledownload.aspx?dlfile=data_history_cons"
    cookie_name: str = "GMProfileInfo"
    cookie_domain: str = "ginniemae.gov"
    cookie_path: str = "/"
    request_delay: float = 2.0
    cookie_expiry_days: int = 365
    schema_base_url: str = "https://www.ginniemae.gov/data_and_reports/disclosure_data/pages/disclosurehistoryfiles.aspx"

    # Folder settings
    data_download_folder: Union[str, Path] = 'data' # For data files (e.g., "data")
    schema_download_folder: Union[str, Path] = 'dictionary_files'  # For schema/PDF files
    dictionary_file: Union[str, Path] = "prefix_dictionary.yaml"
    use_prefix_subfolders: bool = True
    create_prefix_folders: bool = True

    # Schema/PDF download settings
    bad_text_filters: List[str] = None

    # Request/Retry/Timeout settings
    request_timeout_s: int = 30
    retry_total: int = 3
    retry_backoff: float = 1.0
    retry_statuses: List[int] = field(default_factory=lambda: [429, 500, 502, 503, 504])
    retry_allowed_methods: List[str] = field(default_factory=lambda: ["GET"]) 

    # Download streaming settings
    stream_downloads: bool = False
    stream_chunk_size: int = 1048576  # 1 MiB

    # Early exit for consecutive misses settings
    consecutive_miss_exit_threshold: int | None = None

    # Logging and behavior settings
    log_level: str = "INFO"
    logs_folder: Path = Path("logs")
    require_link_on_page: bool = True


@dataclass
class SchemaReaderConfig:
    """Configuration class for the GNMA schema reader."""
    input_folder: Union[str, Path] = "data/raw"
    output_folder: Union[str, Path] = "data/raw"  # Same folder by default
    text_column_name: str = "text_content"
    dictionary_file: Union[str, Path] = "prefix_dictionary.yaml"
    dictionary_folder: Path = Path("dictionary_files")
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
    logs_folder: Path = Path('./logs')
    # Heuristic flags
    merge_continuations: bool = True
    apply_item_grouping: bool = True
    extract_record_types: bool = True

    # Analysis settings
    save_analysis: bool = False
    analysis_folder: Path = Path('analysis')


@dataclass
class ProcessorConfig:
    """Configuration class for the GNMA data processor (now includes staging and parser settings)."""
    # Core folders
    raw_folder: Union[str, Path] = "data/raw"
    clean_folder: Union[str, Path] = "data/clean"
    schema_folder: Union[str, Path] = "dictionary_files/combined"
    dictionary_folder: Union[str, Path] = Path("dictionary_files")
    dictionary_file: Union[str, Path] = "prefix_dictionary.yaml"

    # General behavior
    skip_existing: bool = True
    log_level: str = "INFO"
    batch_size: int = 50
    validate_outputs: bool = True
    create_directories: bool = True
    logs_folder: Path = Path("logs")
    verbose: bool = False

    # Parsing/format detection
    default_delimiter: str = "|"
    date_format: str = "%Y%m"
    fallback_to_manual_processing: bool = True

    # Staging (former formatter) settings
    text_column_name: str = "text_content"
    encoding: str = "utf-8"
    fallback_encoding: str = "iso-8859-1"
    supported_extensions: List[str] = field(default_factory=lambda: ['.txt', '.dat', '.csv'])
    chunk_size: int = 100000  # Number of lines to process at once during file conversion
