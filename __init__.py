#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
GNMA Data Manager - Complete Data Processing Pipeline

This package provides a complete suite of tools for downloading, formatting, parsing,
and processing GNMA (Government National Mortgage Association) data files.

The pipeline consists of modular components that can be used independently or together:

Components:
    - GNMAHistoricalDownloader: Download historical data and schema files
    - GNMADataFormatter: Convert ZIP files to Parquet format
    - GNMADataParser: Intelligent parsing with format detection
    - GNMADataProcessor: Process raw data into structured datasets
    - GNMASchemaReader: Read and process schema files from PDFs
    - GNMAPipeline: Complete workflow orchestrator

Configuration:
    - DownloadConfig: Configuration for downloader
    - FormatterConfig: Configuration for formatter
    - ProcessorConfig: Configuration for processor
    - SchemaReaderConfig: Configuration for schema reader

Usage:
    # Original single-import style (backwards compatible)
    from gnma_data_manager import (
        GNMAHistoricalDownloader, DownloadConfig,
        GNMADataFormatter, FormatterConfig,
        GNMADataParser,
        GNMADataProcessor, ProcessorConfig,
        GNMASchemaReader, SchemaReaderConfig,
        GNMAPipeline
    )
    
    # New modular import style
    from gnma_data_manager.downloader import GNMAHistoricalDownloader
    from gnma_data_manager.config import DownloadConfig
    
    # Quick pipeline creation
    from gnma_data_manager.utils import create_pipeline_from_env
    pipeline = create_pipeline_from_env()

Author: Jonathan E. Becker
Created: 2024
"""

# Import all main classes for backwards compatibility
from .config_options import (
    DownloadConfig,
    FormatterConfig,
    ProcessorConfig,
    SchemaReaderConfig
)

from .types import (
    ConversionResult,
    ProcessingResult,
    DateFormatError,
    DownloadError
)

from .downloader import GNMAHistoricalDownloader
from .formatter import GNMADataFormatter
from .parser import GNMADataParser
from .processor import GNMADataProcessor
from .schema_reader import GNMASchemaReader
from .pipeline import GNMAPipeline

from .utils import (
    create_default_configs,
    create_pipeline_from_env
)

# Define what gets imported with "from gnma_data_manager import *"
__all__ = [
    # Main classes
    'GNMAHistoricalDownloader',
    'GNMADataFormatter', 
    'GNMADataParser',
    'GNMADataProcessor',
    'GNMASchemaReader',
    'GNMAPipeline',
    
    # Configuration classes
    'DownloadConfig',
    'FormatterConfig',
    'ProcessorConfig', 
    'SchemaReaderConfig',
    
    # Result classes
    'ConversionResult',
    'ProcessingResult',
    
    # Exceptions
    'DateFormatError',
    'DownloadError',
    
    # Utility functions
    'create_default_configs',
    'create_pipeline_from_env'
]

# Package metadata
__version__ = "1.0.0"
__author__ = "Jonathan E. Becker"
__description__ = "Complete GNMA data processing pipeline"
__url__ = "https://github.com/your-repo/gnma_data_manager"

# Backwards compatibility note
import warnings

def _show_migration_note():
    """Show a note about the new modular structure (optional)."""
    print("GNMA Data Manager: Now available in modular structure!")
    print("Import individual components: from gnma_data_manager.formatter import GNMADataFormatter")
    print("Or use backwards-compatible imports as before.")

# Uncomment the line below if you want to show the migration note
# _show_migration_note()
