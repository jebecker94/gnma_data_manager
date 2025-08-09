#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
GNMA Data Manager - Utility Functions

This module contains shared utility functions used across the GNMA data processing
pipeline. These functions provide common functionality for configuration creation,
environment setup, and other shared operations.

Functions:
    - create_default_configs: Create default configuration objects
    - create_pipeline_from_env: Create pipeline from environment variables
    - Shared utility functions

Dependencies:
    - config: All configuration classes
    - pipeline: GNMAPipeline

Author: Jonathan E. Becker
"""

import os
from pathlib import Path
from typing import Tuple, Union
from dotenv import load_dotenv

# Imports from other modules
try:
    from .config_options import DownloadConfig, ProcessorConfig, SchemaReaderConfig
    from .pipeline import GNMAPipeline
except ImportError:
    from config_options import DownloadConfig, ProcessorConfig, SchemaReaderConfig
    from pipeline import GNMAPipeline

# ==========================================
# CONVENIENCE FUNCTIONS
# ==========================================

def create_default_configs(
    email_value: str,
    id_value: str,
    user_agent: str,
    base_data_folder: Union[str, Path] = "data",
    base_schema_folder: Union[str, Path] = "dictionary_files"
) -> Tuple[DownloadConfig, ProcessorConfig, SchemaReaderConfig]:
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
    
    processor_config = ProcessorConfig(
        raw_folder=f"{base_data_folder}/raw",
        clean_folder=f"{base_data_folder}/clean",
        schema_folder=f"{base_schema_folder}/combined"
    )
    
    schema_reader_config = SchemaReaderConfig(
        input_folder=f"{base_data_folder}/raw",
        output_folder=f"{base_data_folder}/raw"
    )
    
    return download_config, processor_config, schema_reader_config


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
    
    download_config, processor_config, schema_reader_config = create_default_configs(
        email_value=os.getenv('email_value'),
        id_value=os.getenv('id_value'),
        user_agent=os.getenv('user_agent')
    )
    
    return GNMAPipeline(download_config, processor_config, schema_reader_config)

