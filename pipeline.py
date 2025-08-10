#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
GNMA Pipeline Orchestrator

This module contains the GNMAPipeline class that coordinates the entire GNMA data
processing workflow. It provides a high-level interface for running complete
data processing pipelines.

Features:
    - Complete workflow orchestration
    - Integration of all pipeline components
    - Error handling and recovery
    - Progress monitoring and logging
    - Flexible workflow configuration

Classes:
    - GNMAPipeline: Main pipeline orchestrator

Workflow:
    1. Download raw data files
    2. Read schema files
    3. Process into structured datasets

Dependencies:
    - All other modules (downloader, processor, schema_reader)
    - config: All configuration classes

Author: Jonathan E. Becker
"""

import logging
from pathlib import Path
from typing import Dict, Any
from dotenv import load_dotenv

# Imports from other modules
try :
    from .config import DownloadConfig, ProcessorConfig, SchemaReaderConfig
    from .downloader import GNMAHistoricalDownloader
    from .processor import GNMADataProcessor
    from .schema_reader import GNMADataSchemaReader
except ImportError:
    from config import DownloadConfig, ProcessorConfig, SchemaReaderConfig
    from downloader import GNMAHistoricalDownloader
    from processor import GNMADataProcessor
    from schema_reader import GNMADataSchemaReader

class GNMAPipeline:
    """
    Complete GNMA data processing pipeline orchestrator.
    
    This class coordinates the entire workflow:
    1. Download raw data files
    2. Process into structured datasets
    """
    
    def __init__(
        self,
        download_config: DownloadConfig,
        processor_config: ProcessorConfig,
        schema_reader_config: SchemaReaderConfig,
        verbose: bool = True
    ):
        """
        Initialize the complete pipeline.
        
        Args:
            download_config: Configuration for downloader
            processor_config: Configuration for processor
            schema_reader_config: Configuration for schema reader
            verbose: Whether to show detailed progress
        """
        self.verbose = verbose
        self.download_config = download_config  # Store for logging setup
        self._setup_logging()
        
        # Initialize all components
        self.downloader = GNMAHistoricalDownloader(download_config)
        self.processor = GNMADataProcessor(processor_config)
        self.schema_reader = GNMADataSchemaReader(schema_reader_config)
        
        self.logger.info("GNMA Pipeline initialized with all components")
    
    def _setup_logging(self) -> None:
        """Set up pipeline logging."""
        # Ensure logs directory exists
        self.download_config.logs_folder.mkdir(parents=True, exist_ok=True)
        
        logging.basicConfig(
            level=logging.INFO if self.verbose else logging.WARNING,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(self.download_config.logs_folder / 'gnma_pipeline.log'),
                logging.StreamHandler()
            ]
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
            'processor': 'ready',
            'pipeline': 'ready'
        }

if __name__ == "__main__":
    load_dotenv()
    download_config = DownloadConfig()
    processor_config = ProcessorConfig()
    schema_reader_config = SchemaReaderConfig()
    pipeline = GNMAPipeline(download_config, processor_config, schema_reader_config)