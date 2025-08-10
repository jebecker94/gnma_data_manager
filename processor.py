#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
GNMA Data Processor

This module contains the GNMADataProcessor class for processing GNMA raw data files
into structured datasets. It handles the conversion of raw parquet files (from the formatter)
into properly structured datasets using schema definitions.

Features:
    - Schema-driven data processing
    - Support for both delimited and fixed-width formats
    - Intelligent format detection integration
    - Multiple record type handling
    - Batch processing capabilities

Classes:
    - GNMADataProcessor: Main processor class

Future Improvements:
    - Enhanced memory management
    - Parallel processing support
    - Advanced data validation
    - Custom transformation pipelines

Dependencies:
    - config: ProcessorConfig
    - types: ProcessingResult
    - parser: GNMADataParser (for format detection)

Author: Jonathan E. Becker
"""

import os
import logging
import os
import pandas as pd
import polars as pl
from pathlib import Path
from typing import Dict, List, Optional, Union, Tuple
import yaml
from dotenv import load_dotenv
import zipfile

# Imports from other modules
try:
    from .config import ProcessorConfig
    from .types_and_errors import ProcessingResult, ConversionResult
except ImportError:
    from config import ProcessorConfig
    from types_and_errors import ProcessingResult, ConversionResult


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
        
        # Initialize embedded parser for format detection
        self.parser = self._create_embedded_parser()
        
        # Load prefix dictionary
        self._load_prefix_dictionary()
        
        # Create directories if needed
        if self.config.create_directories:
            self._ensure_directories_exist()
        
        self.logger.info("GNMA Data Processor initialized")
    
    def _setup_logging(self) -> None:
        """Set up logging configuration."""
        log_level = getattr(logging, self.config.log_level.upper(), logging.INFO)
        
        # Ensure logs directory exists
        self.config.logs_folder.mkdir(parents=True, exist_ok=True)
        
        logging.basicConfig(
            level=log_level,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(self.config.logs_folder / 'gnma_processor.log'),
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

    # ==========================================
    # Embedded Parser (migrated from GNMADataParser)
    # ==========================================
    def _create_embedded_parser(self):
        class _Parser:
            def __init__(self, verbose: bool = False):
                self.verbose = verbose
                self.schema_cache = {}
                self._format_cache = {}

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
                if has_max_length and has_format:
                    return 'DELIMITED'
                elif has_begin_end:
                    return 'FIXED_WIDTH'
                else:
                    return 'UNKNOWN'

            def analyze_prefix_format(self, prefix: str, schema_folder: Union[str, Path]) -> Dict:
                """
                Parse a GNMA data file using its corresponding schema.
                
                Args:
                    data_file_path: Path to the data file
                    schema_file_path: Path to the schema CSV file
                    
                Returns:
                    pd.DataFrame: Parsed data
                """
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
                            'error': 'Empty schema file',
                            'schema_exists': True
                        }
                        self._format_cache[prefix] = result
                        return result
                    file_format = self.detect_file_format(schema_df)
                    columns = list(schema_df.columns)
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

        return _Parser(verbose=getattr(self.config, 'verbose', False))

    # ==========================================
    # Formatting (migrated from GNMADataFormatter)
    # ==========================================
    def _get_formatter_setting(self, name: str, default):
        """Safely get a formatter-related setting from config with a default."""
        return getattr(self.config, name, default)

    def _get_output_path_for_format(self, input_path: Path, output_folder: Optional[Path] = None) -> Path:
        """Generate the output path for a converted file in raw folder, preserving prefix subfolder."""
        if output_folder is None:
            output_folder = Path(self._get_formatter_setting('raw_folder', 'data/raw'))

        # Maintain the same relative structure: data/raw/{prefix}/file.parquet when possible
        try:
            # If input is under raw_folder/prefix
            raw_folder = Path(self._get_formatter_setting('raw_folder', 'data/raw'))
            relative_parts = input_path.relative_to(raw_folder).parts
            if len(relative_parts) >= 1:
                prefix_folder = output_folder / relative_parts[0]
                return prefix_folder / input_path.with_suffix('.parquet').name
        except Exception:
            pass

        return output_folder / input_path.with_suffix('.parquet').name

    def stage_zip_to_parquet(
        self,
        zip_file_path: Union[str, Path],
        output_file_path: Optional[Union[str, Path]] = None,
        chunk_size: Optional[int] = None,
        **kwargs,
    ) -> bool:
        """
        Convert a single ZIP file containing text files to Parquet format using streaming.
        
        Args:
            zip_file_path: Path to the ZIP file
            output_file_path: Optional path for output file (auto-generated if not provided)
            chunk_size: Optional override for number of lines to process at once
            **kwargs: Additional arguments (text_column_name, encoding, etc.)
            
        Returns:
            True if conversion successful, False otherwise
        """

        zip_path = Path(zip_file_path)

        # Destination
        if output_file_path is None:
            output_path = self._get_output_path_for_format(zip_path)
        else:
            output_path = Path(output_file_path)

        # Skip existing
        if self._get_formatter_setting('skip_existing', True) and output_path.exists():
            self.logger.debug(f"Skipping existing file: {output_path}")
            return True

        text_column_name = kwargs.get('text_column_name', self._get_formatter_setting('text_column_name', 'text_content'))
        encoding = kwargs.get('encoding', self._get_formatter_setting('encoding', 'utf-8'))
        fallback_encoding = self._get_formatter_setting('fallback_encoding', 'iso-8859-1')
        supported_extensions = self._get_formatter_setting('supported_extensions', ['.txt', '.dat', '.csv'])
        chunk_size = chunk_size or self._get_formatter_setting('chunk_size', 100_000)

        output_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                text_files = [
                    name for name in zip_ref.namelist()
                    if any(name.lower().endswith(ext) for ext in supported_extensions)
                ]
                if not text_files:
                    self.logger.warning(f"No text files found in {zip_path}")
                    return False

                all_chunks = []
                total_rows = 0

                for text_file in text_files:
                    self.logger.debug(f"  Extracting: {text_file}")
                    try:
                        with zip_ref.open(text_file) as file:
                            buffer = []
                            while True:
                                line = file.readline()
                                if not line:
                                    break
                                try:
                                    decoded_line = line.decode(encoding).rstrip('\r\n')
                                except UnicodeDecodeError:
                                    try:
                                        decoded_line = line.decode(fallback_encoding).rstrip('\r\n')
                                    except Exception:
                                        self.logger.warning(f"Skipping undecodable line in {text_file}")
                                        continue
                                buffer.append(decoded_line)
                                if len(buffer) >= chunk_size:
                                    df = pl.LazyFrame({text_column_name: buffer})
                                    all_chunks.append(df)
                                    total_rows += len(buffer)
                                    buffer = []
                            if buffer:
                                df = pl.LazyFrame({text_column_name: buffer})
                                all_chunks.append(df)
                                total_rows += len(buffer)
                    except Exception as e:
                        self.logger.error(f"Error processing {text_file}: {e}")
                        continue

                if all_chunks:
                    try:
                        combined_df = pl.concat(all_chunks)
                        combined_df.sink_parquet(output_path)
                        self.logger.debug(f"Saved {total_rows} lines to {output_path}")
                        return True
                    except Exception as e:
                        self.logger.error(f"Error combining chunks for {zip_path}: {e}")
                        return False
                else:
                    self.logger.warning(f"No content found in {zip_path}")
                    if output_path.exists():
                        output_path.unlink()
                    return False

        except Exception as e:
            self.logger.error(f"Error converting {zip_path}: {e}")
            if output_path.exists():
                try:
                    output_path.unlink()
                except Exception:
                    pass
            return False

    def stage_txt_to_parquet(
        self,
        txt_file_path: Union[str, Path],
        output_file_path: Optional[Union[str, Path]] = None,
        chunk_size: Optional[int] = None,
        **kwargs,
    ) -> bool:
        """
        Convert a single TXT file to Parquet format using streaming.
        
        Args:
            txt_file_path: Path to the TXT file
            output_file_path: Optional path for output file (auto-generated if not provided)
            chunk_size: Optional override for number of lines to process at once
            **kwargs: Additional arguments (text_column_name, encoding, etc.)
            
        Returns:
            True if conversion successful, False otherwise
        """
        txt_path = Path(txt_file_path)

        if output_file_path is None:
            output_path = self._get_output_path_for_format(txt_path)
        else:
            output_path = Path(output_file_path)

        if self._get_formatter_setting('skip_existing', True) and output_path.exists():
            self.logger.debug(f"Skipping existing file: {output_path}")
            return True

        text_column_name = kwargs.get('text_column_name', self._get_formatter_setting('text_column_name', 'text_content'))
        encoding = kwargs.get('encoding', self._get_formatter_setting('encoding', 'utf-8'))
        fallback_encoding = self._get_formatter_setting('fallback_encoding', 'iso-8859-1')
        chunk_size = chunk_size or self._get_formatter_setting('chunk_size', 100_000)

        output_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            all_chunks = []
            total_rows = 0
            buffer = []
            with open(txt_path, 'r', encoding=encoding) as txt_file:
                while True:
                    line = txt_file.readline()
                    if not line:
                        break
                    try:
                        line = line.rstrip('\r\n')
                        buffer.append(line)
                        if len(buffer) >= chunk_size:
                            df = pl.LazyFrame({text_column_name: buffer})
                            all_chunks.append(df)
                            total_rows += len(buffer)
                            buffer = []
                    except Exception as e:
                        self.logger.warning(f"Error processing line in {txt_path}: {e}")
                        continue

            if buffer:
                df = pl.LazyFrame({text_column_name: buffer})
                all_chunks.append(df)
                total_rows += len(buffer)

            if all_chunks:
                try:
                    combined_df = pl.concat(all_chunks)
                    combined_df.sink_parquet(output_path)
                    self.logger.debug(f"Saved {total_rows} lines to {output_path}")
                    return True
                except Exception as e:
                    self.logger.error(f"Error combining chunks for {txt_path}: {e}")
                    return False
            else:
                self.logger.warning(f"No content found in {txt_path}")
                if output_path.exists():
                    output_path.unlink()
                return False

        except UnicodeDecodeError:
            # Try with fallback encoding
            try:
                all_chunks = []
                total_rows = 0
                buffer = []
                with open(txt_path, 'r', encoding=fallback_encoding) as txt_file:
                    while True:
                        line = txt_file.readline()
                        if not line:
                            break
                        try:
                            line = line.rstrip('\r\n')
                            buffer.append(line)
                            if len(buffer) >= chunk_size:
                                df = pl.LazyFrame({text_column_name: buffer}).collect()
                                all_chunks.append(df)
                                total_rows += len(buffer)
                                buffer = []
                        except Exception as e:
                            self.logger.warning(f"Error processing line in {txt_path}: {e}")
                            continue
                if buffer:
                    df = pl.LazyFrame({text_column_name: buffer}).collect()
                    all_chunks.append(df)
                    total_rows += len(buffer)
                if all_chunks:
                    try:
                        combined_df = pl.concat(all_chunks)
                        combined_df.write_parquet(output_path)
                        self.logger.debug(f"Saved {total_rows} lines to {output_path}")
                        return True
                    except Exception as e:
                        self.logger.error(f"Error combining chunks for {txt_path}: {e}")
                        return False
                else:
                    self.logger.warning(f"No content found in {txt_path}")
                    if output_path.exists():
                        output_path.unlink()
                    return False
            except Exception as e:
                self.logger.error(f"Error converting {txt_path} with fallback encoding: {e}")
                if output_path.exists():
                    try:
                        output_path.unlink()
                    except Exception:
                        pass
                return False
        except Exception as e:
            self.logger.error(f"Error converting {txt_path}: {e}")
            if output_path.exists():
                try:
                    output_path.unlink()
                except Exception:
                    pass
            return False

    def stage_prefix(
        self,
        prefix: str,
        base_folder: Optional[Union[str, Path]] = None,
        show_progress: bool = True,
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
            base_folder = Path(self._get_formatter_setting('raw_folder', self.config.raw_folder))
        else:
            base_folder = Path(base_folder)

        # Determine prefix folder
        prefix_folder = base_folder / prefix
        if not prefix_folder.exists():
            self.logger.error(f"Prefix folder not found: {prefix_folder}")
            return ConversionResult()

        result = ConversionResult()

        # ZIP files
        zip_files = list(prefix_folder.glob(f"{prefix}_*.zip"))
        result.total = len(zip_files)
        for idx, zip_file in enumerate(zip_files, 1):
            output_file = self._get_output_path_for_format(zip_file)
            if self._get_formatter_setting('skip_existing', True) and output_file.exists():
                result.add_skip(str(zip_file))
                continue
            try:
                success = self.stage_zip_to_parquet(zip_file, output_file)
                if success:
                    result.add_success(str(zip_file))
                else:
                    result.add_failure(str(zip_file), "Conversion returned False")
            except Exception as e:
                result.add_failure(str(zip_file), str(e))

        # TXT files
        txt_files = list({*prefix_folder.glob(f"{prefix}_*.txt"), *prefix_folder.glob(f"{prefix}_*.TXT")})
        result.total += len(txt_files)
        for txt_file in txt_files:
            output_file = self._get_output_path_for_format(txt_file)
            if self._get_formatter_setting('skip_existing', True) and output_file.exists():
                result.add_skip(str(txt_file))
                continue
            try:
                success = self.stage_txt_to_parquet(txt_file, output_file)
                if success:
                    result.add_success(str(txt_file))
                else:
                    result.add_failure(str(txt_file), "Conversion returned False")
            except Exception as e:
                result.add_failure(str(txt_file), str(e))

        return result

    def stage_all(self, exclude_prefixes: Optional[List[str]] = None) -> Dict[str, ConversionResult]:
        """
        Process all prefixes in the configuration.
        """
        if exclude_prefixes is None:
            exclude_prefixes = []
        results: Dict[str, ConversionResult] = {}
        for prefix in self.prefix_dict.keys():
            if prefix not in exclude_prefixes:
                results[prefix] = self.stage_prefix(prefix)
        return results

    def create_prefix_subfolders(self, prefixes: List[str] | None = None) :
        """
        Create subfolders for each Record_Type within data/clean/prefix folders.
        Only processes prefixes where the combined schema file has a Record_Type column.
        """

        if self.config.verbose:
            self.logger.info("Creating prefix folders in clean data directory")

        # If no prefixes are provided, use all prefixes from the dictionary
        if prefixes is None:
            prefixes = self.prefix_dict.keys()
        
        # Process each prefix
        for prefix in prefixes:
            if self.config.verbose:
                self.logger.info(f"Processing prefix: {prefix}")
            
            # Check if combined schema file exists
            combined_file = self.config.dictionary_folder / f'combined/{prefix}_combined_schema.csv'
            if not combined_file.exists():
                if self.config.verbose:
                    self.logger.debug(f"No combined schema file found: {combined_file.name}")
                continue
            
            try:
                # Load combined schema file
                df = pd.read_csv(combined_file)
                
                if df.empty:
                    if self.config.verbose:
                        self.logger.debug("Combined schema file is empty")
                    continue
                
                # Check if Record_Type column exists
                if 'Record_Type' not in df.columns:
                    if self.config.verbose:
                        self.logger.debug("No Record_Type column found - skipping organization")
                    continue
                
                # Get unique record types
                record_types = df['Record_Type'].dropna().unique()
                
                if len(record_types) == 0:
                    if self.config.verbose:
                        self.logger.debug("No record types found in Record_Type column")
                    continue
                
                if self.config.verbose:
                    self.logger.info(f"Found {len(record_types)} record types")
                
                # Create subfolders for each record type
                base_clean_dir = self.config.clean_folder / f'{prefix}'
                for record_type in record_types:
                    record_type_dir = base_clean_dir / str(record_type)
                    record_type_dir.mkdir(parents=True, exist_ok=True)
                    
                    if self.config.verbose:
                        self.logger.debug(f"Created folder: {record_type_dir.relative_to(Path('.'))}")
                
                if self.config.verbose:
                    self.logger.info(f"Organized {prefix} by {len(record_types)} record types")
                    
            except Exception as e:
                if self.config.verbose:
                    self.logger.warning(f"Error processing {prefix}: {e}")
        
        if self.config.verbose:
            self.logger.info("Completed prefix folder creation")

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
    
    def load_schema_by_record_type(self, file_prefix: str) -> Dict[str, pd.DataFrame]:
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
    
    def transform_prefix(
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
        self.logger.info(f"Transforming prefix: {prefix}")
        
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
            schemas = self.load_schema_by_record_type(prefix)
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
                
                # Transform the file
                success, record_count = self.transform_file(
                    parquet_file, prefix, record_type, output_file, file_format
                )
                
                if success:
                    result.add_success(str(parquet_file), str(output_file), record_count, record_type)
                else:
                    result.add_failure(str(parquet_file), f"Failed to process record type {record_type}")
        
        self.logger.info(f"Completed {prefix}: {result.successful}/{result.total_attempted} successful, "
                        f"{result.total_records_processed} total records processed")
        
        return result
    
    def run_pipeline_for_prefix(
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
        self.logger.info(f"Running pipeline for: {prefix}")
        
        # Step 1: Format raw data if requested
        if include_formatting:
            self.logger.info(f"Staging raw data for {prefix}...")
            format_result = self.stage_prefix(prefix, show_progress=False)
            self.logger.info(f"Staging complete: {format_result.successful} files converted")
        
        # Step 2: Process formatted data into structured datasets
        return self.transform_prefix(prefix, record_types, show_progress=True)

    def transform_fixed_width(
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

    def transform_delimited(
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
    
    def transform_file(
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
            schemas = self.load_schema_by_record_type(prefix)
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
                df = self.transform_delimited(parquet_file, file_schema, record_type)
            elif file_format == 'FIXED_WIDTH':
                df = self.transform_fixed_width(parquet_file, file_schema, record_type)
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
    
    def transform_all(
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
        
        self.logger.info(f"Transforming {len(prefixes)} prefixes: {prefixes}")
        
        for i, prefix in enumerate(prefixes, 1):
            if show_progress:
                self.logger.info(f"Processing prefix {i}/{len(prefixes)}: {prefix}")
            
            try:
                result = self.transform_prefix(prefix, record_types, show_progress)
                results[prefix] = result
            except Exception as e:
                self.logger.error(f"Error processing prefix {prefix}: {e}")
                # Create empty result for failed prefix
                failed_result = ProcessingResult()
                failed_result.add_failure(prefix, str(e))
                results[prefix] = failed_result
        
        return results


if __name__ == "__main__":

    load_dotenv()

    config = ProcessorConfig()

    processor = GNMADataProcessor(config)

    processor.create_prefix_subfolders()

    # Process the LoanPerf Prefix
    # result = processor.format_prefix("nissues")
    # processor.process_multiple_prefixes(prefixes=['nissues'])
