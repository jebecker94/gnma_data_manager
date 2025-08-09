#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
GNMA Data Manager - Type Definitions and Result Classes

This module contains result classes, exceptions, and type definitions
used across the GNMA data processing pipeline.

Classes:
    - ConversionResult: Results from data format conversion operations
    - ProcessingResult: Results from data processing operations
    - DateFormatError: Exception for date parsing errors
    - DownloadError: Exception for download errors

Author: Jonathan E. Becker
"""

from pathlib import Path
from typing import List, Union
import datetime

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
    
    def add_skip(self, file_path: Union[str, Path], reason: str = "File already exists"):
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
    
    def add_success(self, file_path: Union[str, Path], output_path: Union[str, Path], record_count: int, record_type: str):
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
    
    def add_skip(self, file_path: Union[str, Path], reason: str = "File already exists"):
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

class SchemaReaderError(Exception):
    """Custom exception for schema reader errors."""
    pass
