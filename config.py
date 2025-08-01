# -*- coding: utf-8 -*-
"""
Created on Sun Dec 29 2024
Edited on Fri Aug 01 2025
@author: Jonathan E. Becker
"""

# Import Packages
import os
from decouple import config
from pathlib import Path

# Specific Data Folders
PROJECT_DIR = config('PROJECT_DIR', default=Path(os.getcwd()))
DATA_DIR = config('DATA_DIR', default=PROJECT_DIR/'data')
DICTIONARY_DIR = config('DICTIONARY_DIR', default=PROJECT_DIR/'dictionary_files')
RAW_DIR = config('RAW_DIR', default=DATA_DIR/'raw')
CLEAN_DIR = config('CLEAN_DIR', default=DATA_DIR/'clean')
