# -*- coding: utf-8 -*-
"""
Created on Sun Dec 29 08:46:50 2024
@author: Jebecker3
"""

# Import Packages
import os
from platform import system
from decouple import config
from pathlib import Path

# Specific Data Folders
PROJECT_DIR = config('PROJECT_DIR', default=Path(os.getcwd()))
DATA_DIR = config('DATA_DIR', default=PROJECT_DIR/'data')
RAW_DIR = config('HMDA_RAW_DIR', default=DATA_DIR/'raw')
CLEAN_DIR = config('HMDA_CLEAN_DIR', default=DATA_DIR/'clean')
