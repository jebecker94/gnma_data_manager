#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on: Sat Dec 3 09:49:44 2022
Last updated on: Friday May 2 23:21:00 2025
@author: Jonathan E. Becker
"""

# Import Packages
import os
import glob
import pandas as pd
import numpy as np
import config
import re

# Get Combined Suffix
def get_combined_suffix(files: list) :
    """Create a suffix for a combined file from a list of individual files."""

    # Get Suffixes from File Names and Create Combined Suffix from Min and Max Dates
    suffixes = [os.path.splitext(os.path.basename(file))[0].split('_')[-1] for file in files]
    suffixes = ["".join(re.findall(r'\d', suffix)) for suffix in suffixes] # Extract only numeric characters
    combined_suffix = '_'+min(suffixes)+'-'+max(suffixes)

    # Return Combined Suffix
    return combined_suffix

#%% Import Functions
# Import mfpldaily
def import_mfpldaily() :

    pass

# Import mfpldailymni
def import_mfpldailymni() :

    pass

# Import mfplmon
def import_mfplmon() :

    pass

# Import mfppprpt
def import_mfppprpt() :

    pass

# Import mfpppmon
def import_mfpppmon() :

    pass

# Import mftermpools
def import_mftermpools() :

    pass

## Main Routine
if __name__ == '__main__' :

    # Set Folder Structure
    DATA_DIR = config.DATA_DIR
    RAW_DIR = config.RAW_DIR
    CLEAN_DIR = config.CLEAN_DIR
    PROJECT_DIR = config.PROJECT_DIR

    # Make Data Directories if missing
    if not os.path.exists(DATA_DIR) :
        os.makedirs(DATA_DIR)
    if not os.path.exists(RAW_DIR) :
        os.makedirs(RAW_DIR)
    if not os.path.exists(CLEAN_DIR) :
        os.makedirs(CLEAN_DIR)

