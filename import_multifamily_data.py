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

