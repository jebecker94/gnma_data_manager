#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on: Sat Dec 3 09:49:44 2022
Last updated on: Friday May 2 23:21:00 2025
@author: Jonathan E. Becker
"""

#%% Setup
# Import Packages
import os
import glob
import pandas as pd
import numpy as np
import config

#%% Import Functions
# Import platdailyPPS
def import_platdailyPPS() :

    pass

# Import platdailyPS
def import_platdailyPS() :

    pass

# Import platdcoll
def import_platdcoll() :

    pass

# Import platmonPPS
def import_platmonPPS() :

    pass

# Import platmonPS
def import_platmonPS() :

    pass

# Import platcoll
def import_platcoll() :

    pass

# Import hplatdailyPS
def import_hplatdailyPS() :

    pass

# Import hplatdailyS
def import_hplatdailyS() :

    pass

# Import hplatmonPS
def import_hplatmonPS() :

    pass

# Import hplatmonS
def import_hplatmonS() :

    pass

#%% Main Routine
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
