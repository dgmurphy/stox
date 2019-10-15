import pandas as pd 
import numpy as np
import os.path
import sys
from math import floor
from datetime import datetime, timedelta
from lib.ntlogging import logging
from lib.stox_utils import *


def make_blacklist(cfg):

    pct_cutoff = 0.51

    file_list = [
        'analysis_4_days_1000_dollars.csv',
        'analysis_9_days_1000_dollars.csv',
        'analysis_14_days_1000_dollars.csv',
        'analysis_19_days_1000_dollars.csv',
        'analysis_30_days_1000_dollars.csv',
        'analysis_60_days_1000_dollars.csv',
        'analysis_90_days_1000_dollars.csv'
    ]

    logging.info(f"Loading {file_list[0]}")
    days4_df = pd.read_table(STOX_DATA_DIR + file_list[0], sep=",")
    logging.info(f"days4_df shape: {days4_df.shape}")

    days4_df = days4_df[days4_df.pct_black > pct_cutoff]
    logging.info(f"days4_df > pct cutoff shape: {days4_df.shape}")

    keep_symbols = set(days4_df['symbol'].tolist())
    drop_symbols = set()

    for file in file_list[1:]:
        logging.info(f"Checking symbols in {file}")
        df = pd.read_table(STOX_DATA_DIR + file, sep=",")
        df = df[df['pct_black'] > pct_cutoff]
        check_symbols = df['symbol'].tolist()
        for k in keep_symbols:
            if k not in check_symbols:
                drop_symbols.add(k)

    print(f"{len(drop_symbols)} symbols getting dropped.")
    #logging.info(str(drop_symbols))

    for d in drop_symbols:
        keep_symbols.remove(d)

    print(f"{len(keep_symbols)} symbols kept.")

    sys.exit()