import pandas as pd 
import numpy as np
import os.path
import sys
from datetime import datetime, timedelta
from lib.ntlogging import logging
from lib.stox_utils import *


def filter_symbols(cfg):

    summary_input_file = SUMMARY_INPUT_FILE
    symbols_output_file = SYMBOLS_FILE

    start_list = cfg['date_start'].split('-')
    start_yr = int(start_list[0])
    start_mo = int(start_list[1])
    start_d = int(start_list[2])

    end_list = cfg['date_end'].split('-')
    end_yr = int(end_list[0])
    end_mo = int(end_list[1])
    end_d = int(end_list[2])

    PRICES_START_DATE = pd.Timestamp(start_yr, start_mo, start_d)
    PRICES_END_DATE = pd.Timestamp(end_yr, end_mo, end_d)
    

    if not os.path.exists(summary_input_file):
        logging.critical("Location file not found: " +
                            summary_input_file)
        sys.exit()

    try:
        logging.info("Reading: " + summary_input_file)
        stox_df = pd.read_table(summary_input_file, sep=',')
        stox_df['stock_from_date'] = pd.to_datetime(stox_df['stock_from_date'])
        stox_df['stock_to_date'] = pd.to_datetime(stox_df['stock_to_date'])

    except Exception as e:
        logging.warning("Not parsed: " + summary_input_file + "\n" + str(e))
        sys.exit()
        
    # drop any symbols that don't cover at least the analysis window
    stox_df = stox_df[(stox_df['stock_from_date'] <= PRICES_START_DATE) &
                    (stox_df['stock_to_date'] >= PRICES_END_DATE)]

    logging.info("Done filtering symbols.\n")

    return stox_df['symbol'].tolist()

