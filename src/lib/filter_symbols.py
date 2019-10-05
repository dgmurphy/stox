import pandas as pd 
import numpy as np
import os.path
import sys
from datetime import datetime, timedelta
from lib.ntlogging import logging

def filter_symbols(cfg):

    summary_input_file = cfg['data_dir'] + cfg['summary_input_file']
    symbols_output_file = cfg['data_dir'] + cfg['symbols_file']
    

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
        
    # drop any symbols that don't cover at least 2009-2019
    stox_df = stox_df[(stox_df['stock_from_date']<=pd.Timestamp(2009,1,1)) &
                    (stox_df['stock_to_date']>=pd.Timestamp(2019,1,1))]

    #  DEBUG keep small batch of symbols
    if cfg['use_all_symbols'].lower().startswith('n'):
        limit = int(cfg['symbols_limit'])
        stox_df = stox_df[:limit]

    stox_df = stox_df['symbol']
    logging.info("Writing " + symbols_output_file)
    stox_df.to_csv(symbols_output_file, index=False)                 
    logging.info("Symbols shape " + str(stox_df.shape))
    print(stox_df.head(100))
    logging.info("Done filtering symbols.\n")

