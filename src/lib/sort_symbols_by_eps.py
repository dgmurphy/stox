import pandas as pd 
import numpy as np
import os.path
import sys
from datetime import datetime, timedelta
import configparser
from lib.ntlogging import logging
from lib.filter_symbols import *


def load_earnings(fname):

    try:
        logging.info("Reading " + fname)
        earn_df = pd.read_table(fname, sep=',')
        logging.info("Earnings df shape " + str(earn_df.shape))
        
    except Exception as e: 
        logging.critical("Not parsed: " + fname + "\n" + str(e))
        sys.exit()   

    return earn_df
     

def sort_symbols_by_eps(cfg):

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

    earnings_input_file = cfg['raw_data_dir'] + cfg['earnings_input_file']
    sorted_symbols_output_file = cfg['stox_data_dir'] + cfg['symbols_file']
        
    # load earnings
    earn_df = load_earnings(earnings_input_file)

    logging.info("Filtering by date range.")
    earn_df['date'] = pd.to_datetime(earn_df['date'])

    #Filter by start / end dates
    earn_df = earn_df[(earn_df['date'] >= PRICES_START_DATE) &
                   (earn_df['date'] <= PRICES_END_DATE)]

    logging.info("Filtered dates df shape " + str(earn_df.shape))

    earn_df = earn_df.groupby('symbol')
    logging.info(str(len(earn_df)) + " symbols groups found")

    # get list of symbols whose valid listing dates cover the start-end period
    logging.info("Filtering symbols (keep symbols covering the date span)")
    symbols_span_list = filter_symbols(cfg)
    logging.info(f"{len(symbols_span_list)} symbols cover the date span")

    # make df of sorted symbols
    cols = ['symbol', 'avg_eps']
    lst = []
    for symbol, symbol_grp in earn_df:
        # keep only symbols that pass the symbols listing span
        if symbol in symbols_span_list:
            logging.info("Getting mean eps for " + symbol)
            avg_eps = symbol_grp['eps'].mean()
            lst.append([symbol, avg_eps])
        
    symsort_df = pd.DataFrame(lst, columns=cols)
    #symsort_df = symsort_df.dropna()
    symsort_df = symsort_df.sort_values('avg_eps', ascending=False)

    # apply symbols limit
    limit = int(cfg['symbols_limit'])
    symsort_df = symsort_df[:limit]
    
    # write sorted symbols
    logging.info(f"Writing {len(symsort_df)} sorted symbols to " + 
                 sorted_symbols_output_file)

    symsort_df.to_csv(sorted_symbols_output_file, index=False)
    return symsort_df['symbol']

