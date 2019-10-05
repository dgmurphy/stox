import pandas as pd 
import numpy as np
import os.path
import sys
from datetime import datetime, timedelta
from lib.ntlogging import logging


def load_prices(prices_file):

    try:
        logging.info("Reading " + prices_file)
        prices_df = pd.read_table(prices_file, sep=',')
        logging.info("Prices df shape " + str(prices_df.shape))
        
    except Exception as e: 
        logging.critical("Not parsed: " + prices_file + "\n" + str(e))
        sys.exit()   

    return prices_df
     

def filter_prices(cfg):

    start_list = cfg['prices_date_start'].split('-')
    start_yr = int(start_list[0])
    start_mo = int(start_list[1])
    start_d = int(start_list[2])

    end_list = cfg['prices_date_end'].split('-')
    end_yr = int(end_list[0])
    end_mo = int(end_list[1])
    end_d = int(end_list[2])

    PRICES_START_DATE = pd.Timestamp(start_yr, start_mo, start_d)
    PRICES_END_DATE = pd.Timestamp(end_yr, end_mo, end_d)

    symbols_input_file = cfg['data_dir'] + cfg['symbols_file']
    prices_input_file = cfg['data_dir'] + cfg['prices_input_file']
    filtered_prices_output_file = cfg['data_dir'] + cfg['prices_filtered_file']
 
    try:
        with open(symbols_input_file) as f:
            symbols = f.read().splitlines()
            logging.info("Read " + str(len(symbols)) + " symbols")

    except Exception as e:
        logging.warning("Not parsed: " + symbols_input_file + "\n" + str(e))
        sys.exit()
        
    # load prices
    prices_df = load_prices(prices_input_file)
    logging.info("Filtering prices with symbols. ")

    prices_df = prices_df[prices_df['symbol'].isin(symbols)]
    prices_df = prices_df.drop(['open', 'high', 'low'], axis=1)
    logging.info("Filtered symbols df shape " + str(prices_df.shape))

    logging.info("Filtering by date range.")
    prices_df['date'] = pd.to_datetime(prices_df['date'])
    prices_df = prices_df[(prices_df['date']>=PRICES_START_DATE) &
                   (prices_df['date']<=PRICES_END_DATE)]
    logging.info("Filtered dates df shape " + str(prices_df.shape))

    # write filtered prices
    logging.info("Writing filtered prices to " + filtered_prices_output_file)
    prices_df.to_csv(filtered_prices_output_file, index=False)

