import pandas as pd 
import numpy as np
import os.path
import sys
from datetime import datetime, timedelta
from lib.ntlogging import logging
from lib.stox_utils import clean_outliers


def load_df(df_file):

    try:
        logging.info("Reading " + df_file)
        df = pd.read_table(df_file, sep=',')
        logging.info("df shape " + str(df.shape))
        
    except Exception as e: 
        logging.critical("Not parsed: " + df_file + "\n" + str(e))
        sys.exit()   

    return df
     

def clean_prices(cfg):

    prices_input_file = cfg['raw_data_dir'] + cfg['raw_prices_input_file']
    prices_output_file = cfg['stox_data_dir'] + cfg['cleaned_prices_file']
 
    # load prices
    prices_df = load_df(prices_input_file)

    # Clean outliers 
    # TODO sort by symbol or date
    prices_df = clean_outliers(prices_df)
    
                   
    logging.info("Cleaned prices shape " + str(prices_df.shape))

    # write filtered prices
    logging.info("Writing cleaned prices to " + prices_output_file)
    prices_df.to_csv(prices_output_file, index=False)
    
    
