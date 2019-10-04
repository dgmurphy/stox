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
     

def main():

    PRICES_START_DATE = pd.Timestamp(2017,1,1)
    PRICES_END_DATE = pd.Timestamp(2018,1,1)

    symbols_input_file = "../data/symbols.csv"
    prices_input_file = "../data/stock_prices_latest.csv"
    filtered_prices_output_file = "../data/stock_prices_filtered.csv"

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
    logging.info("Writing filtered prices")
    prices_df.to_csv(filtered_prices_output_file, index=False)




if __name__ == '__main__':
    main()
    print("DONE\n")
