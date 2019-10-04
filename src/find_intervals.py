import pandas as pd 
import numpy as np
import os.path
import sys
from datetime import datetime, timedelta
from pandas.plotting import register_matplotlib_converters
from pandas import Grouper
import matplotlib.pyplot as plt
from lib.ntlogging import logging

def load_prices(prices_file):

    try:
        logging.info("Reading " + prices_file)
        prices_df = pd.read_table(prices_file, sep=',')
        prices_df['date'] = pd.to_datetime(prices_df['date'])
        logging.info("Prices df shape " + str(prices_df.shape))
        
    except Exception as e: 
        logging.critical("Not parsed: " + prices_file + "\n" + str(e))
        sys.exit()   

    return prices_df
     

def main():

    proc_starttime = datetime.now()
    logging.info("Processing started: " + str(proc_starttime))

    prices_input_file = "../data/stock_prices_filtered.csv"

    #register_matplotlib_converters()
        
    # load prices
    prices_df = load_prices(prices_input_file)
    prices_df['date'] = pd.to_datetime(prices_df['date'])
    prices_df = prices_df.groupby('symbol')
    
    num_groups = len(prices_df)
    logging.info("# groups: " + str(num_groups))

    # pick epoch
    epoch = pd.Timestamp(2017,1,3)
    has_epoch = 0
    missing_epoch = 0
    for symbol_name, symbol_df in prices_df:
        
        # group into time intervals
        symbol_df = symbol_df.groupby(Grouper(key='date', freq='W'))
        for tspan, tspan_df in symbol_df:
            min_date = tspan_df['date'].min()
            max_date = tspan_df['date'].max()
            logging.info(symbol_name + " " + str(min_date) + " " + str(max_date))


            

        #logging.info(name + " # intervals: " + str(len(symbol_df)))

    

    proc_endtime = datetime.now()
    logging.info(("Total Processing time (min): " + 
        str((proc_endtime - proc_starttime).total_seconds() / 60.0)))

    plt.show()


if __name__ == '__main__':
    main()
    print("DONE\n")
