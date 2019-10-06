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
     

def build_intervals(cfg):

    # Processing timer
    proc_starttime = datetime.now()
    logging.info("Processing started: " + str(proc_starttime))

    # PROPS
    INTERVAL = cfg['stock_hold_time']  # Amount of time between start/end price
    prices_input_file = cfg['stox_data_dir'] + cfg['daily_prices_file']
    prices_output_file = (cfg['stox_data_dir'] + cfg['prices_grouped_prefix'] + 
                          INTERVAL + ".csv")

    #register_matplotlib_converters()
        
    # load prices
    prices_df = load_prices(prices_input_file)
    prices_df['date'] = pd.to_datetime(prices_df['date'])
    prices_df = prices_df.groupby('symbol')
    
    num_groups = len(prices_df)
    logging.info("# groups: " + str(num_groups))


    #cols for the output data frame
    cols = ['symbol', 'interval', 'days', 'd0', 'p0', 'd1', 'p1', 'deltap']
    rowlst = [] # holder for row
    symbol_count = 0
    percent_complete = 0
    for symbol_name, symbol_df in prices_df:

        logging.info("Processing " + symbol_name + " (" + 
                      str(percent_complete) + "%)")
        
        # group into time intervals
        symbol_df = symbol_df.groupby(Grouper(key='date', freq=INTERVAL))
        inum = 1  # interval index
        for tspan, tspan_df in symbol_df:

            # logging.info("processing span: " + str(tspan))
            tspan_df = tspan_df.sort_values(['date']).reindex()

            if len(tspan_df) > 0:
                d0 = tspan_df.iloc[0].loc['date']
                d1 = tspan_df.iloc[-1].loc['date']
                delta_s = (d1 - d0).total_seconds()
                # elapsed days is inclusive
                delta_days = 1.0 + (delta_s / (60.0 * 60.0 * 24.0))
                p0 = tspan_df.iloc[0].loc['close_adjusted']
                p1 = tspan_df.iloc[-1].loc['close_adjusted']
                deltap = p1 - p0

                # format output
                p0 = str(f"{p0:.4f}")
                p1 = str(f"{p1:.4f}")
                deltap = str(f"{deltap:.3f}")
                delta_days = str(f"{delta_days:.3f}")

                rowlst.append([symbol_name, inum, delta_days, d0, p0, 
                    d1, p1, deltap])

                inum += 1

        symbol_count += 1
        percent_complete = int((symbol_count / num_groups) * 100.0)
        
        o_df = pd.DataFrame(rowlst, columns=cols) 

    logging.info("Writing output csv " + prices_output_file)
    o_df.to_csv(prices_output_file, index=False, sep="\t")

    # Processing timer
    proc_endtime = datetime.now()
    logging.info(("Total Processing time (min): " + 
        str((proc_endtime - proc_starttime).total_seconds() / 60.0)))

    plt.show()
