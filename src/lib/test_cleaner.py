import pandas as pd 
import numpy as np
import os.path
import sys
from datetime import datetime, timedelta
from pandas.plotting import register_matplotlib_converters
import matplotlib.pyplot as plt
from lib.ntlogging import logging
from lib.stox_utils import clean_outliers



def cleaner(df):

    logging.info("Cleaning outliers.")
    # drop rows with no volume & prices less than epsilon
    eps = 0.01
    df = df.loc[(df.open > eps) &
                (df.close > eps) &
                (df.high > eps) & 
                (df.close > eps) & 
                (df.volume > 0)].copy()
   
    #  keep everthing inside +/- 3 std deviations from mean
        # rolling price sampling window 
    window = 21

    df['mean'] = df['close'].rolling(window).mean()
    df['std'] = df['close'].rolling(window).std()
    df = df[(df.close <= df['mean'] + 3 * df['std']) &
                        (df.close >= df['mean'] - 3 * df['std'])]

    #df = df.dropna()
    #df.to_csv("clean_outliers.csv")
    #df = df.drop(['mean', 'std'], axis=1)

    return df    

def test_cleaner(cfg):

    param_list = cfg['cleaner_test_params'].split(" ")
    if len(param_list) < 3:
        logging.warn("Plot params malformed. Skipping plot.")
        return
    else:
        logging.info(f"Using symbol {param_list[0].strip()}")
        logging.info(f"  from {param_list[1]}")
        logging.info(f"  to {param_list[2]}")

    register_matplotlib_converters()
    
    prices_input_file = cfg['raw_data_dir'] + cfg['raw_prices_input_file']
    try:
        logging.info("Reading " + prices_input_file)
        prices_df = pd.read_table(prices_input_file, sep=',')
        prices_df['date'] = pd.to_datetime(prices_df['date'])
        logging.info("Prices df shape " + str(prices_df.shape))
        
    except Exception as e: 
        logging.critical("Not parsed: " + prices_input_file + "\n" + str(e))
        sys.exit()   

    # param string [symbol start-date end-date] 
    #   e.g. IBM 2009-01-01 2019-01-01
    symbol = param_list[0].strip()

    start_list = param_list[1].split('-')
    start_yr = int(start_list[0])
    start_mo = int(start_list[1])
    start_d = int(start_list[2])

    end_list = param_list[2].split('-')
    end_yr = int(end_list[0])
    end_mo = int(end_list[1])
    end_d = int(end_list[2])

    date_start = pd.Timestamp(start_yr, start_mo, start_d)
    date_end = pd.Timestamp(end_yr, end_mo, end_d)
   
    # filter on date range
    logging.info("Filtering on date range")
    df = prices_df[(prices_df['date'] >= date_start) & (prices_df['date'] <= date_end)]
    df = df.sort_values(['date'])

    # get group for this symbol
    logging.info("Filtering on symbol")
    df = df.groupby('symbol').get_group(symbol)

    # write raw df to file
    span_str = (date_start.strftime("%Y-%m-%d") + "_" +
        date_end.strftime("%Y-%m-%d"))
    csv_name = cfg['stox_data_dir'] + symbol + "_" + span_str + "_raw.csv"
    cdf.to_csv(csv_name, index=False, sep="\t", float_format='%.3f')


    # test cleaner
    cdf = cleaner(df)

    # write cdf to file
    span_str = (date_start.strftime("%Y-%m-%d") + "_" +
        date_end.strftime("%Y-%m-%d"))
    csv_name = cfg['stox_data_dir'] + symbol + "_" + span_str + "_cleantest.csv"
    cdf.to_csv(csv_name, index=False, sep="\t", float_format='%.3f')


    # PLOT
    fig, axs = plt.subplots(2)
    plt.suptitle(symbol, fontsize=10)

    axs[0].set_title('Raw', {'fontsize': 10})
    axs[0].scatter(df['date'].tolist(), df['close'], color = 'blue', s=2)

    axs[1].set_title('Cleaned', {'fontsize': 10})
    axs[1].scatter(cdf['date'].tolist(), cdf['close'], color = 'green', s=2,
                   sharex=axs[0])


    plt_filename = cfg['stox_data_dir'] + symbol + "_" + span_str + ".png"
    plt.savefig(plt_filename)
    plt.show()


