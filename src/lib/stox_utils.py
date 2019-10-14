import pandas as pd 
import numpy as np
import configparser
from lib.ntlogging import logging

RAW_DATA_DIR = "../data/raw/"
STOX_DATA_DIR = "../data/stox/"
SUMMARY_INPUT_FILE = RAW_DATA_DIR + "dataset_summary.csv"
RAW_PRICES_INPUT_FILE = RAW_DATA_DIR + "stock_prices_latest.csv"
EARNINGS_INPUT_FILE = RAW_DATA_DIR + "earnings_latest.csv"
CLEANED_PRICES_FILE = STOX_DATA_DIR + "stock_prices_cleaned.csv"
FILTERED_PRICES_FILE = STOX_DATA_DIR + "stock_prices_filtered.csv"
SYMBOLS_FILE = STOX_DATA_DIR + "symbols.csv"
BUY_SELL_RESULTS_FILE = STOX_DATA_DIR + "buy_sell_results.csv"
ANALYSIS_FILE_PREFIX = STOX_DATA_DIR + "analysis_"


    
def load_config():

    # Config parser
    ini_filename = "stox.ini"
    logging.info("Reading config from: " + ini_filename)
    config = configparser.ConfigParser()
    try:
        config.read(ini_filename)
    except Exception as e: 
        logging.critical("Error reading .ini file: " + ini_filename)
        logging.critical("Exception: " + str(type(e)) + " " + str(e))
        sys.exit()


    return config

def save_config(config):
    
    ini_filename = "stox.ini"
    with open(ini_filename, 'w') as configfile:
        config.write(configfile)
        logging.info("Saved " + ini_filename)


def clean_outliers(df, window):

    df = df.copy()
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values(['date'])

    # drop rows with no volume & prices less than epsilon
    eps = 0.01
    df = df.loc[(df.open > eps) &
                (df.close > eps) &
                (df.high > eps) & 
                (df.close > eps) & 
                (df.volume > 0)].copy()
   
    #  keep everthing inside +/- 3 std deviations from mean
        # rolling price sampling window 
    window = int(window)   
    devs = 3 # std devs

    #df['zscore'] = (df.close - df.close.mean())/df.close.std(ddof=0)
    #df['zscore'] = df['zscore'].abs()
    df['mean'] = df['close'].rolling(window, center=True).mean()
    df['std'] = df['close'].rolling(window, center=True).std()
    df = df[(df.close <= df['mean'] + devs * df['std']) &
                        (df.close >= df['mean'] - devs * df['std'])]
    df = df.drop(['mean', 'std'], axis=1)

    return df
