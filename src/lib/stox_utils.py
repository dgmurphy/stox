import pandas as pd 
import numpy as np
import configparser
from lib.ntlogging import logging
    
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

def clean_outliers(df):

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
    df = df.drop(['mean', 'std'], axis=1)

    return df