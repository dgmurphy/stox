# stox_v0.py
import pandas as pd 
import numpy as np
import os.path
import sys
from datetime import datetime, timedelta
from lib.ntlogging import logging

summary_input_file = "../data/dataset_summary.csv"

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
    
# Keep stocks with from date on or before 1/1/2009
#df[(df['date']>datetime.date(2016,1,1)) & (df['date']<datetime.date(2016,3,1))]  
stox_df = stox_df[(stox_df['stock_from_date']<=pd.Timestamp(2009,1,1)) &
                   (stox_df['stock_to_date']>=pd.Timestamp(2019,1,1))]
print(str(stox_df.shape))
print(stox_df.head(100))

