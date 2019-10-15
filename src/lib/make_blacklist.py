import pandas as pd 
import numpy as np
import os.path
import sys
from math import floor
from datetime import datetime, timedelta
from lib.ntlogging import logging
from lib.stox_utils import *


# for a symbol, get the pct_black for each of the holds
#  return a row with the pct_black for each hold plus the avg
# def get_avg_blk(symbol, df_list):

#     pct_blk = 0
#     for df in df_list:
#         # get the value at col 'pct_black' from the symbol row
#         #  here there should be only one result -> values[0]
#         pct_blk = df.loc[df.symbol == symbol, 'pct_black'].values[0]
#         print(f"pct_blk {pct_blk}")

#     sys.exit()

def make_blacklist(cfg):

    pct_cutoff = 0.51

    file_list = [
        'analysis_4_days_1000_dollars.csv',
        'analysis_9_days_1000_dollars.csv',
        'analysis_14_days_1000_dollars.csv',
        'analysis_19_days_1000_dollars.csv',
        'analysis_30_days_1000_dollars.csv',
        'analysis_60_days_1000_dollars.csv',
        'analysis_90_days_1000_dollars.csv'
    ]
    df_list = []  # keep each df 

    logging.info(f"Loading {file_list[0]}")
    df = pd.read_table(STOX_DATA_DIR + file_list[0], sep=",")
    logging.info(f"file0 df shape: {df.shape}")

    df = df[df.pct_black > pct_cutoff]
    logging.info(f"file0 df > pct cutoff shape: {df.shape}")

    df_list.append(df)

    keep_symbols = set(df['symbol'].tolist())
    drop_symbols = set()

    for file in file_list[1:]:
        logging.info(f"Checking symbols in {file}")
        df = pd.read_table(STOX_DATA_DIR + file, sep=",")
        df = df[df['pct_black'] > pct_cutoff]
        check_symbols = df['symbol'].tolist()
        for k in keep_symbols:
            if k not in check_symbols:
                drop_symbols.add(k)
        
        df_list.append(df)

    print(f"{len(drop_symbols)} symbols getting dropped.")
    #logging.info(str(drop_symbols))

    for d in drop_symbols:
        keep_symbols.remove(d)

    print(f"{len(keep_symbols)} symbols kept.")
    print(f"{len(df_list)} dfs made")


    # build a df with the pct_black results across all holds
    cols = ['symbol', 'd4', 'd9', 'd14', 'd19', 'd30', 'd60', 'd90', 'avg_return']
    rows_list = []
    i = 0
    for symbol in keep_symbols:
        rtn_sum = 0  # to make average return
        row = [symbol]
        for df in df_list:

            rtn_sum += df.loc[df.symbol == symbol, 'avg_return'].values[0]

            pct_blk = df.loc[df.symbol == symbol, 'pct_black'].values[0]
            row.append(pct_blk)

        avg_return = rtn_sum / float(len(file_list))
        row.append(avg_return)        
        rows_list.append(row)
        i += 1
        if((i % 100) == 0): 
            logging.info(f"processing symbol {i} of {len(keep_symbols)}")

    # blacklist df
    logging.info(f"Building blacklist df with {len(rows_list)} rows")
    bl_df = pd.DataFrame(rows_list, columns=cols)
    
    # add column for avg pct_blk
    bl_df['avg_pct_blk'] = bl_df.iloc[:, 1:8].mean(axis=1)
    bl_df = bl_df.sort_values('avg_pct_blk', ascending=False)

    logging.info(f"bl_df shape {bl_df.shape}")
    print(bl_df.head())

    bl_file = BLACKLIST_FILE_PREFIX + "_1000_dollars.csv"
    bl_df.to_csv("blacklist.csv", index=False, float_format='%.3f')
