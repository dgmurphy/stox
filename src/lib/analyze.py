import pandas as pd 
import numpy as np
import os.path
import sys
from math import floor
from datetime import datetime, timedelta
from lib.ntlogging import logging
from lib.load_config import load_config

# Build the following row for each symbol in the buy_sell_results:
#   symbol  num_trades  pct_black  num_black num_red avg_return
#   avg_gain max_gain max_gain_date avg_loss max_loss max_loss_date
#
#  The dates are the buy dates

def analyze(cfg):

    buy_sell_results_file =  cfg['stox_data_dir'] + cfg['buy_sell_results']
    analysis_file = cfg['stox_data_dir'] + cfg['analysis_file']

    # clean up the existing output file (ignore !exists error)
    try:
        os.remove(analysis_file)
    except OSError:
        pass   

    # load buy-sell results and group by symbol
    try:
        logging.info(f"Reading {buy_sell_results_file}")
        bsr_df = pd.read_table(buy_sell_results_file, sep=",")
        #bsr_df['buy_date'] = pd.to_datetime(bsr_df['buy_date'])
        #bsr_df['sell_date'] = pd.to_datetime(bsr_df['sell_date'])
        bsr_df = bsr_df.groupby('symbol')
        logging.info("Found " + str(len(bsr_df)) + " symbols in buy-sell data.")
    
    except Exception as e:
        logging.warning("Not parsed: " + buy_sell_results_file + "\n" + str(e))
        sys.exit()

    results_lst = []  # output rows
    qmax = 100000   # output queue max
    write_header = True  # results file header (write once flag)
    symnum = 0
    numsyms = len(bsr_df)
    
    for symbol, sym_df in bsr_df:

        num_trades = len(sym_df)
        blk_trades_df = sym_df[sym_df['gain_total'] > 0.0]
        red_trades_df = sym_df[sym_df['gain_total'] < 0.0]
        num_black = len(blk_trades_df)
        pct_black = float(num_black) / float(num_trades)
        num_red = len(red_trades_df)

        avg_return = sym_df["gain_total"].mean()
        avg_gain = blk_trades_df["gain_total"].mean()
        max_gain = blk_trades_df["gain_total"].max()
        max_gain_date = blk_trades_df.loc[blk_trades_df['gain_total'].idxmax(), 'buy_date']
        
        avg_loss = red_trades_df["gain_total"].mean()
        max_loss = red_trades_df["gain_total"].min()
        max_loss_date = blk_trades_df.loc[blk_trades_df['gain_total'].idxmin(), 'buy_date']

        row = [symbol, num_trades, pct_black, num_black, num_red, avg_return,
               avg_gain, max_gain, max_gain_date, avg_loss, max_loss, max_loss_date]

        results_lst.append(row)

        # peridocially write the results list
        if len(results_lst) >= qmax:
            logging.info(f"Writing {qmax} results to {analysis_file}")
            append_analysis_csv(analysis_file, results_lst, write_header)
            write_header = False
            results_lst = []

        symnum += 1    # keep track of how many symbols have been processed
        logging.info(f"{symbol} \t\t[{symnum} of {numsyms}] \tpct_black: " +
                     f"{pct_black:.1f} avg_return: {avg_return:.2f} ")       

     # final csv update
    if len(results_lst) > 0:
        logging.info(f"Writing {len(results_lst)} results to {analysis_file}")
        append_analysis_csv(analysis_file, results_lst, write_header)

def append_analysis_csv(csv_file, results_lst, write_header):

    # columns for output dataframe
    cols = ['symbol', 'num_trades', 'pct_black', 'num_black', 'num_red',
            'avg_return', 'avg_gain', 'max_gain', 'max_gain_date',
            'avg_loss', 'max_loss', 'max_loss_date']

    out_df = pd.DataFrame(results_lst, columns=cols).sort_values('pct_black',
                          ascending=False)

    with open(csv_file, 'a') as f:
        out_df.to_csv(f, index=False, sep=",", float_format='%.3f', 
                      header=write_header)
