import pandas as pd 
import numpy as np
import os.path
import sys
from math import floor
from datetime import datetime, timedelta
from lib.ntlogging import logging

def main():

    prices_input_file = "../data/stock_prices_grouped.csv"
    buy_sell_output_file = "../data/buy_sell.csv"
    budget_dollars = 2000.0
    fee_dollars = 10.0  # Fidelity charges $4.95 per trade


    try:
        logging.info("Reading: " + prices_input_file)
        stox_df = pd.read_table(prices_input_file, sep='\t')
        
    except Exception as e:
        logging.warning("Not parsed: " + prices_input_file + "\n" + str(e))
        sys.exit()

    logging.info("Calculating shares & profits columns...")
    stox_df['shares'] = np.floor(budget_dollars / stox_df['p0'])
    stox_df['profit'] = stox_df['deltap'] * stox_df['shares'] - fee_dollars

    # group and calculate percentage of profitable intervals
    cols = ['symbol', 'total_intervals', 'percent_black', 'num_black', 'num_red', 'average_gain', 
            'max_gain', 'max_loss']
    rowlst = [] # holder for row

    logging.info("Calculating interval stats...")
    stox_grps = stox_df.groupby('symbol')
    for symbol_name, symbol_df in stox_grps:
        num_intervals = len(symbol_df)
        profit_df = symbol_df.loc[symbol_df['profit'] > 0.0]
        num_black = len(profit_df) # number of profitable intervals
        percent_black = num_black / num_intervals
        num_red = num_intervals - num_black
        average_gain = symbol_df['profit'].mean()
        max_gain = symbol_df['profit'].max()
        max_loss = symbol_df['profit'].min()

        # format output
        percent_black = str(f"{percent_black:.2f}")
        average_gain = str(f"{average_gain:.2f}")
        max_gain = str(f"{max_gain:.2f}")
        max_loss = str(f"{max_loss:.2f}")

        rowlst.append([symbol_name, num_intervals, percent_black, num_black, 
                       num_red, average_gain, max_gain, max_loss])

    o_df = pd.DataFrame(rowlst, columns=cols).sort_values('percent_black',
                        ascending=False)
    logging.info("Shape of o_df: " + str(o_df.shape))
    logging.info("Writing output csv " + buy_sell_output_file)
    o_df.to_csv(buy_sell_output_file, index=False, sep=",")



if __name__ == '__main__':
    main()
    print("DONE\n")
