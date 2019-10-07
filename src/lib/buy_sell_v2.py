import pandas as pd 
import numpy as np
import os.path
import sys
from math import floor
from datetime import datetime, timedelta
from lib.ntlogging import logging
from lib.load_config import load_config


def buy_sell_v2(cfg):

    prices_input_file = (cfg['stox_data_dir'] + cfg['daily_prices_file'])
    buy_sell_output_file = cfg['stox_data_dir'] + cfg['buy_sell_results']
    budget_dollars = float(cfg['budget_dollars'])
    fee_dollars = float(cfg['tx_fee'])
    hold_days = int(cfg['stock_hold_time'])

    # load prices and group by symbol
    try:
        logging.info("Reading: " + prices_input_file)
        stox_df = pd.read_table(prices_input_file, sep=',')
        stox_df['date'] = pd.to_datetime(stox_df['date'])
        stox_df = stox_df.groupby('symbol')
        logging.info("Found " + str(len(stox_df)) + " symbols in price data.")
        
        
    except Exception as e:
        logging.warning("Not parsed: " + prices_input_file + "\n" + str(e))
        sys.exit()

    # columns for output dataframe
    cols = ['symbol', 'interval', 'days_held', 'buy_date', 
            'shares_bought', 'buy_price', 'sell_date', 
            'shares_sold', 'sell_price', 'gain_total']

    rowlist = []  # holder for df row

    # Loop over each symbol
    for symbol, sym_df in stox_df:

        interval = 1
        buying = True 
        
        # loop until hold time elapses
        # TODO REFACTOR FOR SPEED
        # update shares owned based on splits until sell date
        for row in sym_df.itertuples():

            if buying:
                bdate = row.date  # buy date
                buy_price = (float(row.open) + float(row.close)) / 2.0
                
                # can only buy whole shares
                shares_bought = floor(budget_dollars / buy_price)  # could be 0
                cost_dollars = shares_bought * buy_price
                shares_owned = shares_bought
                buying = False
                logging.info(f"{bdate} bought {shares_bought} at {buy_price} for {cost_dollars}")
                
            split_coeff_str = f"{row.split_coefficient:.1}"
            if split_coeff_str != "1e+00":
                split_coeff = float(row.split_coefficient)
                shares_owned = shares_owned * split_coeff
                logging.info("Stock split for " + symbol + " coeff: " + split_coeff_str)

            cdate = row.date  # date in this row

            # TODO this needs to be business days!
            ebd = (cdate - bdate).days    # days since buy date
            logging.info(f"Date in this row {cdate}. Elapsed days: {ebd}")

            if ebd >= (hold_days - 1):
                # sell
                sell_price = (float(row.open) + float(row.close)) / 2.0
                sold_dollars = shares_owned * sell_price
                gain = sold_dollars - cost_dollars
                rowlist.append([symbol, interval, ebd, bdate,  
                                shares_bought, buy_price, cdate,
                                shares_owned, sell_price, gain])
                buying = True
                interval += 1
                logging.info(f"{cdate} sold {shares_owned} at {sell_price} gain: {gain}")

    o_df = pd.DataFrame(rowlist, columns=cols).sort_values(['interval', 'symbol'],
                        ascending=True)

    o_df.to_csv(buy_sell_output_file, index=False, sep=",")
    logging.info("Buy-sell output shape: " + str(o_df.shape))
    logging.info("Wrote buy-sell output file: " + buy_sell_output_file)

if __name__ == '__main__':

    cfg = load_config()
    buy_sell_v2(cfg)
    print("DONE\n")
