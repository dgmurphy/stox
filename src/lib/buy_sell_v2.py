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
    cols = ['symbol', 'interval', 'trading_days_held', 'cal_days_held',
            'buy_date', 'shares_bought', 'buy_price', 'sell_date', 
            'shares_sold', 'sell_price', 'gain_total']

    rowlist = []  # holder for df row
    numsyms = len(stox_df)  # total number of symbols
    symnum = 1  # symbol idx

    cant_afford = set()  # set of symbols whose unit share price exceeds budget
    penny_stocks = set()  # set of low price symbols

    # Loop over each symbol
    for symbol, sym_df in stox_df:

        logging.info("Processing symbol: " + symbol + 
                     "  [" + str(symnum) + " of " + str(numsyms) +"]")

        interval = 1
        etd = 0   #  elapsed trading days
        num_splits = 0
        buying = True 
        
        # loop until hold time elapses
        # TODO REFACTOR FOR SPEED
        # update shares owned based on splits until sell date
        for row in sym_df.itertuples():

            if buying:
                bdate = row.date  # buy date
                buy_price = (float(row.open) + float(row.close)) / 2.0

                # skip if buy price is very small 
                epsilon = .0001
                if buy_price > epsilon:
                    # can only buy whole shares
                    shares_bought = floor(budget_dollars / buy_price)
                    if shares_bought < 1:
                        cant_afford.add(symbol)
                        logging.info("Could not afford " + symbol)
                else:
                    shares_bought = 0
                    penny_stocks.add(symbol)
                    logging.info("Share price too low " + symbol + ": " + str(buy_price))
                
                cost_dollars = shares_bought * buy_price
                shares_owned = shares_bought
                buying = False
                #logging.info(f"{bdate} bought {shares_bought} at {buy_price} for {cost_dollars}")
                
            split_coeff_str = f"{row.split_coefficient:.1}"
            if split_coeff_str != "1e+00":
                split_coeff = float(row.split_coefficient)
                shares_owned = shares_owned * split_coeff
                num_splits += 1

            cdate = row.date  # date in this row
            #logging.info(f"Date in this row {cdate}. Elapsed trading days: {etd}")
            etd += 1  # increment elapsed trading days

            # Sell when number of trading days > hold time
            if etd > hold_days:
                # sell
                cal_days = (cdate - bdate).days
                sell_price = (float(row.open) + float(row.close)) / 2.0
                sold_dollars = shares_owned * sell_price
                gain = sold_dollars - cost_dollars
                rowlist.append([symbol, interval, (etd - 1), cal_days, bdate,  
                                shares_bought, buy_price, cdate,
                                shares_owned, sell_price, gain])

                etd = 0  # reset elapsed training days
                buying = True
                interval += 1
                #logging.info(f"{cdate} sold {shares_owned} at {sell_price} gain: {gain}")
        
        if num_splits > 0:
            logging.info("# stock splits for " + symbol + ": " + str(num_splits) + "\n")

        symnum += 1    # keep track of how many symbols have been processed

    # build the output df
    logging.info("Building df...")
    o_df = pd.DataFrame(rowlist, columns=cols).sort_values(['symbol', 'interval'],
                        ascending=True)

    logging.info("Writing " + buy_sell_output_file)
    o_df.to_csv(buy_sell_output_file, index=False, sep=",")
    logging.info("Buy-sell output shape: " + str(o_df.shape))
    logging.info("Wrote buy-sell output file: " + buy_sell_output_file)
    logging.info("Zero shares bought (price exceeds budget): " + str(cant_afford))
    logging.info("Zero shares bought (price too low): " + str(penny_stocks))

if __name__ == '__main__':

    cfg = load_config()
    buy_sell_v2(cfg)
    print("DONE\n")
