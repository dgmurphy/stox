import pandas as pd 
import numpy as np
import os.path
import sys
from math import floor
from datetime import datetime, timedelta
from lib.ntlogging import logging
from lib.load_config import load_config

# This builds a result row for every trading day in the set.
# Transactions that are pending sale until the hold period is reached
# are put into the pending_lst.
# A sale will close out the first item from the pending list, adjust the shares
# owned at the sell date by the split coefficients, and append the sale
# to  the results_lst.  The final dataframe is built from the results_lst.
def buy_sell_v3(cfg):

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
    
    results_lst = []   # completed transactions
    numsyms = len(stox_df)  # total number of symbols
    cant_afford = set()  # set of symbols whose unit share price exceeds budget
    penny_stocks = set()  # set of low price symbols

    # Loop over each symbol
    symnum = 1  # symbol idx
    for symbol, sym_df in stox_df:

        logging.info("Processing symbol: " + symbol + 
                     "  [" + str(symnum) + " of " + str(numsyms) +"] " +
                     " # trading days: " + len(sym_df))

        pending_lst = []   # buy attributes for each buy date
        row_idx = 0
        for row in sym_df.itertuples():

            buy_price, shares_bought, status = buy(row, budget_dollars)
            split_coeff = row.split_coefficient

            if status == 'price_high':
                cant_afford.add(symbol)  # todo: count these
            elif status == 'price_low':
                penny_stocks.add(symbol) # todo: count these
            
            # add current row to pending sale list
            pending_lst.append([symbol, row_idx, row.date, shares_bought, 
                                buy_price, split_coeff])
            
            # Once the pending sales list is full, start creating results list
            if row_idx >= hold_days:  

                result_row = sell_row(row, pending_lst, results_lst)
                # add the result row to results list
                results_lst.append(result_row)
                # remove the sold row 
                del pending_lst[0]

            row_idx += 1

        symnum += 1    # keep track of how many symbols have been processed

    # build the output df
    logging.info(f"Processed {symnum} symbols. Writing df...")
    o_df = pd.DataFrame(results_lst, columns=cols).sort_values(['symbol', 'interval'],
                        ascending=True)

    logging.info("Writing " + buy_sell_output_file)
    o_df.to_csv(buy_sell_output_file, index=False, sep=",", float_format='%.3f')
    logging.info("Buy-sell output shape: " + str(o_df.shape))
    logging.info("Wrote buy-sell output file: " + buy_sell_output_file)
    logging.info("Zero shares bought (price exceeds budget): " + str(cant_afford))
    logging.info("Zero shares bought (price too low): " + str(penny_stocks))


def sell_row(row, pending_lst, results_lst):

    sold_row = pending_lst[0]
    symbol = sold_row[0]
    idx = sold_row[1]
    buy_date = sold_row[2]
    shares_bought = sold_row[3]
    buy_price = sold_row[4]
    cost_dollars = shares_bought * buy_price

    # update num shares owned 
    shares_owned = shares_bought
    split_coeff = 1  # default, shouldnt need
    for pending_row in pending_lst[1:]:
        split_coeff = pending_row[5]
        split_coeff_str = f"{split_coeff:.1}"    
        if split_coeff_str != "1e+00":
            shares_owned = shares_owned * split_coeff

    sell_date = row.date
    sell_price = (float(row.open) + float(row.close)) / 2.0
    sold_dollars = shares_owned * sell_price

    cal_days = (sell_date - buy_date).days
    trading_days = len(pending_lst) - 1
    gain_total = sold_dollars - cost_dollars

    result_row = [symbol, idx, trading_days, cal_days,
                  buy_date, shares_bought, buy_price,
                  sell_date, shares_owned, sell_price,
                  gain_total]
    
    logging.info("Result row " + str(result_row))

    return result_row

    
def buy(row, budget_dollars):
    
    status = "ok"   # 'ok' if shares bought
                    # 'price_high' if too expensive
                    # 'price_low' if too cheap

    buy_price = (float(row.open) + float(row.close)) / 2.0
    shares_bought = 0

    # share lower price limit
    epsilon = .0001
    if buy_price < epsilon:
        status = "price_low"
    else:
        # can only buy whole shares
        shares_bought = floor(budget_dollars / buy_price)
        if shares_bought < 1:
            status = "price_high"
    
    return buy_price, shares_bought, status
    


if __name__ == '__main__':

    cfg = load_config()
    buy_sell_v3(cfg)
    print("DONE\n")
