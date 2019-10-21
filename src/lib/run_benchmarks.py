import pandas as pd 
import numpy as np
import os.path
import sys
from math import floor
from datetime import datetime, timedelta
from lib.ntlogging import logging
from lib.stox_utils import (BENCHMARK_PRICES_FILE, 
    BENCHMARK_RESULTS_FILE, BENCH_ANALYSIS_FILE_PREFIX,
    STOX_DATA_DIR, BENCH_BLACKLIST_FILE_PREFIX)


def run_benchmarks(cfg):

    holds_cfg = cfg['hold_times_list'].strip()
    budget_cfg = cfg['budget_list'].strip()

    holds_lst = holds_cfg.split(",")
    budget_lst = budget_cfg.split(",")

    # run the buy-sell analysis for each budget and hold time
    for budget in budget_lst:
        b = float(budget.strip())

        for hold in holds_lst:
            h = int(hold.strip())

            cfg['stock_hold_time'] = str(h)
            cfg['budget_dollars'] = str(b)

            logging.info(f"Running buy-sell with {h} days and {b} dollars...")
            buy_sell_benchmarks(cfg)
            logging.info("Running analyze with {h} days and {b} dollars...")
            analyze_benchmarks(cfg)

    # build the blacklist for the benchmarks (one row per symbol)
    make_bench_blacklist(cfg)

def buy_sell_benchmarks(cfg):

    prices_input_file = BENCHMARK_PRICES_FILE
    #buy_sell_output_file = BENCHMARK_RESULTS_FILE
    budget_dollars = float(cfg['budget_dollars'])
    fee_dollars = float(cfg['tx_fee'])
    hold_days = int(cfg['stock_hold_time'])
    low_price_cutoff = float(cfg['low_price_cutoff'])

    # clean up the existing output file (ignore !exists error)
    try:
        os.remove(BENCHMARK_RESULTS_FILE)
    except OSError:
        pass   

    # load prices 
    try:
        logging.info("Reading: " + prices_input_file)
        stox_df = pd.read_table(prices_input_file, sep=',')
        stox_df['date'] = pd.to_datetime(stox_df['date'])
        
    except Exception as e:
        logging.warning("Not parsed: " + prices_input_file + "\n" + str(e))
        sys.exit()
    
    # Filter by date
    start_list = cfg['date_start'].split('-')
    start_yr = int(start_list[0])
    start_mo = int(start_list[1])
    start_d = int(start_list[2])

    end_list = cfg['date_end'].split('-')
    end_yr = int(end_list[0])
    end_mo = int(end_list[1])
    end_d = int(end_list[2])

    prices_start_date = pd.Timestamp(start_yr, start_mo, start_d)
    prices_end_date = pd.Timestamp(end_yr, end_mo, end_d)

    # filter on date range
    logging.info("Filtering by date range.")
    #prices_df['date'] = pd.to_datetime(prices_df['date'])
    stox_df = stox_df[(stox_df['date'] >= prices_start_date) &
                   (stox_df['date'] <= prices_end_date)].sort_values(
                   ['symbol', 'date'])
                   
    logging.info("Filtered dates df shape " + str(stox_df.shape))

    # group by symbol
    stox_df = stox_df.groupby('symbol')  
    logging.info("Found " + str(len(stox_df)) + " symbols in price data.") 

    numsyms = len(stox_df)  # total number of symbols
    cant_afford = set()  # set of symbols whose unit share price exceeds budget
    penny_stocks = set()  # set of low price symbols

    results_lst = []  # completed transactions
    write_header = True  # results file header (write once flag)

    # Loop over each symbol
    symnum = 0  # symbol idx
    
    for symbol, sym_df in stox_df:

        max_gain = max_loss = 0.0
        pending_lst = []   # has buy attributes for each buy date

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

                result_row = sell_row(row, pending_lst, fee_dollars)

                returns = float(result_row[11])

                # just for logging
                if returns > max_gain: max_gain = returns
                if returns < max_loss: max_loss = returns

                # add the result row to results list
                results_lst.append(result_row)
                
                # remove the sold row 
                del pending_lst[0]

            row_idx += 1

        # peridocially write the results list
        qmax = 100000
        if len(results_lst) >= qmax:
            logging.info(f"Writing {qmax} results to {BENCHMARK_RESULTS_FILE}")
            append_buysell_csv(BENCHMARK_RESULTS_FILE, results_lst, write_header, low_price_cutoff)
            write_header = False
            results_lst = []

        symnum += 1    # keep track of how many symbols have been processed
        logging.info(f"{symbol} \t\t[{symnum} of {numsyms}] \ttrade days: " +
                     f"{str(len(sym_df))} max_gain: {max_gain:.2f} " +
                     f"max_loss: {max_loss:.2f}")
 

    # final csv update
    if len(results_lst) > 0:
        logging.info(f"Writing {len(results_lst)} results to {BENCHMARK_RESULTS_FILE}")
        append_buysell_csv(BENCHMARK_RESULTS_FILE, results_lst, write_header, low_price_cutoff)
    
    logging.info("Zero shares bought (price exceeds budget): " + 
                 str(cant_afford))
    logging.info("Zero shares bought (price too low): " + 
                 str(penny_stocks))


# Drop unwanted rows and update csv
def append_buysell_csv(csv_file, results_lst, write_header, low_price_cutoff):

    # columns for output dataframe
    cols = ['symbol', 'interval', 'trading_days_held', 'cal_days_held',
            'buy_date', 'shares_bought', 'buy_price', 'sell_date', 
            'shares_sold', 'sell_price', 'fee', 'gain_total']

    out_df = pd.DataFrame(results_lst, columns=cols).sort_values(['symbol', 
                          'interval'], ascending=True)

    # Drop zero-shares transactions
    out_df = out_df[out_df.shares_bought > 0]

    # drop penny stock transactions
    out_df = out_df[out_df.buy_price > low_price_cutoff]

    with open(csv_file, 'a') as f:
        out_df.to_csv(f, index=False, sep=",", float_format='%.3f', 
                      header=write_header)


def sell_row(row, pending_lst, fee):

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
    gain_total = sold_dollars - cost_dollars - fee

    result_row = [symbol, idx, trading_days, cal_days,
                  buy_date, shares_bought, buy_price,
                  sell_date, shares_owned, sell_price,
                  fee, gain_total]

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
    

def analyze_benchmarks(cfg):

    hold_str = str(int(float(cfg['stock_hold_time'])))
    budget_dollars_str = str(int(float(cfg['budget_dollars'])))

    analysis_postfix = hold_str + "_days_" + budget_dollars_str + "_dollars.csv"

    analysis_output_file = BENCH_ANALYSIS_FILE_PREFIX + analysis_postfix

    # clean up the existing output file (ignore !exists error)
    try:
        os.remove(analysis_output_file)
    except OSError:
        pass   

    # load buy-sell results and group by symbol
    try:
        logging.info(f"Reading {BENCHMARK_RESULTS_FILE}")
        bsr_df = pd.read_table(BENCHMARK_RESULTS_FILE, sep=",")
        #bsr_df['buy_date'] = pd.to_datetime(bsr_df['buy_date'])
        #bsr_df['sell_date'] = pd.to_datetime(bsr_df['sell_date'])
        bsr_df = bsr_df.groupby('symbol')
        logging.info("Found " + str(len(bsr_df)) + " symbols in buy-sell data.")
    
    except Exception as e:
        logging.warning("Not parsed: " + BENCHMARK_RESULTS_FILE + "\n" + str(e))
        sys.exit()

    results_lst = []  # output rows
    qmax = 100000   # output queue max
    write_header = True  # results file header (write once flag)
    symnum = 0
    numsyms = len(bsr_df)
    
    for symbol, sym_df in bsr_df:

        try:

            num_trades = len(sym_df)
            blk_trades_df = sym_df[sym_df['gain_total'] > 0.0]
            red_trades_df = sym_df[sym_df['gain_total'] < 0.0]
            num_black = len(blk_trades_df)
            pct_black = float(num_black) / float(num_trades)
            num_red = len(red_trades_df)

            avg_return = sym_df["gain_total"].mean()
            avg_gain = blk_trades_df["gain_total"].mean()
            avg_loss = red_trades_df["gain_total"].mean()

            max_gain = blk_trades_df["gain_total"].max()
            mg_idx = blk_trades_df['gain_total'].idxmax()
            mg_buy_date = blk_trades_df.loc[mg_idx, 'buy_date']
            mg_buy_price = blk_trades_df.loc[mg_idx, 'buy_price']
            mg_sell_date = blk_trades_df.loc[mg_idx, 'sell_date']
            mg_sell_price = blk_trades_df.loc[mg_idx, 'sell_price']
            
            
            max_loss = red_trades_df["gain_total"].min()
            ml_idx = red_trades_df['gain_total'].idxmin()
            ml_buy_date = red_trades_df.loc[ml_idx, 'buy_date']
            ml_buy_price = red_trades_df.loc[ml_idx, 'buy_price']
            ml_sell_date = red_trades_df.loc[ml_idx, 'sell_date']
            ml_sell_price = red_trades_df.loc[ml_idx, 'sell_price']

            row = [ symbol, 
                    num_trades, 
                    pct_black, 
                    num_black, 
                    num_red, 
                    avg_return,
                    avg_gain, 
                    avg_loss, 
                    max_gain, 
                    mg_buy_date, 
                    mg_buy_price,
                    mg_sell_date, 
                    mg_sell_price, 
                    max_loss, 
                    ml_buy_date,
                    ml_buy_price, 
                    ml_sell_date, 
                    ml_sell_price]

            # drop low numbers of trades
            if num_trades >= int(cfg['analyze_min_trades']):
                results_lst.append(row)
            else:
                logging.info(f"dropped symbol {symbol} for low trade occurrences.")

            # peridocially write the results list
            if len(results_lst) >= qmax:
                logging.info(f"Writing {qmax} results to {analysis_output_file}")
                append_analysis_csv(analysis_output_file, results_lst, write_header)
                write_header = False
                results_lst = []

            symnum += 1    # keep track of how many symbols have been processed
            logging.info(f"{symbol} \t\t[{symnum} of {numsyms}] \tpct_black: " +
                        f"{pct_black:.1f} avg_return: {avg_return:.2f} ")    

        except Exception as e:
            logging.error("Exception in analyze " + str(e))

    # final csv update
    if len(results_lst) > 0:
        logging.info(f"Writing {len(results_lst)} results to {analysis_output_file}")
        append_analysis_csv(analysis_output_file, results_lst, write_header)


def append_analysis_csv(csv_file, results_lst, write_header):

    # columns for output dataframe
    cols = ['symbol', 
            'num_trades', 
            'pct_black', 
            'num_black', 
            'num_red',
            'avg_return', 
            'avg_gain', 
            'avg_loss',
            'max_gain', 
            'mg_buy_date', 
            'mg_buy_price',
            'mg_sell_date', 
            'mg_sell_price',
            'max_loss',
            'ml_buy_date',
            'ml_buy_price',
            'ml_sell_date',
            'ml_sell_price']


    out_df = pd.DataFrame(results_lst, columns=cols).sort_values('pct_black',
                          ascending=False)

    with open(csv_file, 'a') as f:
        out_df.to_csv(f, index=False, sep=",", float_format='%.3f', 
                      header=write_header)


def make_bench_blacklist(cfg):

    file_list = [
        'bench_analysis_4_days_1000_dollars.csv',
        'bench_analysis_9_days_1000_dollars.csv',
        'bench_analysis_14_days_1000_dollars.csv',
        'bench_analysis_19_days_1000_dollars.csv',
        'bench_analysis_30_days_1000_dollars.csv',
        'bench_analysis_60_days_1000_dollars.csv',
        'bench_analysis_90_days_1000_dollars.csv'
    ]
    df_list = []  # keep each df 

    logging.info(f"Loading {file_list[0]}")
    df = pd.read_table(STOX_DATA_DIR + file_list[0], sep=",")
    logging.info(f"file0 df shape: {df.shape}")

    df_list.append(df)

    keep_symbols = set(df['symbol'].tolist())
    drop_symbols = set()

    for file in file_list[1:]:
        logging.info(f"Checking symbols in {file}")
        df = pd.read_table(STOX_DATA_DIR + file, sep=",")
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

    bl_file = BENCH_BLACKLIST_FILE_PREFIX + "1000_dollars.csv"
    bl_df.to_csv(bl_file, index=False, float_format='%.3f')