import pandas as pd 
import numpy as np
import os.path
import sys
import configparser
from math import floor
from datetime import datetime, timedelta
from lib.ntlogging import logging
import pathlib
from pathlib import Path
from lib.filter_symbols import *
from lib.filter_prices import *
from lib.buy_sell_v3 import *
from lib.sort_symbols_by_eps import *
from lib.stox_utils import *
from lib.analyze import *
from lib.plot_price import *
from lib.clean_prices import *
from lib.test_cleaner import *
from lib.make_blacklist import *


def get_symbol_file_rows(cfg):

    symbol_file = SYMBOLS_FILE

    if os.path.exists(symbol_file):
        symbol_df = pd.read_table(symbol_file, sep=',')
        return len(symbol_df)
    else:
        return 0


def write_symbols(cfg):
    logging.info("Running symbols filter...")
    sort_symbols_by_eps(cfg)
    input("OK >")


def run_prices_filter(cfg):
    logging.info("Running prices filter...")
    filter_prices(cfg)
    input("OK >")


def run_buy_sell(cfg):
    logging.info("Running buy-sell...")
    buy_sell_v3(cfg)
    input("OK >")


def rm_stoxdir(cfg):
    stox_dir = STOX_DATA_DIR
    for p in Path(stox_dir).glob("*.*"):
        p.unlink()
    logging.info("Removed stox data.")
    input("OK >")


def run_analysis(cfg):
    logging.info("Running analysis...")
    analyze(cfg)
    input("OK >")


def run_buy_sell_analyze(cfg):

    holds_cfg = cfg['hold_times_list'].strip()
    budget_cfg = cfg['budget_list'].strip()

    holds_lst = holds_cfg.split(",")
    budget_lst = budget_cfg.split(",")

    for budget in budget_lst:
        b = float(budget.strip())

        for hold in holds_lst:
            h = int(hold.strip())

            cfg['stock_hold_time'] = str(h)
            cfg['budget_dollars'] = str(b)

            logging.info(f"Running buy-sell with {h} days and {b} dollars...")
            buy_sell_v3(cfg)
            logging.info("Running analyze with {h} days and {b} dollars...")
            analyze(cfg)

    input("OK > ")


def run_price_plot(cfg):
    logging.info("Running price plot...")
    plot_price(cfg)


def run_clean_prices(cfg):
    logging.info(f"Cleaning with window = {cfg['rolling_sample_window']}")
    clean_prices(cfg)
    input("OK >")


def run_cleaner_test(cfg):
    logging.info("Running cleaner test...")
    test_cleaner(cfg)
    

def run_make_blacklist(cfg):
    logging.info("Running blacklist...")
    make_blacklist(cfg)


def main():

    # make the output dir if needed
    stox_dir = STOX_DATA_DIR
    pathlib.Path(stox_dir).mkdir(exist_ok=True)

    reply = "none"
    while reply != "q":

        # pass the last reply in to menu to show last command
        reply, cfg = show_menu(reply)

        if reply == '0':
            logging.shutdown()
            if os.path.exists("log-stox.log"):
                os.remove("log-stox.log")
            logging.info("Log deleted.")
            input("OK >")

        elif reply == '1':
            rm_stoxdir(cfg)

        elif reply == "2":
            run_clean_prices(cfg)

        elif reply == "3":
            write_symbols(cfg)

        elif reply == "4":
            run_prices_filter(cfg)

        elif reply == "5":
            run_buy_sell(cfg)

        elif reply == "6":
            run_analysis(cfg)

        elif reply == "7":
            run_price_plot(cfg)

        elif reply == "8":
            run_cleaner_test(cfg)

        elif reply == "9":
            run_buy_sell_analyze(cfg)

        elif reply == "10":
            run_make_blacklist(cfg)


# Kaggle:
# https://www.kaggle.com/tsaustin/us-historical-stock-prices-with-earnings-data

def show_menu(previous):

    config = load_config()
    cfg = config['stox']
    # update_symbol_count(cfg)
    num_symbols = get_symbol_file_rows(cfg)

    # menu
    prompt = "\n---- STOX MENU ----"

    prompt += "\n\nRaw data: 1998-01-02 to 2019-08-09 (7091 symbols)"
    prompt += f"\nSymbols limit: {cfg['symbols_limit']}"
    prompt += f"\nSpan: {cfg['date_start']} to {cfg['date_end']}"
    prompt += f"\nhold (days): {cfg['stock_hold_time']}"
    prompt += f"\nbudget: {cfg['budget_dollars']}"
    prompt += f"\nlow price cutoff: {cfg['low_price_cutoff']}"
    prompt += f"\nrolling window {cfg['rolling_sample_window']}"

    prompt += ("\nSymbols in current symbol file: " + 
                str(num_symbols) + "\n")

    prompt += "\nCommands:"           
    prompt += "\n0) Delete the log"
    prompt += "\n1) Delete generated data"
    prompt += "\n2) Process raw prices file (clean outliers)"
    prompt += "\n3) Update symbols file (filter by date & limit)"
    prompt += "\n4) Filter prices list (dates, symbols)"
    prompt += "\n5) Run buy-sell process"
    prompt += "\n6) Analyze buy-sell results"
    prompt += "\n7) Plot prices: " + cfg['plot_params']
    prompt += "\n8) Run cleaner test: " + cfg['cleaner_test_params']
    prompt += "\n9) Run buy-sell + analyze matrix"
    prompt += "\n10) Make blacklist"
    prompt += "\nq) Quit"
    prompt += "\nLast command was: " + str(previous)
    prompt += "\nstox > "
    reply = input(prompt)

    return reply.strip(), cfg


if __name__ == '__main__':
    main()
    print("DONE\n")