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
from lib.stox_utils import load_config, save_config
from lib.analyze import *
from lib.plot_price import *
from lib.clean_prices import *


def set_budget(current):
    prompt = "\nBudget [" + current + "] > "
    reply = str(input(prompt).strip())   
    if len(reply) < 1: 
        return current
    else:
        return reply


def set_fee(current):
    prompt = "\nFee [" + current + "] > "
    reply = str(input(prompt).strip())
    if len(reply) < 1: 
        return current
    else:
        return reply

def set_low_price_cutoff(current):
    prompt = "\nLow price cutoff [" + current + "] > "
    reply = str(input(prompt).strip())
    if len(reply) < 1: 
        return current
    else:
        return reply


def set_prices_date_start(current):
    prompt = "\nPrices start date: [" + current + "] > "
    reply = input(prompt).strip()
    if len(reply) < 1: 
        return current
    else:
        return reply


def set_prices_date_end(current):
    prompt = "\nPrices end date: [" + current + "] > "
    reply = input(prompt).strip()
    if len(reply) < 1: 
        return current
    else:
        return reply


def set_stock_hold_time(current):
    prompt = "\nStock hold time: [" + current + "] > "
    reply = input(prompt).strip()
    if len(reply) < 1: 
        return current
    else:
        return reply

def set_symbols_limit(current):
    prompt = "\nSymbols limit [" + current + "] > "
    reply = input(prompt).strip()
    if len(reply) < 1: 
        return current
    else:
        return reply


def set_plot_params(current):
    prompt = "\nPlot params [" + current + "] > "
    reply = input(prompt).strip()
    if len(reply) < 1: 
        return current
    else:
        return reply


def get_symbol_file_rows(cfg):

    symbol_file = cfg['stox_data_dir'] + cfg['symbols_file']

    if os.path.exists(symbol_file):
        symbol_df = pd.read_table(symbol_file, sep=',')
        return len(symbol_df)
    else:
        return 0

def update_symbol_count(cfg):
    sym_list = filter_symbols(cfg)
    cfg['num_symbols_in_window'] = str(len(sym_list))


def write_symbols(cfg):
    logging.info("Running symbols filter...")
    sort_symbols_by_eps(cfg)
    input("OK >")


def run_prices_filter(cfg):
    logging.info("Running prices filter...")
    filter_prices(cfg)
    input("OK >")


def run_intervals(cfg):
    logging.info("Running intervals...")
    build_intervals(cfg)
    input("OK >")


def run_buy_sell(cfg):
    logging.info("Running buy-sell...")
    buy_sell_v3(cfg)
    input("OK >")


def rm_stoxdir(cfg):
    stox_dir = cfg['stox_data_dir'] 
    for p in Path(stox_dir).glob("*.csv"):
        p.unlink()
    logging.info("Removed stox data.")
    input("OK >")


def run_analysis(cfg):
    logging.info("Running analysis...")
    analyze(cfg)
    input("OK >")

def run_price_plot(cfg):
    logging.info("Running price plot...")
    plot_price(cfg)

def run_clean_prices(cfg):
    logging.info("Running prices cleaner...")
    clean_prices(cfg)


def main():

    config = load_config()
    cfg = config['stox']
    update_symbol_count(cfg)

    # make the output dir if needed
    stox_dir = cfg['stox_data_dir']
    pathlib.Path(stox_dir).mkdir(exist_ok=True)


    reply = "none"
    while reply != "q":

        # pass the last reply in to menu to show last command
        reply = show_menu(cfg, reply)

        if reply == '0':
            logging.shutdown()
            if os.path.exists("log-stox.log"):
                os.remove("log-stox.log")
            logging.info("Log deleted.")

        elif reply == '1':
            rm_stoxdir(cfg)

        elif reply == "2":
            cfg['date_start'] = set_prices_date_start(
                cfg['date_start'])
            update_symbol_count(cfg)

        elif reply == "3":
            cfg['date_end'] = set_prices_date_end(
                cfg['date_end'])
            update_symbol_count(cfg)
         
        elif reply == "4":
            cfg['symbols_limit'] = set_symbols_limit(cfg['symbols_limit'])

        elif reply == "5":
            write_symbols(cfg)

        elif reply == "6":
            run_prices_filter(cfg)

        elif reply == "7":
            cfg['stock_hold_time'] = set_stock_hold_time(
                cfg['stock_hold_time'])

        elif reply == "8":
            cfg['budget_dollars'] = set_budget(
                cfg['budget_dollars'])

        elif reply == "9":
            cfg['tx_fee'] = set_fee(cfg['tx_fee'])

        elif reply == "10":
            cfg['low_price_cutoff'] = set_low_price_cutoff(
                cfg['low_price_cutoff'])

        elif reply == "11":
            run_buy_sell(cfg)

        elif reply == "12":
            run_analysis(cfg)

        elif reply == "13":
            cfg['plot_params'] = set_plot_params(
                cfg['plot_params'])

        elif reply == "14":
            run_price_plot(cfg)

        elif reply == "15":
            run_clean_prices(cfg)


    # save before exit
    save_config(config)


# Kaggle:
# https://www.kaggle.com/tsaustin/us-historical-stock-prices-with-earnings-data

def show_menu(cfg, previous):

    num_symbols = get_symbol_file_rows(cfg)

    # menu
    prompt = "\n---- STOX MENU ----"

    prompt += "\n\n First date in data set: 1998-01-02"
    prompt += "\n Last date in data set: 2019-08-09"
    prompt += "\n Available stock symbols in entire data set: 7091\n"
    
    prompt += "\n Current window start date: " + cfg['date_start']
    prompt += "\n Current window end date: " + cfg['date_end']
    prompt += ("\n Available symbols spanning date window: " + 
               cfg['num_symbols_in_window'] + "\n")
    prompt += ("\n # symbols in current symbol file: " + 
                str(num_symbols) + "\n")

    prompt += "\nCommands:"           
    prompt += "\n0) Delete the log"
    prompt += "\n1) Delete generated data"
    prompt += "\n2) Change window start date: " + cfg['date_start']
    prompt += "\n3) Change window end date: " + cfg['date_end']
    prompt += "\n4) Change symbols limit (n highest EPS): " + cfg['symbols_limit']
    prompt += "\n5) Update symbols file"
    prompt += "\n6) Filter prices list"
    prompt += "\n7) Set hold interval (days): " + cfg['stock_hold_time']   
    prompt += "\n8) Change purchasing budget: " + cfg['budget_dollars']
    prompt += "\n9) Change transaction fee: " + cfg['tx_fee']
    prompt += "\n10) Change low-price cutoff: " + cfg['low_price_cutoff']
    prompt += "\n11) Run buy-sell process"
    prompt += "\n12) Analyze buy-sell results"
    prompt += "\n13) Change plot params: " + cfg['plot_params']
    prompt += "\n14) Plot prices"
    prompt += "\n15) Clean raw prices file"
    prompt += "\nq) Quit"
    prompt += "\nLast command was: " + str(previous)
    prompt += "\nstox > "
    reply = input(prompt)

    return reply.strip()


if __name__ == '__main__':
    main()
    print("DONE\n")