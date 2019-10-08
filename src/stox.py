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
from lib.build_intervals import *
from lib.buy_sell_v2 import *
from lib.sort_symbols_by_eps import *
from lib.load_config import *


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
    buy_sell_v2(cfg)
    input("OK >")


def rm_stoxdir(cfg):
    stox_dir = cfg['stox_data_dir'] 
    for p in Path(stox_dir).glob("*.csv"):
        p.unlink()
    logging.info("Removed stox data.")
    input("OK >")


def main():

    config = load_config()
    cfg = config['stox']
    update_symbol_count(cfg)

    # make the output dir if needed
    stox_dir = cfg['stox_data_dir']
    pathlib.Path(stox_dir).mkdir(exist_ok=True)


    reply = ""
    while reply != "q":
        reply = show_menu(cfg)
        if reply == "1":
            cfg['date_start'] = set_prices_date_start(
                cfg['date_start'])
            update_symbol_count(cfg)

        elif reply == "2":
            cfg['date_end'] = set_prices_date_end(
                cfg['date_end'])
            update_symbol_count(cfg)
          
        elif reply == "3":
            cfg['symbols_limit'] = set_symbols_limit(cfg['symbols_limit'])

        elif reply == "4":
            write_symbols(cfg)

        elif reply == "5":
            run_prices_filter(cfg)

        elif reply == "6":
            cfg['stock_hold_time'] = set_stock_hold_time(
                cfg['stock_hold_time'])

        elif reply == "7":
            run_intervals(cfg)

        elif reply == "8":
            cfg['budget_dollars'] = set_budget(
                cfg['budget_dollars'])

        elif reply == "9":
            cfg['tx_fee'] = set_fee(cfg['tx_fee'])

        elif reply == "10":
            run_buy_sell(cfg)
 
        elif reply == "11":
            save_config(config)

        elif reply == '12':
            rm_stoxdir(cfg)

        elif reply == '13':
            if os.path.exists("log-stox.log"):
                os.remove("log-stox.log")
    
def show_menu(cfg):

    num_symbols = get_symbol_file_rows(cfg)

    # menu
    prompt = "\n---- STOX MENU ----"

    prompt += "\n\n First date in data set: TBD"
    prompt += "\n Last date in data set: TBD"
    prompt += "\n Available stock symbols in entire data set: 7091\n"
    
    prompt += "\n Current window start date: " + cfg['date_start']
    prompt += "\n Current window end date: " + cfg['date_end']
    prompt += ("\n Available stock symbols in current date window: " + 
               cfg['num_symbols_in_window'] + "\n")
    prompt += ("\n # symbols in current symbol file: " + 
                str(num_symbols) + "\n")
               

    prompt += "\n1) Change window start date: " + cfg['date_start']
    prompt += "\n2) Change window end date: " + cfg['date_end']
    prompt += "\n3) Change symbols limit (n highest EPS): " + cfg['symbols_limit']
    prompt += "\n4) Update symbols file"
    prompt += "\n5) Filter daily prices list"
    prompt += "\n6) Set hold interval: " + cfg['stock_hold_time']   
    prompt += "\n7) NA"
    prompt += "\n8) Change purchasing budget: " + cfg['budget_dollars']
    prompt += "\n9) Change transaction fee: " + cfg['tx_fee']
    prompt += "\n10) Run buy-sell process"
    prompt += "\n11) Save config"
    prompt += "\n12) Delete generated data"
    prompt += "\n13) Delete the log"
    prompt += "\nq) Quit"
    prompt += "\nstox > "
    reply = input(prompt)

    return reply.strip()




if __name__ == '__main__':
    main()
    print("DONE\n")