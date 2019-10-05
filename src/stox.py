import pandas as pd 
import numpy as np
import os.path
import sys
import configparser
from math import floor
from datetime import datetime, timedelta
from lib.ntlogging import logging
from lib.filter_symbols import *
from lib.filter_prices import *
from lib.build_intervals import *
from lib.buy_sell import *


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


def set_use_all_symbols(current):
    prompt = "\nUse all symbols [" + current + "] > "
    reply = input(prompt).strip().lower()
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


def set_stock_hold_time(current):
    prompt = "\nStock hold time: [" + current + "] > "
    reply = input(prompt).strip()
    if len(reply) < 1: 
        return current
    else:
        return reply


def run_symbols_filter(cfg):
    logging.info("Running symbols filter...")
    filter_symbols(cfg)


def run_prices_filter(cfg):
    logging.info("Running prices filter...")
    filter_prices(cfg)


def run_intervals(cfg):
    logging.info("Running intervals...")
    build_intervals(cfg)


def run_buy_sell(cfg):
    logging.info("Running buy-sell...")
    buy_sell(cfg)


def main():

    # Config parser
    ini_filename = "stox.ini"
    #logging.info("Reading config from: " + ini_filename)
    config = configparser.ConfigParser()
    try:
        config.read(ini_filename)
    except Exception as e: 
        logging.critical("Error reading .ini file: " + ini_filename)
        logging.critical("Exception: " + str(type(e)) + " " + str(e))
        sys.exit()

    cfg = config['stox']

    reply = ""
    while reply != "q":
        reply = show_menu(cfg)
        if reply == "1":
            cfg['use_all_symbols'] = set_use_all_symbols(cfg['use_all_symbols'])
        elif reply == "2":
            cfg['symbols_limit'] = set_symbols_limit(cfg['symbols_limit'])
        elif reply == "3":
            run_symbols_filter(cfg)
        elif reply == "4":
            cfg['prices_date_start'] = set_prices_date_start(
                cfg['prices_date_start'])
        elif reply == "5":
            cfg['prices_date_end'] = set_prices_date_end(
                cfg['prices_date_end'])
        elif reply == "6":
            run_prices_filter(cfg)
        elif reply == "7":
            cfg['stock_hold_time'] = set_stock_hold_time(
                cfg['stock_hold_time'])
        elif reply == "8":
            run_intervals(cfg)
        elif reply == "9":
            cfg['budget_dollars'] = set_budget(
                cfg['budget_dollars'])
        elif reply == "10":
            cfg['tx_fee'] = set_fee(cfg['tx_fee'])
        elif reply == "11":
            run_buy_sell(cfg)
 
        elif reply == "12":
            with open(ini_filename, 'w') as configfile:
                config.write(configfile)
            logging.info("Saved " + ini_filename)

    
def show_menu(cfg):

    # menu
    prompt = "\n STOX MENU\n"
    prompt += "\n1) Set use-all-symbols: " + cfg['use_all_symbols']
    prompt += "\n2) Symbols limit: " + cfg['symbols_limit']
    prompt += "\n3) * Run symbol filter"
    prompt += "\n4) Set prices start: " + cfg['prices_date_start']
    prompt += "\n5) Set prices end: " + cfg['prices_date_end']
    prompt += "\n6) * Run prices filter"
    prompt += "\n7) Set hold interval: " + cfg['stock_hold_time']
    prompt += "\n8) * Run intervals"
    prompt += "\n9) Set budget: " + cfg['budget_dollars']
    prompt += "\n10) Set fee: " + cfg['tx_fee']
    prompt += "\n11) * Run buy-sell" 
    prompt += "\n12) Save config"
    prompt += "\nq) Quit"
    prompt += "\nstox > "
    reply = input(prompt)

    return reply.strip()




if __name__ == '__main__':
    main()
    print("DONE\n")