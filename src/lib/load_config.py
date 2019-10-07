import configparser
from lib.ntlogging import logging
    
def load_config():

    # Config parser
    ini_filename = "stox.ini"
    logging.info("Reading config from: " + ini_filename)
    config = configparser.ConfigParser()
    try:
        config.read(ini_filename)
    except Exception as e: 
        logging.critical("Error reading .ini file: " + ini_filename)
        logging.critical("Exception: " + str(type(e)) + " " + str(e))
        sys.exit()


    return config

def save_config(config):
    
    ini_filename = "stox.ini"
    with open(ini_filename, 'w') as configfile:
        config.write(configfile)
        logging.info("Saved " + ini_filename)

