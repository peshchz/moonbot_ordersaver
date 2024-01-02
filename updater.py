#%%
import os,sys
import configparser
import time
import random
from config_worker import MyConfig
import urllib.request
import traceback
import logging
from logging.config import dictConfig
import ssl

ssl._create_default_https_context = ssl._create_unverified_context
url = 'https://vseispravim.ru/test-file/ordersaver.exe'
file_name = 'ordersaver.exe'

config = MyConfig()

def logProblem(logger):
    try:
        logger.debug('logProblem')        
        err_txt = traceback.format_exc()
        logger.debug(err_txt)
        logger.debug('Stop with error')
    except:
        pass


log_config = {
    "version":1,
    "handlers":{
        "fileHandler":{
            "class":"logging.FileHandler",
            "formatter":"myFormatter",
            "filename":"update_log.txt"
        }
    },
    "loggers":{
        'errlog':{
            "handlers":["fileHandler"],
            "level":"DEBUG",
        }
    },
    "formatters":{
        "myFormatter":{
            "format":"%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        }
    }
}

dictConfig(log_config)
logger = logging.getLogger('errlog')

try:
    if config.config['main_data']['need_download_saver'] == '1':
        sleep = random.randint(1,int(config.config['main_data']['start_sleep_max_sec']))
        print('sleep',sleep)
        time.sleep(sleep)
        urllib.request.urlretrieve(url, file_name)
        config.changeConfig('main_data','need_download_saver',0)
        config.changeConfig('main_data','need_update_version_in_bd',1)
        logger.debug('New version downloaded')
        print('Downloaded')
except:
    logProblem(logger)

# %%
