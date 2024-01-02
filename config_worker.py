import os,sys
import configparser
import time
import random
from datetime import timedelta
from datetime import datetime as dtm
#%%
class MyConfig():
    def __init__(self):        
        self.config = configparser.ConfigParser()
        self.checkFile()
        self.parseBasesInfo()

    def checkFile(self):
        print('config path',os.path.exists('config.ini'))      
        if not os.path.exists('config.ini'):
            print('New config created!')
            self.config['main_data'] = {
                'user_id':1,
                'site':'b',
                'local_bd_rows_limit':100,
                'remote_bd_rows_limit':30,
                'start_sleep_max_sec':10,
                'send_data_sleep_sec':1,
                'test':0,
                'stop':0,
            }
            self.saveConfig()
        else:
            self.config.read('config.ini')
        
    def parseBasesInfo(self):
        self.bases = {}
        for seq in self.config.sections():
            if seq in ['main_data']:
                continue
            self.bases[seq] = {}
            for k,v in self.config[seq].items():
                self.bases[seq][k] = v
            if 'last_date' not in self.bases[seq]:
                self.bases[seq]['last_date'] = self.getDateForNewParse()

    def getDateForNewParse(self):
        #return int((dtm.now() - timedelta(days=80)).timestamp())
        return int(dtm(2021,3,15).timestamp())

    def saveConfig(self):
        with open('config.ini', 'w') as configfile:
            self.config.write(configfile)

    def changeConfig(self,bot_name,param,value):
        self.config[bot_name][param] = str(value) 
        self.saveConfig()
