#%%
#%load_ext autoreload
#%autoreload 2
import os,sys
from sqlite3.dbapi2 import Timestamp
from datetime import timedelta
from datetime import datetime as dtm
import configparser
import sqlite3
import pymysql
import time
import random
import logging
from logging.config import fileConfig
import traceback
import json

from settings import server_data#,bases,main_data
from config_worker import MyConfig
from strat_saver import ParseStratFile
from server_connector import Server
from local_base_connector import LocalBase
from local_alex_base_saver import saveOrdersToLocalAlexBase,UpdateSaleData
from local_alex_base_connector import LocalAlexBase
from log_writer import ParseLogFile

#%%
def logProblem(logger,bot_name=''):
    try:
        logger.debug('logProblem')
        logger.debug(bot_name)       
        err_txt = traceback.format_exc()
        logger.debug(err_txt)
        logger.debug('Stop with error')
    except:
        pass

def closeServerConnection(logger,serv):
    try:
        serv.cur.close()
        serv.con.close()
        if hasattr(serv,'r'):
            logger.debug(serv.r)
    except:
        pass

def closeProgramm(config,logger_err):
    config.changeConfig('main_data','stop',0)
    logger_err.debug('Close programm with error')
    sys.exit()

def randomSleep(sleep_max_sec):
    sleep = random.randint(1,int(sleep_max_sec))
    print('sleep',sleep)
    time.sleep(sleep)

class Worker():
    def __init__(self,bot_name,config,serv,logger,logger_err):        
        self.bot_name = bot_name
        self.config = config
        self.serv = serv
        self.logger = logger
        self.logger_err = logger_err
        self.need_reload_txt_log = False
        self.alex_bd_path = config.config['main_data'].get('alex_bd_path',None)

    def reloadAllData(self):
        if self.serv.need_reload_all_data == True:
            self.logger_err.debug(f'need_reload_all_data {self.bot_name}')
            self.logger_err.debug('UpdateSaleData addDataInDf start')
            update = UpdateSaleData(self.bot_name,self.alex_bd_path)
            update.addDataInDf()
            update.createNullClosedLists()
            self.orders_data = update.null_close_df
            moon_bd_data = []
            for self.moon_col_name,self.id_list in update.null_close_date_id.items():
                if len(self.id_list) == 0:
                    continue
                self.logger_err.debug(f'UpdateSaleData find nulled sell {self.moon_col_name}')
                json_id = json.dumps(self.id_list)
                self.logger_err.debug(f'id {json_id}')
                self.readMoonBotBD(base['file'],False)
                moon_bd_data.extend(self.moon_bd)
            if len(moon_bd_data) > 0:
                self.moon_bd = moon_bd_data
                len_bd = len(moon_bd_data)
                self.logger_err.debug(f'UpdateSaleData addDataFromMoonToNulledClose')
                self.logger_err.debug(f'total moon rows {len_bd}')
                self.addDataFromMoonToNulledClose()
                update.null_close_df = self.orders_data
            self.orders_data = []
            self.logger_err.debug('UpdateSaleData updateDataInBD start')
            update.updateDataInBD()
            update.getFirstDate()
            update.l.closeConnections()
            self.logger_err.debug('Data to alex bd updated!')
            first_date = update.first_date# - timedelta(seconds=1)
            self.serv.delAllBotDataInBd(first_date)
            self.logger_err.debug('Data from servers bd deleted!')

    def updateReloadTxtLogStatus(self):
        if self.serv.need_reload_txt_log == True:
            self.need_reload_txt_log = True
            self.logger_err.debug(f'need_reload_txt_log {self.bot_name}')

    def getStratsInfo(self,logger):
        try:
            self.strat = ParseStratFile(self.path)
            self.strat.parceStrats()
            logger.debug(f'Parsed strats {len(self.strat.parsed_strats)}')
            self.strats_is_parsed = True
        except:
            self.strat = None
            self.strats_is_parsed = False

    def checkNeedSaveStratSettings(self):
        self.serv_strat = self.serv.strats[self.market_type_site][self.strat_name]
        self.serv.current_strat = self.serv.strats[self.market_type_site][self.strat_name]
        serv_strat = self.serv_strat
        serv_strat_date = serv_strat['last_edit_date']
        serv_strat_order_size = float(serv_strat['order_size'])
        parsed_strat_active = int(self.parsed_strat['Active']['value']) #-1 - active, 0 - not
        if serv_strat_date == None:
            serv_strat_date = dtm(1970,1,1)
        try:
            parsed_strat_date = dtm.strptime(self.parsed_strat['LastEditDate']['value'], '%Y-%m-%d %H:%M')            
        except:
            parsed_strat_date = dtm(1970,1,2)
        try:
            parsed_strat_order_size = float(self.parsed_strat['OrderSize']['value'])           
        except:
            print('Problem with order size from file')
            parsed_strat_order_size = 10
        if parsed_strat_date > serv_strat_date:
            self.need_save_strat_settings = True
            logger.debug(f'parsed_strat_date > serv_strat_date. {parsed_strat_date} > {serv_strat_date}')
        elif serv_strat_order_size != parsed_strat_order_size and parsed_strat_active != 0:
            logger.debug(f'order size changed. Serv: {serv_strat_order_size}, Bot: {parsed_strat_order_size}, active {parsed_strat_active}')
            self.need_save_strat_settings = True
            self.parsed_strat['LastEditDate']['value'] = dtm.now().strftime('%Y-%m-%d %H:%M')
            logger.debug(f'serv_strat_order_size != parsed_strat_order_size. {serv_strat_order_size}. {parsed_strat_order_size}')
        elif self.serv_strat['need_reload'] == 1:
            self.need_save_strat_settings = True
            self.serv.fixReloadFact()
        else:
            self.need_save_strat_settings = False

    def checkCorrectStratName(self):
        '''Стала появляться проблема с сохранением настроек страт. Разбираюсь в причинах'''
        parsed_strat = self.parsed_strat
        if 'StrategyName' not in parsed_strat:
            self.logger_err.debug('Error with strat name')
            self.logger_err.debug(parsed_strat)
            return False
        if self.strat_name == 'Unknown':
            return False
        if 'Task ' in self.strat_name or len(self.strat_name) > 50:
            self.parsed_strat['StrategyName']['value'] = 'Unknown'
            return False
        return True

    def saveParsedStrats(self,serv):
        self.serv = serv
        #self.logger = logger
        need_reload_strats_from_bd = False
        self.logger.debug(f'start saveParsedStrats')
        for self.parsed_strat in self.strat.parsed_strats:
            self.strat_name = self.parsed_strat['StrategyName']['value']
            if self.strat_name in serv.strats.get(self.market_type_site,{}):
                self.checkNeedSaveStratSettings()
                if self.need_save_strat_settings:
                    if not self.checkCorrectStratName():
                        continue
                    #serv.market_type_site = self.market_type_site
                    serv.createOrUpdateStrat('update',self.parsed_strat,self.market_type)
                    serv.saveStratSettings(self.parsed_strat)
                    #if serv.stop_save_strats == True:
                    #    break
                    logger.debug(f'Save strat {self.strat_name}')
                    need_reload_strats_from_bd = True
            else:
                self.logger.debug(f'Create new strat {self.strat_name}')
                serv.createOrUpdateStrat('create',self.parsed_strat,self.market_type)
                need_reload_strats_from_bd = True
        if need_reload_strats_from_bd:
            self.logger.debug(f'need_reload_strats_from_bd')
            serv.getAllStrategies()
            self.logger.debug(f'strats reloaded')

    def getPathFromBdPath(self,base):
        bd_path = base['file']
        if bd_path.find('.db') > -1:
            txt = bd_path
            poz = max(txt.rfind(r'\\z'[0]),txt.rfind('/'))
            self.path = txt[:poz+1]
        else:
            self.path = bd_path

    def getConfigData(self,param,default=None):
        return self.config.config[self.bot_name].get(param,default)

    def getLastSessionInfo(self):
        self.last_row_num = int(self.getConfigData('last_row_num',0))
        self.log_file_date = self.getConfigData('log_file_date','no_have_date')
        self.last_date = self.config.getDateForNewParse() if self.serv.need_reload_all_data \
            else int(self.getConfigData('last_date',0))

    def delAlexBdData(self):
        if self.need_reload_txt_log == True:
            str_date = str(self.first_log_date)
            self.logger.debug(f'start delAlexBdData from {str_date}')
            self.need_reload_txt_log = False
            local = LocalAlexBase(self.alex_bd_path)
            first_log_date = dtm.strptime(str(self.first_log_date),'%Y-%m-%d %H:%M:%S')-\
                timedelta(seconds=1)
            local.delBotDataInBd(self.bot_name,first_log_date)
            local.closeConnections()

    def readLogFile(self,bot_name):
        self.logger.debug(f'Start parse log file')
        self.getLastSessionInfo()
        print('==2=self.last_row_num',self.last_row_num)
        path = self.path.replace('data','logs')
        if self.need_reload_txt_log:
            self.log_file_date = 'Need reload'
        self.logger.debug(f'work with log file {self.log_file_date}')
        self.p = ParseLogFile(path,self.last_row_num,bot_name,self.log_file_date,self.logger_err)
        if self.p.start_session_time:
            self.start_session_time = self.p.start_session_time.timestamp() - 25*60*60
        else:
            self.start_session_time = None
        self.last_session_time = self.p.last_session_time
        self.overload = self.p.overload
        self.order_task = self.p.order_task
        self.orders_data = self.p.orders_data
        self.bans = self.p.bans
        self.logger.debug(f'Log file is parsed')
        self.first_log_date = self.p.first_log_date

    def fixInConfigLastRowNum(self):
        print('===last_row_num',self.p.last_row_num)
        self.config.changeConfig(bot_name,'last_row_num',self.p.last_row_num)
        if self.p.config_need_update_log_file:
            self.config.changeConfig(bot_name,'log_file_date',self.p.current_date)        
                 
    def readMoonBotBD(self,file_name,read_all=True):
        start = dtm.now()
        bd = LocalBase(file_name,self.config.config['main_data'])
        self.logger.debug(f'moon bd is connected')
        bd_data = []
        poz = 0
        while True:
            self.logger.debug(f'Start read data from moon bd')
            if read_all==False:
                bd_limit = bd.select_limit
                part = self.id_list[poz:poz+bd_limit]
                poz += bd_limit
                bd.readSelectedData(self.moon_col_name,part,logger_err)
            else:
                bd.readData(self.start_session_time,self.logger_err)
                bd_limit = bd.limit
            bd_data.extend(bd.data)
            bd_len = len(bd.data)
            self.logger.debug(f'Readed from mb base {bd_len} rows')
            if bd_len != bd_limit:
                bd.closeConnection()
                self.logger.debug(f'Moon closeConnection')
                break
        duration = int((dtm.now() - start).total_seconds())
        if duration > 7:
            self.logger_err.debug(f'Slow work with moon bd {self.bot_name}. Duration {duration} sec')
        self.moon_bd = bd_data
        bd_len = len(self.moon_bd)
        self.logger.debug(f'Total readed from mb base {bd_len} rows')
        self.moon_bd_column_names = bd.columns

    def getSellReason(self,txt):
        reasons = {'StopLoss':'SLoss','Sell Price':'SellPr','LIQUIDATION':'Liquid',
            'TrailingStop':'Trail','Auto Price':'PrDown','Global PanicSell':'Panic',
            'JoinedSell':'Jsell','Sell by FilterCheck':'FiltCheck','Auto Sell Replacing':'PrDown',
            'Sell Level':'SellLvl','BV/SV Stop':'BVSV',
            }
        for reason,k in reasons.items():
            if reason in txt:
                return k
        return 'Other'

    def addDataFromMoonToNulledClose(self):
        len_db = len(self.moon_bd)
        self.logger.debug(f'readed from moon bd: {len_db}')
        if len_db == 0:
            return
        moon_task_ids = {}
        moon_ex_orders = {}
        for row in self.moon_bd:
            row = dict(zip(self.moon_bd_column_names,row))
            try:
                moon_ex_orders[row['ex_order_id']] = row
                moon_task_ids[int(row['task_id'])] = row
            except:
                pass
        for index,order in self.orders_data.iterrows():
            order_task = int(order.get('bot_order_id',-555))
            ex_order_id = order.get('ex_order_id',-555)
            try:
                if order_task in moon_task_ids:
                    row = moon_task_ids[order_task]
                elif moon_ex_orders.get(ex_order_id,'emu') != 'emu':
                    row = moon_ex_orders[ex_order_id]
                else:
                    continue
                if order['sell_price'] is None:
                    moonbot_buy_date = dtm.fromtimestamp(row['buy_date'])
                    buy_date = order['buy_date']
                    if buy_date - timedelta(hours=24*4) < moonbot_buy_date and \
                        buy_date + timedelta(hours=24*4) > moonbot_buy_date:
                        delta = order['buy_price'] / row['sell_price']
                        if delta > 1.3 or delta < 0.7:
                            print('big difference byu sell price',order['buy_price'],row['sell_price'])
                            self.orders_data.loc[index,'sell_price'] = order['buy_price'] * 0.95
                            self.orders_data.loc[index,'comment'] = 'recalc buy -5%'
                        else:                        
                            self.orders_data.loc[index,'sell_price'] = row['sell_price']
                            self.orders_data.loc[index,'comment'] = 'recalc Got moon sell data'
                        self.orders_data.loc[index,'close_date'] = dtm.fromtimestamp(row['close_date'])
                        try:
                            if order['orders_in_net'] is None:
                                self.orders_data.loc[index,'orders_in_net'] = 1
                            profit_percent = round((row['sell_price'] / order['buy_price']-1) * 100,2) - 0.1
                            self.orders_data.loc[index,'profit_percent'] = profit_percent
                            self.orders_data.loc[index,'profit'] = \
                                round(order['order_size'] * profit_percent /100,2)
                        except:
                            pass
            except:
                pass
            
    def addDataFromMoonToLogOrders(self):
        len_db = len(self.moon_bd)
        self.logger.debug(f'readed from moon bd: {len_db}')
        if len_db == 0:
            return
        moon_task_ids = {}
        moon_ex_orders = {}
        for row in self.moon_bd:
            row = dict(zip(self.moon_bd_column_names,row))
            try:
                moon_ex_orders[row['ex_order_id']] = row
                moon_task_ids[int(row['task_id'])] = row
            except:
                pass
        for index,order in enumerate(self.orders_data):
            try:
                order_task = int(order.get('bot_order_id',-555))
            except:
                order_task = -555
            try:
                ex_order_id = order.get('ex_order_id',-555)
            except:
                ex_order_id = -555
            try:
                if order_task in moon_task_ids:
                    row = moon_task_ids[order_task]
                    #self.logger.debug(f'Order task {order_task} founded')
                elif moon_ex_orders.get(ex_order_id,'emu') != 'emu':
                    row = moon_ex_orders[ex_order_id]
                    #self.logger.debug(f'ex_order_id {ex_order_id} founded')
                else:
                    #self.logger_err.debug\
                    #    (f'Cant find in moon bd order_task {order_task} ex_order_id {ex_order_id}')
                    continue
                self.orders_data[index]['sell_reason'] = self.getSellReason(row['SellReason'])
                self.orders_data[index]['moonbot_buy_date'] = dtm.fromtimestamp(row['buy_date'])
                self.orders_data[index]['is_short'] = row['is_short']
                self.orders_data[index]['base_coin'] = row['base_coin']
                self.orders_data[index]['moonbot_buy_price'] = row['buy_price']
                if order['sell_price'] is None:
                    moonbot_buy_date = dtm.fromtimestamp(row['buy_date'])
                    buy_date = self.orders_data[index]['buy_date']
                    if buy_date - timedelta(hours=24*4) < moonbot_buy_date and \
                        buy_date + timedelta(hours=24*4) > moonbot_buy_date:
                        self.orders_data[index]['sell_price'] = row['sell_price']
                        self.orders_data[index]['close_date'] = dtm.fromtimestamp(row['close_date'])
                        self.orders_data[index]['comment'] = '2-Got moon sell data'
                        #if self.orders_data['orders_in_net']
            except:
                pass
                #self.logger.debug\
                #    (f'order_task {order_task} ex_order_id {ex_order_id}')
                #print(moon_ex_orders)
                #print(moon_task_ids)

    def addStratIdToOrderTask(self,serv):
        for strat in self.order_task:
            try:
                strat_id = serv.strats[self.market_type_site][strat]['id']            
                self.order_task[strat]['strategy_id'] = strat_id
            except:
                self.logger.debug(f'Problem with strat in writeOverloadAndOrderTask. name is {strat}')

    def writeOverloadAndOrderTask(self):
        over_cnt = len(self.overload)
        self.logger.debug(f'Save owerloads {over_cnt}')
        self.serv.saveOverloadLog(self.overload)
        tasks = len(self.order_task)
        self.logger.debug(f'Save order tasks {tasks}')
        self.addStratIdToOrderTask(serv)
        try:
            self.serv.saveOrderTask(self.order_task)
        except:
            self.logger_err.debug('Problem with self.order_task')
            self.logger_err.debug(self.order_task)
        try:
            if sum(self.bans.values()) > 0:
                self.serv.saveBans(self.bans)
                self.logger_err.debug('!!!Banned!')
                self.logger_err.debug(self.bans)
                print('!!!Bans',self.bans)
        except:
            self.logger_err.debug('Problem with self.bans')
            self.logger_err.debug(self.bans)

    def usedTimeToRow(self,used_time):
        self.used = ''
        for k,v in used_time.items():
            v = int(v)
            self.used = f'{k}={v},{self.used}'
        
    def saveOrdersToLocalAlexBase(self):
        save = saveOrdersToLocalAlexBase(self.alex_bd_path)
        self.logger.debug(f'Prepare tasks to Alex local bd')
        #save.writeData(self.orders_data)
        save.prepareData(self.orders_data)
        #self.logger.debug(f'writeNewRowsWithJoin to Alex local bd')
        #save.writeNewRowsWithJoin()
        #rows_cnt = len(save.rows_with_join_to_create)
        #self.logger.debug(f'Total rows to create {rows_cnt}')
        self.logger.debug(f'writeNewRowsWithoutJoin to Alex local bd')
        save.writeNewRowsWithoutJoin(self.logger,self.logger_err)
        rows_cnt = len(save.rows_to_create)
        self.logger.debug(f'Total rows to create {rows_cnt}')
        self.logger.debug(f'updateRows to Alex local bd')
        start = dtm.now()
        save.updateRows(self.logger)
        rows_cnt = len(save.rows_to_update)
        self.logger.debug(f'Total rows to update {rows_cnt}')
        duration = int((dtm.now() - start).total_seconds())
        if duration > 7:
            self.logger_err.debug(f'Alex bd. Slow work. {self.bot_name}. Duration {duration} sec. \
                                  Total rows to update {rows_cnt}')
            self.usedTimeToRow(save.used_time)
            self.logger_err.debug(f'Timings is: {self.used}')
            self.used
        orders_data = len(self.orders_data)
        self.logger.debug(f'Total sended tasks to local bd {orders_data}')
    
    def getLastDate(self):
        if len(self.orders_data) != 0:
            row = dict(zip(self.column_names,self.orders_data[-1]))
            self.last_date = int(dtm.strptime(row['close_date'],'%Y-%m-%d %H:%M:%S').timestamp())
            last_date = dtm.strptime(row['close_date'],'%Y-%m-%d %H:%M:%S')
            self.logger.debug(f'change last date: {last_date}')
            for row in self.orders_data: #Выдача некорректно сортируется при запросе к SQLlite 
                row = dict(zip(self.column_names,row))
                self.last_bd_date_dtm = dtm.strptime(row['close_date'],'%Y-%m-%d %H:%M:%S')
                row_date = int(self.last_bd_date_dtm.timestamp())
                if row_date > self.last_date:
                    self.logger.debug(f'ERROR with last date! New last date: {self.last_bd_date_dtm}')
                    self.last_date = row_date

    def getOrdersFromAlexBase(self):
        self.l = LocalAlexBase(self.alex_bd_path)
        last_date = dtm.fromtimestamp(self.last_date)
        values = {'bot_name':self.bot_name,'date':last_date}
        self.logger.debug(f'start getOrdersFromAlexBase')
        self.l.getAllOrders(values)
        self.orders_data = self.l.data_from_bd
        len_df = len(self.orders_data)
        self.logger.debug(f'getOrdersFromAlexBase: {len_df} rows, from date: {last_date}')
        self.column_names = self.l.column_names
        self.l.closeConnections()
        self.getLastDate()

    def deleteBotsUnusedRows(self):
        local = LocalAlexBase(self.alex_bd_path)
        date = dtm.strptime(str(self.last_session_time),'%Y-%m-%d %H:%M:%S')- \
            timedelta(hours=12)
        local.deleteBotsUnusedRows(self.bot_name,date)
        str_date = str(self.last_session_time)
        self.logger.debug(f'deleteBotsUnusedRows from {str_date}')
        local.closeConnections()
#%%
fileConfig('logging_config.ini')
logger = logging.getLogger()
logger_err = logging.getLogger('errlog')
#%%
config = MyConfig()
if config.config['main_data']['stop'] == '1':
    logger.debug('Is stopped in settings')
    sys.exit()
#%%

i = 0
#while i < 4:
randomSleep(config.config['main_data']['start_sleep_max_sec'])
logger.debug('Try connect to server')
i += 1

#%%
try:        
    serv = Server(server_data,config,logger,logger_err)
    if not serv.connection:
        logger.debug('is not serv.connection')
        logProblem(logger_err)
        closeProgramm(config,logger_err)
    #else:
    #    break
except:
    logger.debug('except connection')
    logProblem(logger_err)
    closeProgramm(config,logger_err)

#if not serv.connection:
#    closeProgramm(config,logger_err)
#%%

#%%
logger.debug(f'Total bots {len(config.bases.items())}')
try:
    config.changeConfig('main_data','stop',1)
    for bot_name,base in config.bases.items():
        try:
            logger.debug(f'Bot: {bot_name}')

            serv.checkBot(bot_name)
            work = Worker(bot_name,config,serv,logger,logger_err)
            if serv.need_reload_all_data == True:
                work.reloadAllData()
            if serv.need_reload_txt_log == True:
                work.updateReloadTxtLogStatus()
            work.getPathFromBdPath(base)
            work.getStratsInfo(logger)

            work.market_type = base['market_type']
            work.site = config.config['main_data']['site']
            work.market_type_site = (base['market_type'],config.config['main_data']['site'])
            work.saveParsedStrats(serv)
            while True:
                work.readLogFile(bot_name)
                if len(work.p.rows) == 0 and not work.serv.need_reload_all_data:
                    logger.debug('No have data in log')
                    break
                work.delAlexBdData()
                work.writeOverloadAndOrderTask()
                if len(work.orders_data) > 0:
                    logger.debug('readMoonBotBD')
                    work.readMoonBotBD(base['file'])
                    logger.debug('addDataFromMoonToLogOrders')
                    work.addDataFromMoonToLogOrders()
                logger.debug('saveTasksToLocalAlexBase')
                work.saveOrdersToLocalAlexBase()
                work.fixInConfigLastRowNum()
                work.getOrdersFromAlexBase()
                sleep = int(config.config['main_data']['send_data_sleep_sec'])
                serv.saveData(work.orders_data,work.column_names,base['market_type'],logger,sleep)
                work.config.changeConfig(bot_name,'last_date',work.last_date)
                if hasattr(work,'last_bd_date_dtm'):
                    work.config.changeConfig(bot_name,'last_bd_date_dtm',work.last_bd_date_dtm)
                #work.config = MyConfig()
                if not work.p.big_data_size:
                    break
                else:
                    work.deleteBotsUnusedRows()
            serv.saveBotData()
        except:
            logger.debug('Done with problem1')
            logProblem(logger_err,bot_name)
        logger.debug(f'===Done - normal {bot_name}')
    local = LocalAlexBase(work.alex_bd_path)
    local.deleteUnusedRows()
    local.closeConnections()
    work.config.changeConfig('main_data','stop',0)
    serv.cur.close()
    serv.con.close()
    logger.debug(f'===Done - normal')
        
except:
    try:
        logger.debug('Done with problem2')
        logProblem(logger_err,bot_name)
        closeServerConnection(logger_err,serv)
        closeProgramm(config,logger_err)
    except:
        pass
logger.debug('Done!!!')
#%%
"""
#%%
work.strat = ParseStratFile(work.path)
work.strat.parceStrats()
#%%
config.config['002_0']['last_row_num']
#%%
config.changeConfig('002_0','last_row_num',133)
# %%
"""
# %%
