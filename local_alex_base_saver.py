#%%
#%load_ext autoreload
#%autoreload 2
import sys
from glob import glob
from logging import lastResort
import os
import os.path
from datetime import datetime as dtm
from datetime import timedelta
import time
import json
import pandas as pd
#import numpy as np
from local_alex_base_connector import LocalAlexBase

# %%
def normalizeDFList(lst):
    '''Преобразовать список из панды для джанго'''
    lst=list(lst)
    lst.sort()
    return list(map(int,lst))


class WorkWithDF():
    def formatDfFromAlexDb(self,df):
        for col in ['date','buy_date']:
            df[col] = pd.to_datetime(df[col], format='%Y-%m-%d %H:%M:%S',errors='ignore')
        for col in ['join_num','main_join_num']:
            df[col] = df[col].astype(float, errors='ignore')
        df['changed'] = 0
        df['is_sale'] = 0
        return df

    def compareTotalBuySizeWithSaleSize(self):
        if 'close_date' in self.joined_df.columns:
            query = (self.joined_df['close_date'].isnull())#при перезагрузки ДФ
        else:
            query = (~self.joined_df['buy_date'].isnull())#при стандартном парсинге логов
        self.buy_df = self.joined_df[query].sort_values(['date','quantity'])
        if len(self.buy_df[~self.buy_df['quantity'].isnull()]) == 0:
            return False #нужно взять другой таймфрейм
        buy_sum = self.buy_df['quantity'].sum()
        if buy_sum < self.sale['quantity'] * .95:
            return False #нужно взять другой таймфрейм
        return True

    def createBuyOrdersList(self):
        self.buy_orders_list = []
        i = 0
        for _,row in self.buy_df.iterrows():
            buy_order = {}
            for val in ['buy_date','buy_price','order_size']:
                buy_order[f'{val}_{i}_'] = str(row[val])
            buy_order[f'create_date_{i}_'] = str(row['date'])
            #buy_order['num'] = i
            self.buy_orders_list.append(buy_order)
            i += 1

    def changeDtmFormat(self,val):
        try:
            return dtm.strptime(str(val),'%Y-%m-%d %H:%M:%S')
        except:
            return val
        
    def generateSalesData(self):
        sale_data = {}
        sale_data['order_size'] = self.buy_df['order_size'].min()
        sale_data['buy_task_create_date'] = self.changeDtmFormat(self.buy_df['date'].min())
        sale_data['buy_date'] = self.changeDtmFormat(self.buy_df['buy_date'].min())
        sale_data['orders_list'] = json.dumps(self.buy_orders_list)
        sale_data['orders_in_net'] = len(self.buy_df)
        #цена покупки первого ордера
        sale_data['buy_price'] = self.buy_df.iloc[0]['buy_price']
        self.sale_data = sale_data
        #self.correctOrderSize()
                             
class saveOrdersToLocalAlexBase(WorkWithDF):
    def __init__(self,alex_bd_path):
        self.alex_bd_path = alex_bd_path        
        self.l = LocalAlexBase(self.alex_bd_path)
        self.limit = 1000

    def getMainJoinNum(self):
        '''
        если нет записей с таким же join_num, добавим его как основной
        '''
        if len(self.l.data_from_bd) == 0:
            main_join_num = None
        else:
            main_join_num = self.l.data_from_bd[0][1]
        self.o['main_join_num'] = main_join_num

    def sellCheckTaskInBD(self):
        self.row = dict(zip(self.l.column_names, self.l.data_from_bd[0]))
        self.sell_row_id = self.row['id']
        keys = ['close_date','sell_price','quantity','total_spent','profit','profit_percent']
        if self.row['buy_date'] is not None:#для ордеров без сеток
            self.o['main_join_num'] = None
            self.o['join_num'] = None
            self.row['join_num'] = None
            self.o['buy_date'] = \
                dtm.strptime(self.row['buy_date'],'%Y-%m-%d %H:%M:%S')
            self.o['orders_in_net'] = 1            
            keys.extend(['main_join_num','join_num','buy_date','orders_in_net'])
        self.keys = keys

    def sellAddMoonBotKeys(self):
        keys = ['sell_reason','moonbot_buy_date','is_short','base_coin','moonbot_buy_price','emulator']
        for key in keys:
            if key in self.o:
                self.keys.append(key)

    def generateFullJoinedNums(self):
        jn = set()
        for row in self.l.data_from_bd:
            row = dict(zip(self.l.column_names, row))
            jn.add(str(row['join_num']))
            jn.add(str(row['main_join_num']))
        if None in jn:
            jn.remove(None)
        jn = list(jn)
        jn_str = '","'.join(jn)
        self.row['joined_nums'] =  f'"{jn_str}"'

    def correctOrderSize(self):
        #для случаев, когда сетка не закрылась, а размер ордера увеличился в боте
        orders = []
        for row in self.l.data_from_bd:
            row = dict(zip(self.l.column_names, row))
            orders.append(float(row['order_size']))
        max_o = max(orders)
        if max_o/self.order_size > 30:
            self.order_size = (max_o + self.order_size) / 2

    def _del__createIdListForUnionToNetAndBuyData(self):
        self.buy_id_list = []
        self.buy_orders_list = []
        i = 0
        for row in self.l.data_from_bd:
            row_dict = dict(zip(self.l.column_names, row))
            buy_id = row_dict['id']
            self.buy_id_list.append(buy_id)
            buy_order = {}
            for val in ['buy_date','buy_price','order_size']:
                buy_order[f'{val}_{i}_'] = row_dict[val]
            buy_order[f'create_date_{i}_'] = row_dict['date']
            #buy_order['num'] = i
            self.buy_orders_list.append(buy_order)
            i += 1
        self.buy_id_list.append(self.sell_row_id)

    def writeNetUnionKey(self):
        if self.need_update_net_union_key:
            self.l.addNetUnionKey(self.buy_id_list,self.sell_row_id)

    def sellCountProfitPercent(self):
        try:
            if 'order_size' not in self.o:
                self.o['order_size'] = self.o['total_spent']
            self.o['profit_percent'] = round(float(self.o['profit']) / float(self.o['order_size']) * 100,2)
        except:
            print(self.o['profit'],self.o['order_size'],type(self.o['profit']),type(self.o['order_size']))
            self.o['profit_percent'] = 0

    def getMainJoinNumFromTasks(self):
        for row in self.l.data_from_bd:
            row = dict(zip(self.l.column_names, row))
            if row['main_join_num'] is not None:
                self.main_join_num = row['main_join_num']
                return

    def updateJoinNumInTasks(self):
        for row in self.l.data_from_bd:
            row = dict(zip(self.l.column_names, row))
            row['date'] = dtm.strptime(row['date'],'%Y-%m-%d %H:%M:%S')
            if row['join_num'] is None:
                keys = ['join_num','main_join_num']
                row['join_num'] = self.o['join_num']
                row['main_join_num'] = self.main_join_num
                self.l.updateRow(row,keys)
            elif row['main_join_num'] is not None:
                keys = ['join_num']
                row['join_num'] = self.o['join_num']
                self.l.updateRow(row,keys)

    def workWIthJoinTasks(self):
        self.main_join_num = None
        self.l.readJoinNum(self.o)
        self.main_join_num = self.o['join_num']
        self.getMainJoinNumFromTasks()
        self.updateJoinNumInTasks()

    def updateJoinNumInCurrentTask(self):
        keys = ['join_num','main_join_num']
        self.o['main_join_num'] = self.main_join_num
        self.l.updateRow(self.o,keys)

    def splitOrders(self,orders_data):
        self.rows_to_create = []
        self.rows_to_update = []
        for row in orders_data:
            if 'status' not in row:
                continue
            row['local_row_create_date'] = self.now
            if row['status'] == 'start' and 'join_num' not in row:
                self.rows_to_create.append(row)
            else:
                self.rows_to_update.append(row)
                
    def prepareData(self,orders_data):
        #orders_data = orders_data
        self.now = dtm.utcnow()
        self.splitOrders(orders_data)

    def _del_writeNewRowsWithJoin(self):
        for self.o in self.rows_with_join_to_create:
            #if self.o['status'] == 'start':
                #self.o['local_row_create_date'] = self.now
                #if 'join_num' in self.o:#TODO del later
            self.l.getMainJoinNum(self.o)
            self.getMainJoinNum()
            #print(self.l.data_from_bd)
            try:
                self.l.createRow(self.o)
            except:
                time.sleep(2)
                self.l.createRow(self.o)

    def writeNewRowsWithoutJoin(self,logger,logger_err):
        sleep,poz,saved = 0.5,0,0
        while poz < len(self.rows_to_create):
            part = self.rows_to_create[poz:poz+self.limit]
            saved += len(part)
            self.l.createManyRows(part,logger_err)
            logger.debug(f'Sended to bd: {len(part)}')
            poz += self.limit
            time.sleep(sleep)

    def addTime(self,timer_type):
        if timer_type not in self.used_time:
            self.used_time[timer_type] = 0
        self.used_time[timer_type] = self.used_time[timer_type] + (time.perf_counter() - self.start)

    def updateRows(self,logger):
        #for self.o in orders_data:
        self.used_time = {}
        for self.o in self.rows_to_update:
            self.start = time.perf_counter()
            self.l.find_orders_interval_hours = 24
            if 'status' not in self.o:
                self.addTime('not in o')
                continue
            if self.o['status'] == 'start':
                #self.o['local_row_create_date'] = self.now
                if 'join_num' not in self.o:
                    continue
                self.l.getMainJoinNum(self.o)
                self.getMainJoinNum()
                try:
                    self.l.createRow(self.o)
                except:
                    time.sleep(2)
                    self.l.createRow(self.o)
                self.addTime(self.o['status'])
            elif self.o['status'] == 'buy':
                keys = ['buy_date','buy_price','quantity','order_size','is_short']
                self.l.updateRow(self.o,keys)
                self.addTime(self.o['status'])
            elif self.o['status'] == 'c_id':
                self.keys = ['ex_order_id']
                if self.o['ex_order_id'] == 'emu':
                    self.keys.append('emulator')
                    self.o['emulator'] = 1
                self.sellAddMoonBotKeys()
                self.l.updateRow(self.o,self.keys)
                self.addTime(self.o['status'])
            elif self.o['status'] == 'sell':
                for find_interval in [24,48,72,96]:
                    self.l.find_orders_interval_hours = find_interval
                    self.l.readTask(self.o)
                    if self.l.data_from_bd == []:
                        continue
                    self.sellCheckTaskInBD()
                    self.sellAddDataToSellInfo(logger)
                    self.sellAddMoonBotKeys()
                    self.sellCountProfitPercent()
                    self.keys = list(set(self.keys))
                    self.l.updateRow(self.o,self.keys)
                    if self.l.cur.rowcount == 0:
                        continue
                    self.writeNetUnionKey()
                    self.addTime(self.o['status'])
                self.l.find_orders_interval_hours = 24
            elif self.o['status'] == 'join_tasks':
                self.workWIthJoinTasks()
                self.updateJoinNumInCurrentTask()
                self.addTime(self.o['status'])
        #self.start = time.perf_counter()
        #self.l.deleteUnusedRows()
        #self.addTime('deleteUnusedRows')
        self.l.closeConnections()

    def generateDfsForSale(self):
        df = pd.DataFrame(self.l.data_from_bd,columns = self.l.column_names)
        self.joined_df = self.formatDfFromAlexDb(df)
        sale_id = int(self.o['bot_order_id'])
        self.sale = self.joined_df[self.joined_df['bot_order_id']==sale_id].iloc[0]
        
    def sellAddDataToSellInfo(self,logger):
        self.sale_keys = ['buy_date','buy_task_create_date','orders_in_net','order_size','buy_price','orders_list']
        join_num = self.row['join_num']
        main_join_num = self.row['main_join_num']
        self.need_update_net_union_key = False
        if join_num is not None and main_join_num is not None:
            #if join_num != main_join_num:
            self.row['date'] = dtm.strptime(self.row['date'],'%Y-%m-%d %H:%M:%S')
            self.row['joined_nums'] = f'"{join_num}","{main_join_num}"'
            for time_line in [10,30,120]:
                #первый раз из массива строк выберем join_num. Лень переписывать
                self.l.getJoinedOrders(self.row,time_line) 
                self.generateFullJoinedNums()
                self.l.getJoinedOrders(self.row,time_line)
                #второй раз уже работаем с пандой
                self.generateDfsForSale()
                if not self.compareTotalBuySizeWithSaleSize():
                    continue #нужно взять другой таймфрейм
                if len(self.joined_df) > 0:
                    self.need_update_net_union_key = True
                    self.createBuyOrdersList()
                    self.buy_id_list = normalizeDFList(self.joined_df['id'].unique())
                    self.generateSalesData()
                    for key in self.sale_keys:
                        self.o[key] = self.sale_data[key]
                    self.keys.extend(self.sale_keys)
                return

class UpdateSaleData(WorkWithDF):
    #TODO если есть строки с заполненными buy_date close_date и join = None - поставить им 1 ордер в сетке
    def __init__(self,bot_name,alex_bd_path):
        self.alex_bd_path = alex_bd_path
        self.bot_name = bot_name
        self.l = LocalAlexBase(self.alex_bd_path)
        self.l.readAllTasksForUpdateBug(bot_name)
        df = pd.DataFrame(self.l.data_from_bd,columns = self.l.column_names)
        self.alex_df = self.formatDfFromAlexDb(df)
        self.sale_keys = ['buy_date','buy_task_create_date','orders_in_net','order_size','buy_price','orders_list']
    
    def getFirstDate(self):
        self.l.selectFirstRow()
        self.first_date = self.changeDtmFormat(self.l.data_from_bd[0])
    
    def createSaleDf(self,time_line):
        """
        Создадим ДФ для конкретной продажи
        """
        df = self.alex_df
        s = self.sale
        s_date = dtm.strptime(str(s['date']),'%Y-%m-%d %H:%M:%S')
        query = (df['coin']==s['coin']) & (df['bot_name']==s['bot_name']) &\
            (df['strat_name']==s['strat_name']) &\
            (df['date'] > s_date-timedelta(minutes=time_line)) &\
            (df['date'] < s_date+timedelta(minutes=time_line))
        #делаю +timedelta(minutes=time_line)), т.к. у sale ранее date заменялась на дату первого ордера
        self.sale_df = df[query]
        
    def findJoinedNumRows(self):
        s = self.sale_df
        joins = set()
        for col in ['join_num','main_join_num']: #добавим join из селла
            if self.sale[col] > 0:
                joins = joins.union([self.sale[col]])
        #выполним дважды прогон. Сначала загрузим main_join_num, потом его учетом еще поищем join
        for _ in [1,2]:            
            rows_with_joins = s[s['join_num'].isin(joins) | s['main_join_num'].isin(joins)]
            for col in ['join_num','main_join_num']:
                joins = joins.union(list(rows_with_joins[col].dropna().unique()))
            if None in joins:
                joins.remove(None)
        self.joined_nums = joins

    def createDfWithJoinedNums(self):
        s = self.sale_df
        query = s['join_num'].isin(self.joined_nums) | s['main_join_num'].isin(self.joined_nums)
        self.joined_df = s[query]
        
    def updateDataInJoinedRows(self):
        df = self.alex_df
        if not self.compareTotalBuySizeWithSaleSize():
            return False #нужно взять другой таймфрейм
        if abs(self.buy_df['quantity'].sum()/self.sale['quantity'])-1 > 0.05:
            print(self.joined_df[['id','date','buy_date','coin','strat_name','quantity','join_num','main_join_num']])
            print('Orders quntity != sale q')
            return True #ничего не добавляем тогда
        self.createBuyOrdersList()
        joined_rows_id = self.joined_df['id'].unique()
        #проставим айди селла всем
        df.loc[df['id'].isin(joined_rows_id),['sell_row_id','changed']] = [self.sale['id'],1]
        self.generateSalesData()
        for key in self.sale_keys:
            df.loc[self.sale_index,key] = self.sale_data[key]
        df.loc[self.sale_index,'is_sale'] = 1
        return True

    def matchOrdersWithQuantity(self):
        '''
        когда не удается сджойнить по ключу, начинаем перебирать по количеству
        '''
        sdf = self.sale_df
        sale_id = self.sale['id']
        query = (sdf['close_date'].isnull()) & (sdf['date']<=self.sale['date']) & \
            ((sdf['sell_row_id']==-1) | (sdf['sell_row_id']==sale_id))
        df = self.sale_df[query].sort_values(['date','quantity'])
        q_total = 0
        self.joined_rows_ids = [sale_id]
        for _,row in df.iterrows():
            q_total += float(row['quantity'])
            self.joined_rows_ids.append(row['id'])
            if q_total == float(self.sale['quantity']):
                return True
        return False

    def createCalcedJoinedDf(self):
        query = (self.sale_df['id'].isin(self.joined_rows_ids))
        self.joined_df = self.sale_df[query]

    def addDataInDf(self): 
        df = self.alex_df
        for self.sale_index,self.sale in df[df['close_date'].notnull()].iterrows():
            for time_line in [10,30,120]:
                self.createSaleDf(time_line)
                self.findJoinedNumRows()
                self.createDfWithJoinedNums()
                if self.updateDataInJoinedRows():
                    break
                if self.matchOrdersWithQuantity():
                    self.createCalcedJoinedDf()
                    if self.updateDataInJoinedRows():
                        break

    def readCloseDateNulledDF(self):
        date = dtm.now() - timedelta(hours=4*24)
        values = {'bot_name':self.bot_name,'date':date}
        self.l.getCloseNulledOrders(values)
        df = pd.DataFrame(self.l.data_from_bd,columns = self.l.column_names)
        self.null_close_df = self.formatDfFromAlexDb(df)
        self.close_nulled_keys = ['sell_price','close_date','comment','orders_in_net',\
            'profit_percent','profit']
        
    def createNullClosedLists(self):        
        self.readCloseDateNulledDF()
        self.null_close_date_id = {}
        df = self.null_close_df
        #bot_order_id - это task id в мунботе
        query = (df['bot_order_id'].notnull())&(df['bot_order_id']!=-555)
        self.null_close_date_id['task_id'] = normalizeDFList(df[query]['bot_order_id'].unique())
        query = (df['ex_order_id'].notnull())&(df['ex_order_id']!='emu')&(~df['bot_order_id'].notnull())
        self.null_close_date_id['ex_order_id'] = list(df[query]['ex_order_id'].unique())

    def updateDataInBD(self):
        self.sale_keys.append('sell_row_id')
        df = self.alex_df[self.alex_df['changed']==1]        
        for _,row in df[df['is_sale']!=1].iterrows():
            self.l.updateBuyRow(row['id'],row['sell_row_id'])
        for _,row in df[df['is_sale']==1].iterrows():
            for col in ['date','buy_date','buy_task_create_date']:
                row[col] = self.changeDtmFormat(row[col])
            self.l.updateRow(row,self.sale_keys)
        df = self.null_close_df[self.null_close_df['close_date'].notnull()]
        self.l.find_orders_interval_hours = 24*4
        for _,row in df.iterrows():
            for col in ['date','buy_date','buy_task_create_date','close_date']:
                row[col] = self.changeDtmFormat(row[col])
            self.l.updateRow(row,self.close_nulled_keys)
            #print(f'updated {self.l.cur.rowcount}')
        self.l.find_orders_interval_hours = 24
#%%
"""
u = UpdateSaleData('002_0')

u.createNullClosedLists()
u.null_close_date_id

#%%
u.updateDataInBD()
#%%
import pathlib
#from log_writer import ParseLogFile
path = str(pathlib.Path(__file__).parent.resolve())
fn = 'LOG_2022-05-01.log'
#sself = ParseLogFile(path,0,'bot_n',fn,print)

import logging
from logging.config import fileConfig
fileConfig('logging_config.ini')
logger = logging.getLogger()
logger_err = logging.getLogger('errlog')

#%%
save = saveOrdersToLocalAlexBase()
#save.l.selectFirstRow()
#save.l.data_from_bd[0]

save.l.deleteAllRows()

save.prepareData(sself.orders_data)
save.writeNewRowsWithoutJoin(logger,logger_err)
#%%
save.updateRows(logger)
#%%
save.rows_to_create
#%%

#%%

#%%
u.addDataInDf()
u.updateDataInBD()
u.l.closeConnections()
print('Done!')

#%%
"""
#%%
#%%