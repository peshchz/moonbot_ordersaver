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
from local_alex_base_connector import LocalAlexBase
import pathlib
import pandas as pd
#import traceback

class ParseDefs():
    def getStratNameFromRow(self,row):
        row = row.replace('<Short>','')
        if 'strategy <' in row:
            start = 'strategy <'
            name = row[row.find(start)+len(start):row.find('>')]
        elif 'using strategy ' in row:
            start = 'using strategy '
            stop = '  ( '
            name = row[row.find(start)+len(start):row.find(stop)]
        else:
            name = 'Unknown'
        if 'Task ' in name or len(name) > 50:
            name = 'Unknown'
        if len(name) == 0:
            name = 'Unknown'
        if name == 'Unknown':
            try:
                self.logger_err.debug('Problem with strat name')
                self.logger_err.debug(row)
            except:
                pass
        return name
        
class ParseLogFile(ParseDefs):
    def __init__(self,path,config_last_row_num,bot_name,config_last_log_file,logger_err):
        self.bot_name = bot_name
        self.path = path
        self.config_last_row_num = int(config_last_row_num)
        print('===self.config_last_row_num',self.config_last_row_num)
        self.config_last_log_file = config_last_log_file
        self.logger_err = logger_err
        self.max_data_size = 900000
        self.generateDictsAndLists()
        self.getFilesFromPath()
        its_second_file = False
        for _,self.file_data in self.files_df.iterrows():#в 00.00+-5 мин запись ведется в 2 лога
            if its_second_file:
                print('its_second_file')
                self.log_file_is_changed = True
                self.config_need_update_log_file = True
                self.config_last_row_num = 0 #для второго лога
            self.getDates()
            self.readFile()
            self.file_started = True
            self.getActualRows()
            if self.big_data_size:
                return
            its_second_file = True

    def generateDictsAndLists(self):
        self.overload = []
        self.log = []
        self.last_10s_vals = {}
        self.order_task = {}
        self.orders_data = []
        self.start_session_time = None
        self.last_session_time = None
        self.bans = dict(
            moon = 0,
            binance = 0,
        )
        self.big_data_size = False
        self.log_file_is_changed = False #для изменения даты в первых строках с часом 23
        self.config_need_update_log_file = False #для обновления лога

    def getDates(self):
        #лог начинается с 23:59. Для этих записей надо брать предыдущую дату
        self.current_date = str(self.file_data['data_date'])[:10]
        self.previous_date = str(self.file_data['previous_date'])[:10]
        
    def getFilesFromPath(self):
        '''получим список свежих файлов с каталога'''
        file_mask = '{}**/LOG*.log'.format(self.path)
        all_files = glob(file_mask, recursive=True)
        files_list = []
        for f in all_files:
            file_change_date = dtm.fromtimestamp(os.path.getmtime(f))
            name_date = f[f.find('LOG_')+4:f.find('.log')]
            try:
                data_date = dtm.strptime(f'{name_date}','%Y-%m-%d')
            except:
                continue
            if self.config_last_log_file in f:
                current_file = 1  if self.config_last_log_file != 'Need reload' else 0
                self.current_data_date = data_date
            else:
                current_file = 0
            files_list.append({'file_name':f,'file_change_date':file_change_date,
                               'data_date':data_date,
                               'current_file':current_file,
                               })
        df = pd.DataFrame.from_records(files_list).sort_values(['data_date'])
        df['previous_date'] = df['data_date'] - timedelta(hours=24)
        if len(df[df['current_file']==1]) == 0:
            #если файл не задан, выберем последний
            #так же можно детектить его отсутствие по self.config_last_log_file == 'no_have_date'
            if self.config_last_log_file == 'Need reload':
                file_n = 0 if len(df)<30 else len(df)-29
            else:
                file_n = len(df)-1
            df.loc[file_n,'current_file'] = 1
            self.current_data_date = df.loc[df['current_file']==1].\
                iloc[0]['data_date']
            self.log_file_is_changed = True
            self.config_need_update_log_file = True
            self.config_last_row_num = 0
        self.files_df = df[df['data_date']>=self.current_data_date]
        self.first_log_date = df['data_date'].min()


    def readFile(self):
        def tryRead(f):
            try:
                return f.readline()
            except:
                return []
        r_num = 0
        self.rows = []
        print('read',self.file_data['file_name'])
        with open(self.file_data['file_name'], encoding='utf8') as f:
            line = tryRead(f)
            while line:
                r_num += 1
                if r_num > self.config_last_row_num:
                    self.rows.append(line)
                line = tryRead(f)
                if len(self.rows) +len(self.orders_data) > self.max_data_size:
                    #TODO сделать чтение файла частями
                    self.big_data_size = True
                    break
        if len(self.rows) > 0:
            if self.rows[-1] == '\n':
                self.rows = self.rows[:-1]

    def getActualRows(self):
        #rows = self.txt.split('\n')
        self.last_row_num = len(self.rows) + int(self.config_last_row_num)
        print('===self.last_row_num',self.last_row_num,self.file_data['file_name'])
        #rows = rows[self.config_last_row_num:]
        #for row in reversed(self.rows):
        for row in self.rows:
            self.row = row
            self.time = row[:row.find(' ')]
            
            try:
                self.getActualRowDate()
                self.workWithBans()
                self.workWithOwerload() #TODO I dell reverse
                self.workWithOrderTask()
                self.grabOrdersData()
            except:
                #print('Error with parse row')
                #err_txt = traceback.format_exc()
                #print(err_txt)
                continue
        print('len self.orders_data',len(self.orders_data))

    def getActualRowDate(self):
        self.date = self.current_date
        if self.log_file_is_changed and self.file_started:
            hour = self.time[:2]
            if hour.isdigit():
                hour = int(hour)
            #только в первые записи, где есть 23 часа. Тогда удаляем 1 день
            if hour == 23:
                self.date = self.previous_date
                print('Use self.previous_date')
            #как только появится другой час - перестаем менять дату
            if hour > 0 and hour < 23:
                print(hour,'self.file_started = False')
                self.file_started = False
        #self.date = str(self.date)[:10]
        try:
            self.row_time = dtm.strptime(f'{self.date} {self.time}','%Y-%m-%d %H:%M:%S')
        except:
            self.row_time = dtm.strptime(f'{self.date} {self.time}','%Y-%m-%d %H:%M:%S.%f')
        self.getStartSessionTime()
        self.getLastSessionTime()

    def getLastSessionTime(self):
        if self.last_session_time is None:
            self.last_session_time = self.row_time
        elif self.last_session_time < self.row_time:
            self.last_session_time = self.row_time

    def getStartSessionTime(self):
        if self.start_session_time is None:
            self.start_session_time = self.row_time
        elif self.start_session_time > self.row_time:
            self.start_session_time = self.row_time

    def grabOrdersData(self):
        row = self.row
        if '[' not in row:
            return
        o = OrdersDataFromLog(row,self.bot_name)
        o.vals['file_name'] = str(self.file_data['data_date'])[:10]
        if '<Short>' in row:            
            o.vals['is_short'] = 1
        else:
            o.vals['is_short'] = 0
        row = row.replace('<Short>','')
        self.row = row
        if 'Buy order DONE' in row:
            o.getBuyInfo(self.row_time,self.date)
        if 'JoinCheck:  Cancel' in row or 'JoinRequestCheck: Cancel' in row:
            o.getTasksFromJoinCheck()
            o.getJoinNum()
        if 'Task' in row and 'started' in row.lower():
            o.getStratName()
            if 'JoinNum:' in row:
                o.getJoinNum()
        if 'cID' in row and 'Sell order' in row:#покупка не нужна, т.к. нигде не используется
            o.getCID()
        if 'SELL order DONE !' in row:
            o.getSellInfo(self.row_time)
        if 'date' not in o.vals:
            o.vals['date'] = self.row_time
        self.orders_data.append(o.vals)

    def workWithBans(self):
        '''
        Account ban detected - binance
        This market is BlackListed - moon
        '''
        words = {'binance':'Account ban detected', 'moon':'This market is BlackListed'}
        for k,v in words.items():
            if v in self.row:
                self.bans[k] = 1

    def workWithOrderTask(self):
        row = self.row
        if 'Task' in row and 'started' in row:            
            strat_name = self.getStratNameFromRow(row)
            if strat_name not in self.order_task:
                self.order_task[strat_name] = {'quantity':1}
            else:
                self.order_task[strat_name]['quantity'] += 1

    def workWithOwerload(self):
        if 'API or CPU overload' in self.row:
            self.last_10s = self.time[:-1]
            if self.last_10s not in self.last_10s_vals:
                self.last_10s_vals[self.last_10s] = {}
            self.getOwerload()

    def getOwerload(self):
        row = self.row
        need_add = False
        overload = {'date':self.row_time}
        spl = dict(
            cpu = {'start':'Sys: ','stop':'AppLatency'},
            request = {'start':'API Req: ','stop':'API Orders','stop2':'/'},
            orders_1m = {'start':'API Orders:','stop':'Orders 10S:','stop2':'/'},
            orders_10s = {'start':'Orders 10S:','stop2':'/'},
        )
        for name,s in spl.items():
            stop = row.find(s['stop']) if 'stop' in s else len(row)
            txt = row[row.find(s['start']) + len(s['start']):stop]
            try:
                if 'stop2' in s:
                    val = int(txt[:txt.find(s['stop2'])].strip())
                    max_val = int(txt[txt.find(s['stop2'])+1:].strip())
                    percent = int(val/max_val*100)
                else:
                    val = txt.strip()
                    percent = int(val)
            except:
                percent = 0
            overload[name] = percent
            if percent > 89:
                if name not in self.last_10s_vals.get(self.last_10s,{}):
                    self.last_10s_vals[self.last_10s][name] = ''
                    need_add = True
        if need_add:
            self.overload.append(overload)

class OrdersDataFromLog(ParseDefs):
    def __init__(self,row,bot_name):
        self.row = row
        self.vals = {'bot_name':bot_name}
        self.getTaskAndCoin()

    def getTaskAndCoin(self):
        v = self.row[:self.row.find(')')].split(' ')
        self.vals['coin'] = v[2].replace(':','').replace('<Short>','')
        self.vals['bot_order_id'] = v[-1].replace('(','')

    def getStratName(self):
        self.vals['status'] = 'start'
        self.vals['strat_name'] = self.getStratNameFromRow(self.row)

    def getTasksFromJoinCheck(self):
        self.vals['status'] = 'join_tasks'
        row = self.row
        row = row.replace(' > (JoinNum','> (JoinNum')
        start = 'sells <'
        stop = '> (JoinNum'
        jt = row[row.find(start)+len(start):row.find(stop)].replace(' ',"','")
        current_id = self.vals['bot_order_id']
        self.vals['joined_tasks'] = f"'{jt}'"
        #self.vals['joined_tasks'] = f"'{jt}','{current_id}'"
        self.vals['joined_tasks'] = self.vals['joined_tasks'].replace("'',","")
        
    def getJoinNum(self):
        row = self.row
        start = 'JoinNum: '
        row = row[row.find(start)+len(start):]
        stop = min(row.find(')'),row.find(' '))
        self.vals['join_num'] = row[:stop]

    def getCID(self):
        self.vals['status'] = 'c_id'
        row = self.row
        start = 'cID: '
        self.vals['ex_order_id'] = row[row.find(start)+len(start):].split(' ')[0]
    
    def getSellInfo(self,row_time):
        self.vals['status'] = 'sell'
        row = self.row
        row = row.replace('Avg.Price','sell_price')
        row = row.replace('Delta','profit')
        row = row.replace('Sum','total_spent')
        start = 'Quantity: '
        vals = row[row.find(start):].replace(': ',':').split(' ')
        for v in vals:
            val = v.split(':')
            if len(val) < 2:
                continue
            self.vals[val[0].lower()] = val[1]
        self.vals['profit'] = float(self.vals['profit'][:-1])
        self.vals['total_spent'] = self.vals['total_spent'][:-1] 
        self.vals['close_date'] = row_time

    def getBuyInfo(self,row_time,date):
        self.vals['status'] = 'buy'
        row = self.row
        row = row.replace('Avg.Price','buy_price')
        row = row.replace('Sum','order_size')
        start = 'Opened: '
        vals = row[row.find(start):].replace(': ',':').split(' ')
        for v in vals:
            val = v.split(':',1)
            if len(val) < 2:
                continue
            self.vals[val[0].lower()] = val[1]
        self.vals['buy_date'] = row_time
        self.vals['order_size'] = self.vals['order_size'][:-1] 

        #10:08:58  1000SHIB: [1] (1152005) Task 1152005 started; USDT-1000SHIB (strategy <#f3.f9.m1.m1.str2.d90>) UseAsk: 0.04732 CurAsk: 0.04732  BUY 0% (strategy <#f3.f9.m1.m1.str2.d90>) price)
        #10:08:59  1000SHIB: [1] (1152005) Buy order DONE! FILL: 100%  Opened: 10:08:59.577 Quantity: 211.00000000 Sum: 9.98$ Avg.Price: 0.04729 (ASK + 0% )
        #10:09:00  1000SHIB [1] (1152005)     -- JoinCheck:  Cancel 2 sells <1152005 1152006 > (JoinNum: 257 Key: 2)
        #10:09:02  1000SHIB: [1] (1152021) Task 1152021 (JoinNum: 257) Started; USDT-1000SHIB cur.Ask: 0.04716  AvgPrice = Spent: 19.96$ / Q: 423.00000000 = 0.04718 using strategy #f3.f9.m1.m1.str2.d90  ( DropsDetection ) 
        #10:09:04  1000SHIB [3] (1152007)     -- JoinRequestCheck: Cancel 1 sells <1152021 > (JoinNum: 258) [Key: 2  mKey: 2]
        #10:08:59  1000SHIB: [1] (1152005) USDT-1000SHIB Buy order: 211.0000 USDT-1000SHIB rate: 0.04731 Sum: 9.98$  ID: 3750762199 cID: Bfqqh14Rzvg9xLkrOUmPO
        #10:09:30  1000SHIB: [1] (1152030)   *** SELL order DONE ! ***  Quantity: 636.0000 Avg.Price: 0.04721 Sum: 30.02$ Delta: 0.08$  wsQ:636.0000 wsBTC: 30.02$ rQ: 0.00000000
#%%
"""
path = str(pathlib.Path(__file__).parent.resolve())
fn = 'LOG_2022-05-02.log'
sself = ParseLogFile(path,14,'bot_n',fn,print)
#%%
sself.rows
#%%
sself.orders_data
#%%
"""
# %%
