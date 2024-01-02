#%%
import sqlite3
import pathlib
from datetime import timedelta
from datetime import datetime as dtm
import time        
#%%
class LocalAlexBase():
    def __init__(self,base=None):
        #file_path = str(pathlib.Path(__file__).parent.resolve())
        #base = f'{file_path}\\alex_bd.db'
        if base is not None:
            self.connectBd(base)
        else:
            try:
                base = r'c:\_saver\alex_bd.db'
                self.connectBd(base)
            except:
                print('Not valid',base)
                file_path = str(pathlib.Path(__file__).parent.resolve())
                if '/' in file_path:
                    pref = '/' 
                else:
                    pref = '\\'
                base = f'{file_path}{pref}alex_bd.db'
                print('Change filepath',base)
                self.connectBd(base)
        print('path bd:',base)
        self.offset = 0
        self.columns = None
        self.find_orders_interval_hours = 24
    
    def createTable(self):
        '''
        bot_order_id - это task id в мунботе
        '''
        query = """
            CREATE TABLE IF NOT EXISTS Orders (
            id           INTEGER        PRIMARY KEY AUTOINCREMENT,
            bot_order_id INT,
            bot_name     TEXT,
            coin         TEXT,
            date     DATETIME,
            local_row_create_date     DATETIME,
            buy_task_create_date     DATETIME,
            buy_date     DATETIME,
            moonbot_buy_date     DATETIME,
            close_date   DATETIME,
            orders_in_net    INT,
            quantity     REAL,
            order_size     REAL,
            total_spent     REAL,
            buy_price    REAL,
            moonbot_buy_price    REAL,
            sell_price   REAL,
            profit   REAL,
            profit_percent   REAL,
            base_coin    INT        DEFAULT (1),
            is_short     INT            DEFAULT (0),
            ex_order_id  TEXT,
            join_num     INT,
            main_join_num     INT,
            sell_row_id     INT,
            strat_name  TEXT,
            sell_reason  TEXT,
            emulator    INT    DEFAULT (0),
            orders_list   TEXT,
            file_name  TEXT
        );"""
        self.cur.execute(query)
        query = """CREATE INDEX IF NOT EXISTS order_bot_coin ON Orders (
            bot_order_id,coin,bot_name
        );"""
        self.cur.execute(query)
        #query = """CREATE INDEX IF NOT EXISTS order_join_bot_coin ON Orders (
        #    coin,bot_name,date,join_num,main_join_num
        #);"""
        #self.cur.execute(query)
        try:#TODO позже убрать
            query = """alter table Orders add column file_name TEXT;"""
            self.cur.execute(query)
        except:
            pass
        try:#TODO позже убрать
            query = """alter table Orders add column sell_row_id INT default -1;"""
            self.cur.execute(query)
        except:
            pass
        try:#TODO позже убрать
            query = """alter table Orders add column local_row_create_date DATETIME;"""
            self.cur.execute(query)
        except:
            pass
        try:#TODO позже убрать
            query = """alter table Orders add column buy_task_create_date DATETIME;"""
            self.cur.execute(query)
        except:
            pass
        try:#TODO позже убрать
            query = """alter table Orders add column orders_list TEXT;"""
            self.cur.execute(query)
        except:
            pass
        try:#TODO позже убрать
            query = """alter table Orders add column comment TEXT;"""
            self.cur.execute(query)
        except:
            pass
        try:#TODO позже убрать
            query = """alter table Orders add column emulator INT default 0;"""
            self.cur.execute(query)
        except:
            pass
        #TODO del             buy_date_first_order   DATETIME,

    def connectBd(self,base):
        print('alex_base',base)
        self.local_db = sqlite3.connect(base, timeout=10)
        self.cur = self.local_db.cursor()
        self.createTable()

        #10:08:58  1000SHIB: [1] (1152005) Task 1152005 started; USDT-1000SHIB (strategy <#f3.f9.m1.m1.str2.d90>) UseAsk: 0.04732 CurAsk: 0.04732  BUY 0% (strategy <#f3.f9.m1.m1.str2.d90>) price)
        #10:08:59  1000SHIB: [1] (1152005) Buy order DONE! FILL: 100%  Opened: 10:08:59.577 Quantity: 211.00000000 Sum: 9.98$ Avg.Price: 0.04729 (ASK + 0% )
        #10:09:00  1000SHIB [1] (1152005)     -- JoinCheck:  Cancel 2 sells <1152005 1152006 > (JoinNum: 257 Key: 2)
        #10:09:02  1000SHIB: [1] (1152021) Task 1152021 (JoinNum: 257) Started; USDT-1000SHIB cur.Ask: 0.04716  AvgPrice = Spent: 19.96$ / Q: 423.00000000 = 0.04718 using strategy #f3.f9.m1.m1.str2.d90  ( DropsDetection ) 
        #10:09:04  1000SHIB [3] (1152007)     -- JoinRequestCheck: Cancel 1 sells <1152021 > (JoinNum: 258) [Key: 2  mKey: 2]
        #10:08:59  1000SHIB: [1] (1152005) USDT-1000SHIB Buy order: 211.0000 USDT-1000SHIB rate: 0.04731 Sum: 9.98$  ID: 3750762199 cID: Bfqqh14Rzvg9xLkrOUmPO
        #10:09:30  1000SHIB: [1] (1152030)   *** SELL order DONE ! ***  Quantity: 636.0000 Avg.Price: 0.04721 Sum: 30.02$ Delta: 0.08$  wsQ:636.0000 wsBTC: 30.02$ rQ: 0.00000000

    def executeQuery(self,query,values):
        self.cur.execute(query,values)
        self.commitChanges()

    def commitChanges(self):
        try:
            self.local_db.commit()
        except:
            time.sleep(2)
            self.local_db.commit()

    def closeConnections(self):
        try:
            self.cur.close()
            self.local_db.close()
        except:
            pass

    def generateKeysAndValues(self,values,keys):
        actual_keys = []
        self.actual_vals = []
        for key in keys:
            if key in values:
                actual_keys.append(key)
                self.actual_vals.append(values[key])
        self.keys_str = ', '.join(actual_keys)
        self.keys_for_update_str = '=?, '.join(actual_keys) + '=?'
        self.vals_mask = ('?,' * len(actual_keys))[:-1]

    def generateSelectConditions(self,values):
        #used bot_order_id
        self.conditions = [values['bot_order_id'],values['coin'],values['bot_name'],
            values['date']-timedelta(hours=self.find_orders_interval_hours)]
        self.conditions_keys = 'bot_order_id=? and coin=? and bot_name=? and date>?'

    def generateSelectConditionsForJoinedTasks(self,values):
        #used bot_order_id
        self.conditions = [values['coin'],values['bot_name'],
            values['date']-timedelta(hours=self.find_orders_interval_hours),
            values['date']+timedelta(minutes=1)]
        jt = values['joined_tasks']
        self.conditions_keys = \
            f'bot_order_id in ({jt}) and coin=? and bot_name=? and \
            date>? and date<=?'

    def updateRow(self,values,keys):
        self.generateSelectConditions(values)
        self.generateKeysAndValues(values,keys)        
        add_cond = 'ex_order_id is null and ' if 'ex_order_id' in keys else ''
        query = f"""UPDATE Orders set {self.keys_for_update_str}
            WHERE {add_cond}{self.conditions_keys};"""
        self.executeQuery(query,tuple(self.actual_vals)+tuple(self.conditions))

    def selectFirstRow(self):
        query = f"""SELECT buy_date from Orders where buy_date is not null ORDER BY buy_date;"""
        self.cur.execute(query)
        self.data_from_bd = self.cur.fetchone()
        
    def updateBuyRow(self,row_id,sell_row_id):
        query = f"""UPDATE Orders set sell_row_id=?
            WHERE id=?;"""
        self.executeQuery(query,tuple([sell_row_id,row_id]))
        
    def addNetUnionKey(self,rows_id_list,sell_row_id):
        self.vals_mask = ('?,' * len(rows_id_list))[:-1]
        query = f"""UPDATE Orders set sell_row_id=?
            WHERE id in ({self.vals_mask}) and (sell_row_id = -1 or sell_row_id is null);"""
        self.executeQuery(query,tuple([sell_row_id])+tuple(rows_id_list))

    def createRow(self,values):
        keys = ['bot_order_id','strat_name','coin','join_num','main_join_num',\
            'bot_name','date','is_short','local_row_create_date']
        self.generateKeysAndValues(values,keys)
        query = f"""INSERT INTO Orders ({self.keys_str})
            VALUES ({self.vals_mask});"""
        self.executeQuery(query,tuple(self.actual_vals))

    def createManyRows(self,dataset,logger_err):
        keys = ['bot_order_id','strat_name','coin','join_num','main_join_num',\
            'bot_name','date','is_short','local_row_create_date','file_name']
        keys_str = ', '.join(keys)
        vals = '?,' * len(keys)
        vals = vals[:-1]
        rows = []
        for d in dataset:
            row = []
            for k in keys:
                row.append(d.get(k,None))
            rows.append(row)
        if len(rows) == 0:
            return
        querry = f"""INSERT INTO Orders ({keys_str})
            VALUES ({vals});"""
        try:
            self.cur.executemany(querry, rows)
            self.commitChanges()
        except:
            for row in rows:
                try:
                    self.cur.executemany(querry, [row])
                    self.commitChanges()
                except:
                    logger_err.debug(f'Error with createManyRows. Row: {row}, query {querry}')
                    #print(f'Error with createManyRows. Row: {row}, query {querry}')

    def deleteUnusedRows(self):
        query = f"""DELETE FROM Orders WHERE date<? and buy_date is null and close_date is null;"""
        date = dtm.now() - timedelta(hours=12)
        #date = dtm.now() - timedelta(hours=0)#TODO del
        self.executeQuery(query,tuple([date]))

    def deleteBotsUnusedRows(self,bot_name,last_session_time):
        query = f"""DELETE FROM Orders WHERE bot_name = ? and date<? and 
        buy_date is null and close_date is null;"""
        self.executeQuery(query,tuple([bot_name,last_session_time]))

    def deleteAllRows(self):
        query = f"""DELETE FROM Orders WHERE 1;"""
        self.executeQuery(query,tuple())

    def deleteAllOldRows(self,days):
        query = f"""DELETE FROM Orders WHERE date<?;"""
        date = dtm.now() - timedelta(hours=days*12)
        self.executeQuery(query,tuple([date]))

    def delBotDataInBd(self,bot_name,first_date):
        query = """
            DELETE FROM Orders WHERE bot_name = ? and date>?;"""
        self.executeQuery(query,(bot_name,first_date))
        print('Orders are deleted. Bot name',bot_name)

    def readTask(self,values):
        bot_order_id = values['bot_order_id']
        values['joined_tasks'] = f'"{bot_order_id}"'
        self.generateSelectConditionsForJoinedTasks(values)
        query = f"""SELECT * from Orders where {self.conditions_keys};"""
        self.executeQuery(query,tuple(self.conditions))
        self.data_from_bd = self.cur.fetchall()
        self.createColumns()

    def readAllTasks(self):
        query = f"""SELECT * from Orders where date > ?;"""#bot_name = ? and #orders_in_net != 1 and join_num is not null and
        self.executeQuery(query,tuple())
        self.data_from_bd = self.cur.fetchall()
        self.createColumns()
        
    def readAllTasksForUpdateBug(self,bot_name):
        #в запросе не отрабатывает orders_in_net != 1
        query = f"""SELECT * from Orders where bot_name = ? and join_num is not null and 
                (orders_in_net > 1 or orders_in_net is null);"""#bot_name = ? and #orders_in_net != 1 and join_num is not null and
        self.executeQuery(query,tuple([bot_name]))
        self.data_from_bd = self.cur.fetchall()
        self.createColumns()
        
    def readJoinNum(self,values):
        self.generateSelectConditionsForJoinedTasks(values)
        query = f"""SELECT date,bot_order_id,join_num,main_join_num,bot_name,coin,strat_name from Orders where 
            {self.conditions_keys} 
            ORDER BY 'date';"""
        self.executeQuery(query,tuple(self.conditions))
        self.data_from_bd = self.cur.fetchall()
        self.createColumns()

    def generateSelectConditionsForMainJoin(self,values):
        self.conditions = [values['coin'],values['bot_name'],
            values['date']-timedelta(hours=self.find_orders_interval_hours),
            values['date']+timedelta(minutes=1),values['join_num']]
        self.conditions_keys = \
            f'coin=? and bot_name=? and date>? and date<=? \
            and join_num=? and main_join_num is not null'

    def getMainJoinNum(self,values):
        self.generateSelectConditionsForMainJoin(values)
        query = f"""SELECT join_num,main_join_num,strat_name from Orders where 
            {self.conditions_keys} 
            ORDER BY 'date';"""
        self.executeQuery(query,tuple(self.conditions))
        self.data_from_bd = self.cur.fetchall()
        self.createColumns()
    
    def generateSelectConditionsFirstBuyDate(self,values,time_line):
        self.conditions = [values['coin'],values['bot_name'],
            values['date']-timedelta(minutes=time_line),values['date']+timedelta(minutes=1),
            values['strat_name'],values['bot_order_id']]
        jn = values['joined_nums']
        self.conditions_keys = \
            f'(main_join_num in ({jn}) or join_num in ({jn})) and coin=? and bot_name=? and date>? \
            and date<? and strat_name=? and (buy_date is not null or bot_order_id=?)'

    def getJoinedOrders(self,values,time_line):
        self.generateSelectConditionsFirstBuyDate(values,time_line)
        query = f"""SELECT date,buy_date,main_join_num,join_num,order_size,quantity,buy_price,\
            id,bot_order_id from Orders where 
            {self.conditions_keys} 
            ORDER BY 'date','bot_order_id';"""
        self.executeQuery(query,tuple(self.conditions))
        self.data_from_bd = self.cur.fetchall()
        self.createColumns()

    def generateSelectConditionsAllOrders(self,values):
        self.conditions = [values['bot_name'],values['date']]
        self.conditions_keys = \
            f'bot_name=? and close_date>?'

    def getAllOrders(self,values):
        self.generateSelectConditionsAllOrders(values)
        query = f"""SELECT * from Orders where 
            {self.conditions_keys} 
            ORDER BY 'close_date';"""
        self.executeQuery(query,tuple(self.conditions))
        self.data_from_bd = self.cur.fetchall()
        self.createColumns()

    def generateSelectConditionsCloseNulled(self,values):
        self.conditions = [values['bot_name'],values['date']]
        self.conditions_keys = \
            f'bot_name=? and buy_date<?'

    def getCloseNulledOrders(self,values):
        self.generateSelectConditionsCloseNulled(values)
        query = f"""SELECT * from Orders where 
            {self.conditions_keys} and close_date is null and moonbot_buy_date is not null
            ORDER BY 'close_date';"""
        self.executeQuery(query,tuple(self.conditions))
        #self.executeQuery(query,tuple([]))
        self.data_from_bd = self.cur.fetchall()
        self.createColumns()
        
    def createColumns(self):
        if not self.columns:
            self.column_names = [d[0] for d in self.cur.description]
            #self.remaneColumns()

#%%

