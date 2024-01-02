#%%
import sqlite3
import time

class LocalBase():
    def __init__(self,base,main_data):
        self.connectBd(base)
        self.limit = int(main_data['local_bd_rows_limit'])
        self.select_limit = 30
        self.offset = 0
        self.columns = None
        self.ren_dict = {
            'Coin':'coin',
            'BuyDate':'buy_date',
            'CloseDate':'close_date',
            'Quantity':'quantity',
            'BuyPrice':'buy_price',
            'SellPrice':'sell_price',
            'ProfitBTC':'profit',
            'BaseCurrency':'base_coin',
            'IsShort':'is_short',
            'exOrderID':'ex_order_id',
            'ID':'bot_order_id',
            'TaskID':'task_id',
            'Exchange24hDelta':'market24h',
            'Exchange1hDelta':'market1h',
            'bvsvRatio':'bvsv_current',
            'BTC24hDelta':'btc24h',
            'BTC1hDelta':'btc1h',
            'BTC5mDelta':'btc5m',
            'dBTC1m':'btc1m',
            'Pump1H':'pump1h',
            'Dump1H':'dump1h',
            'd24h':'c24h',
            'd3h':'c3h',
            'd1h':'c1h',
            'd15m':'c15m',
            'd5m':'c5m',
            'd1m':'c1m',
            'Emulator':'emulator',
                    }

    def connectBd(self,base):
        self.local_db = sqlite3.connect(f'file:{base}?mode=ro', uri=True)
        self.cur = self.local_db.cursor()

    def sendQuery(self,last_date,logger_err):
        query = """SELECT * from Orders where CloseDate > ? and deleted = 0 and
            Status = 1 and ChannelName != 'Manual'
            ORDER BY 'CloseDate' LIMIT ? OFFSET ?;"""#and Source = 2 Emulator = 0 and
        i = 0
        while i < 3:    
            try:
                self.cur.execute(query,(last_date, self.limit, self.offset))
                self.data = self.cur.fetchall()
                return True
            except:
                time.sleep(1)
                #self.cur.execute(query,(last_date, self.limit, self.offset))
                logger_err.debug(f'Try connect to moon bd again')
            i += 1
        return False

    def readSelectedData(self,col_name,list_of_ids,logger_err):
        if not self.selectSomeOrders(col_name,list_of_ids,logger_err):
            self.data = []
            logger_err.debug(f'Cant connect with moon bd')
            print((f'Cant connect with moon bd'))
            return
        self.createColumns()
        
    def selectSomeOrders(self,col_name,list_of_ids,logger_err):
        col_name = self.renameColumnToMoonBD(col_name)
        format_strings = ','.join(['?'] * len(list_of_ids))           
        query = f"""SELECT * from Orders where CloseDate is not null and deleted = 0 and
            Status = 1 and ChannelName != 'Manual' and {col_name} in ({format_strings})
            ORDER BY 'CloseDate' LIMIT ?;"""
        i = 0
        while i < 3:
            try:
                self.cur.execute(query,(*list_of_ids,self.select_limit))
                self.data = self.cur.fetchall()
                self.createColumns()
                return True
            except:
                time.sleep(1)
                logger_err.debug(f'Try connect to moon bd again')
            i += 1
        return False

    def closeConnection(self):
        try:
            self.cur.close()
            self.local_db.close()
        except:
            pass

    def readData(self,last_date,logger_err):
        if not self.sendQuery(last_date,logger_err):
            self.data = []
            logger_err.debug(f'Cant connect with moon bd')
            return
        self.offset += self.limit
        self.createColumns()
    
    def createColumns(self):
        if not self.columns:
            self.columns = [d[0] for d in self.cur.description]
            self.renameColumns()

    def renameColumns(self):
        for i,v in enumerate(self.columns):
            if v in self.ren_dict:
                self.columns[i] = self.ren_dict[v]
    
    def renameColumnToMoonBD(self,col):
        for k,v in self.ren_dict.items():
            if col == v:
                return k
        return col
                
#%%
# %%
