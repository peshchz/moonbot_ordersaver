#%%
import os,sys
from datetime import timedelta
from datetime import datetime as dtm
import configparser
import sqlite3
import pymysql
import time
import random
import pickle
import logging
from logging.config import fileConfig
import traceback
import json
from settings import server_data#,bases,main_data


class Strategy():
    def getAllStrategyTypes(self):
        self.cur.execute("""
            SELECT * from strategy_strategytype""")
        rows = self.cur.fetchall()
        self.strat_types = {}
        for r in rows:
            self.strat_types[r[1]] = {'id':r[0]}

    def getAllStrategies(self):
        keys = ['id','name','last_edit_date','strategy_type_id','market_type','site','order_size',
            'm_orders_cnt','m_price_step','m_o_size_step','m_o_size_kind','need_reload']
        query = f"""
            SELECT {', '.join(keys)} from strategy_strategy where user_id = %s;"""
        self.cur.execute(query,(self.user_id))
        rows = self.cur.fetchall()
        self.strats = {}
        for r in rows:
            row = dict(zip(keys,r))
            market_type_site = (row['market_type'],row['site'])
            if market_type_site not in self.strats:
                self.strats[market_type_site] = {}
            #if row['name'] not in self.strats[market_type_site]:
            self.strats[market_type_site][row['name']] = {**row}
                #'id':row['id'],'type':row['strategy_type_id'],'last_edit_date':row['last_edit_date'],
        if hasattr(self,'logger'):
            self.logger.debug('Strats downloaded')

    def getStrategySettings(self):
        self.cur.execute("""SELECT * from strategy_strategysettings;""")
        rows = self.cur.fetchall()
        self.strat_settings = {}
        for r in rows:
            self.strat_settings[r[1]] = {'id':r[0]}

    def _checkCorrectStratName_del(self,name):
        '''Костыль для замены кривых имен'''
        if 'Task ' in name or len(name) > 50:
            name = 'Unknown'
        return name

    def saveStratSettings(self,parsed_strat):
        self.getStrategySettings()
        #self.stop_save_strats = False
        if set(parsed_strat).issubset(set(self.strat_settings)):
            pass
        elif self.is_main_create_settings_bot == 1:
            self.saveNewStratSettingsToBd(parsed_strat)
            self.getStrategySettings()
        else:
            pass
            #print(set(parsed_strat).issubset(set(self.strat_settings)))
            #print('setting',set(parsed_strat).difference(set(self.strat_settings)))
        self.createOrUpdateStrat('update',parsed_strat,self.market_type)

    def getStrat(self,r):
        #strat_is_found = False
        #for k in ['ChannelName','Comment']:
        #    txt = r[k]
        #    if txt.find('<') > -1 and txt.find('>') > -1:
        #        strat_name = txt[txt.find('<')+1:txt.find('>')]
        #        strat_is_found = True
        #        break
        #if not strat_is_found:
        #    strat_name = self.findStratInText(r['Comment'])
        #strat_name = self.checkCorrectStratName(r['strat_name'])
        strat_name = r['strat_name']
        market_type_site = (self.market_type,self.site)
        self.market_type_site = market_type_site
        if strat_name not in self.strats[market_type_site]:
            parsed_strat = {'LastEditDate':{'value':dtm.now().strftime("%Y-%m-%d %H:%M")},
                'OrderSize':{'value':10},
                'JoinSellKey':{'value':0},'TriggerKey':{'value':0},
                'TriggerByKey':{'value':0},'OrdersCount':{'value':1},
                'BuyPriceStep':{'value':0},'OrderSizeStep':{'value':0},
                'OrderSizeKind':{'value':0},'Active':{'value':0},
                'StrategyName':{'value':strat_name},'SignalType':{'value':'Unknown'},
                }
            self.createOrUpdateStrat('create',parsed_strat,self.market_type)
            self.getAllStrategies()
        self.strat_id = self.strats[market_type_site][strat_name]['id']
        self.current_strat = self.strats[market_type_site][strat_name]

    def prepareKeys(self):
        keys_main = [
            'last_edit_date',
            'order_size','join_sell_key','trigger_key','trigger_by_key','m_orders_cnt',
            'm_price_step','m_o_size_step','m_o_size_kind','settings','is_active','bot_id','path']
        keys_create = ['user_id','name','strategy_type_id','market_type','site','create_date']
        if self.query_type == 'create':
            keys_create.extend(keys_main)
            self.keys = keys_create
        else:
            self.keys = keys_main

    def prepareValues(self):
        parsed_strat = self.parsed_strat
        is_active = 1 if int(self.parsed_strat['Active']['value']) == -1 else 0
        last_edit_date = parsed_strat['LastEditDate']['value']
        if len(last_edit_date) < 2:
            last_edit_date = dtm.now().strftime("%Y-%m-%d %H:%M")
        if 'folder' in parsed_strat:
            folder = parsed_strat['folder']['value'] 
        else:
            folder = ''
            self.logger_err.debug('Error with strat folder')
            self.logger_err.debug(parsed_strat)
        order_size = parsed_strat['OrderSize']['value']
        if 'k' in str(order_size):
            order_size = float(order_size.replace('k',''))*1000
        elif not str(order_size).replace('.','',1).isdigit():
            print('bad ordersize',order_size)
            print(parsed_strat['StrategyName']['value'])
            order_size = 111
        values_main = [
            last_edit_date,order_size,
            parsed_strat['JoinSellKey']['value'],parsed_strat['TriggerKey']['value'],
            parsed_strat['TriggerByKey']['value'],parsed_strat['OrdersCount']['value'],
            parsed_strat['BuyPriceStep']['value'],parsed_strat['OrderSizeStep']['value'],
            parsed_strat['OrderSizeKind']['value'],
            self.strat_json, is_active,self.bot_id,folder]
        if self.query_type == 'create':
            strategy_type_id = self.getStrategyType(parsed_strat['SignalType']['value'])
            values_create = [
                self.user_id,parsed_strat['StrategyName']['value'],strategy_type_id,
                self.market_type,self.site,dtm.now().strftime("%Y-%m-%d %H:%M")
                ]
            values_create.extend(values_main)
            self.values = values_create
        else:
            strat_id = self.strats[(self.market_type,self.site)][parsed_strat['StrategyName']['value']]['id']
            values_main.extend([strat_id])
            self.values = values_main
    
    def prepareQuery(self):
        if self.query_type == 'create':
            keys_str = ', '.join(self.keys)
            vals = '%s,' * len(self.keys)
            vals = vals[:-1]
            self.querry = f"""INSERT INTO strategy_strategy ({keys_str})
            VALUES ({vals});"""
        elif self.query_type == 'update':                   
            self.querry = """
                UPDATE strategy_strategy SET last_edit_date = %s, order_size = %s, join_sell_key = %s, 
                trigger_key = %s, trigger_by_key = %s, 
                m_orders_cnt = %s, m_price_step = %s, m_o_size_step = %s, m_o_size_kind = %s,            
                settings = %s, is_active = %s, bot_id = %s, path = %s
                WHERE id = %s;"""

    def createOrUpdateStrat(self,query_type,parsed_strat,market_type):
        self.query_type = query_type
        self.parsed_strat = parsed_strat
        self.market_type = market_type
        self.createJsonWithSettings(parsed_strat)        
        self.prepareKeys()
        self.prepareValues()
        self.prepareQuery()
        try:
            self.cur.execute(self.querry,self.values)
        except:
            print('err with save strat')
            print(self.querry)
            print(self.values)

    def createJsonWithSettings(self,parsed_strat):
        coded_sett = {}
        for setting_name,val in parsed_strat.items():
            if setting_name in self.strat_settings:
                sett_id = self.strat_settings[setting_name]['id']
            else:
                sett_id = setting_name
            coded_sett[sett_id] = val['value']
        self.strat_json = json.dumps(coded_sett)

    def saveNewStratSettingsToBd(self,parsed_strat):
        for setting_name,val in parsed_strat.items():
            if setting_name not in self.strat_settings:
                self.cur.execute("""
                    INSERT INTO strategy_strategysettings (name,sort_order) 
                    VALUES (%s,%s);""", 
                    (setting_name,val['sort_order']))

    def fixReloadFact(self):
        self.cur.execute("""
            UPDATE strategy_strategy SET need_reload  = 0
            WHERE id = %s;""",(self.current_strat['id']))

    def getStrategyType(self,strategy_type):
        if strategy_type not in self.strat_types:
            self.cur.execute("""
                INSERT INTO strategy_strategytype (name) VALUES (%s);""",
                strategy_type)
            self.getAllStrategyTypes()
        return self.strat_types[strategy_type]['id']

    def findStratInText(self,txt):
        market_type_site = (self.market_type,self.site)
        names = []
        for strat_name in self.strats[market_type_site]:
            if txt.find(strat_name) > -1:
                names.append(strat_name)
        if len(names) == 1:
            return names[0]
        if len(names) == 0:
            return 'unknown'
        max_len_name = ''
        for name in names:
            if len(name) > len(max_len_name):
                max_len_name = name
        return max_len_name
    
    def saveOrderTask(self,dataset):
        add_date = dtm.now().strftime("%Y-%m-%d %H:%M")
        keys = ['strategy_id','quantity','add_date','user_id']
        keys_str = ', '.join(keys)
        vals = '%s,' * len(keys)
        vals = vals[:-1]
        rows = []
        for _,d in dataset.items():
            row = []
            d['add_date'] = add_date
            d['user_id'] = self.user_id
            for k in keys:
                row.append(d[k])
            rows.append(row)
        if len(rows) == 0:
            return
        querry = f"""INSERT INTO orders_ordertask ({keys_str})
            VALUES ({vals});"""
        try:
            self.cur.executemany(querry, rows)
        except:
            for row in rows:
                try:
                    self.cur.executemany(querry, [row])
                except:
                    self.logger_err.debug(f'Error with saveOrderTask. Probably bad strat_id: {row}, query {querry}')

    def saveBans(self,dataset):
        add_date = dtm.now().strftime("%Y-%m-%d %H:%M")
        self.cur.execute("""
            INSERT INTO bot_bans (bot_id,user_id_id,date,moon,binance) 
            VALUES (%s,%s,%s,%s,%s);""", 
            (self.bot_id,self.user_id,add_date,dataset['moon'],dataset['binance']))

class Bot():
    def getAllBots(self):
        self.cur.execute("""
            SELECT id,name,need_reload_all_data,is_main_create_settings_bot,need_reload_txt_log from bot_bot where user_id = %s ORDER BY name ASC;""", 
            (self.user_id))
        rows = self.cur.fetchall()
        self.createColumns()
        self.bots = {}
        #self.bot_for_reload_data = None
        all_need_reload = []
        for r in rows:
            row = dict(zip(self.column_names,r))
            self.bots[row['name']] = row
            #if self.bot_for_reload_data is None: #Выберем первого бота для обновления данных
            #    if int(row['need_reload_all_data']) == 1:
            #        self.bot_for_reload_data = row['name']
            if int(row['need_reload_all_data']) == 1:
                all_need_reload.append(row['name'])
        if len(all_need_reload) >= 2:            
            self.bot_for_reload_data = [all_need_reload[0],all_need_reload[-1]]
        elif len(all_need_reload) == 1:
            self.bot_for_reload_data = all_need_reload
        else:
            self.bot_for_reload_data = []

    def checkBot(self,bot_name):
        if bot_name not in self.bots:
            self.cur.execute("""
                INSERT INTO bot_bot (user_id,name,saver_version,correct_timezone_date,
                need_reload_all_data,need_reload_txt_log,is_main_create_settings_bot,
                need_check_work_status) 
                VALUES (%s,%s,0,%s,0,0,0,1);""", 
                (self.user_id,bot_name,dtm.now()-timedelta(hours=24)))
            self.getAllBots()
        self.bot_id = self.bots[bot_name]['id']
        self.is_main_create_settings_bot = self.bots[bot_name]['is_main_create_settings_bot']
        self.need_reload_all_data = True if bot_name in self.bot_for_reload_data else False
        #self.need_reload_all_data = self.bots[bot_name]['need_reload_all_data']
        self.need_reload_txt_log = self.bots[bot_name]['need_reload_txt_log']
        if self.need_reload_txt_log:
            self.need_reload_all_data = False#чтобы очередность в обновлении сохранить
        print('need_reload_all_data',self.need_reload_all_data)
        print('need_reload_txt_log',self.need_reload_txt_log)

    def saveBotData(self):
        self.fixEndReloadAllData()
        self.fixEndReloadLogTxt()
        self.writeLastWorkDate()
        self.getAllBots()

    def fixEndReloadAllData(self):
        if self.need_reload_all_data:
            self.cur.execute("""
                UPDATE bot_bot SET need_reload_all_data = 0, last_reload_date = %s
                WHERE id = %s;""",(dtm.now(),self.bot_id))

    def fixEndReloadLogTxt(self):
        if self.need_reload_txt_log:
            self.cur.execute("""
                UPDATE bot_bot SET need_reload_txt_log = 0
                WHERE id = %s;""",(self.bot_id))
                
    def writeLastWorkDate(self):
        self.cur.execute("""
            UPDATE bot_bot SET last_work_date = %s
            WHERE id = %s;""",(dtm.now(),self.bot_id))

    def checkSaverVersion(self):
        self.cur.execute("""SELECT value from bot_ordersaver where param='version';""")
        version = int(self.cur.fetchone()[0])
        if self.config_version < version:
            self.config.changeConfig('main_data','version',version)
            self.config.changeConfig('main_data','need_download_saver',1)
            print('need_download_saver')
        self.updateVersionInBd()

    def delAllBotDataInBd(self,first_date):
        if self.need_reload_all_data:
            print('Try delete orders')
            self.cur.execute("""
                DELETE FROM orders_orders WHERE bot_id = %s and buy_date>=%s;""",\
                    (self.bot_id,first_date))
            print('Orders are deleted. Bot id',self.bot_id)

    def updateVersionInBd(self):
        if self.need_update_version_in_bd:
            self.config.changeConfig('main_data','need_update_version_in_bd',0)
            for bot_name in list(self.config.bases):
                bot_id = self.bots[bot_name]['id']
                self.cur.execute("""
                    UPDATE bot_bot SET saver_version = %s
                    WHERE id = %s;""",(self.config_version,bot_id))
    
    def saveOverloadLog(self,dataset):
        keys = ['bot_id','date','cpu','request','orders_1m','orders_10s']
        keys_str = ', '.join(keys)
        vals = '%s,' * len(keys)
        vals = vals[:-1]
        rows = []
        for d in dataset:
            row = []
            d['bot_id'] = self.bot_id
            for k in keys:
                row.append(d[k])
            rows.append(row)
        querry = f"""INSERT INTO bot_overload ({keys_str})
            VALUES ({vals});"""
        self.cur.executemany(querry, rows)

class Server(Strategy,Bot):
    def __init__(self,server_data,config,logger,logger_err):
        self.logger = logger
        self.logger_err = logger_err
        server_data['autocommit']=True
        self.con = pymysql.connect(**server_data)
        self.cur = self.con.cursor()
        self.connection = True
        main_data = config.config['main_data']
        self.config = config
        self.user_id = main_data['user_id']
        self.site = main_data['site']
        self.config_version = int(main_data.get('version',0))
        #self.is_main_create_settings_bot = int(main_data.get('main_create_settings_bot',0))
        self.need_update_version_in_bd = \
            int(main_data.get('need_update_version_in_bd',0))
        self.strats = {}        
        try:
            self.getAllBots()
            self.getAllStrategies()
            self.getAllStrategyTypes()
            self.getAllBaseCoins()
            self.getStrategySettings()
            self.checkSaverVersion()
        except:
            self.connection = False
        #self.limit = int(main_data['remote_bd_rows_limit'])
        self.limit = 500

    def createColumns(self):
        self.column_names = [d[0] for d in self.cur.description]

    def getAllBaseCoins(self):
        self.cur.execute("""SELECT * from bot_basecoin;""")
        rows = self.cur.fetchall()
        self.base_coins = {}
        for r in rows:
            self.base_coins[r[2]] = {'id':r[0]}

    def getBaseCoin(self,base_coin_moon_id):
        if not str(base_coin_moon_id).isdigit():
            base_coin_moon_id = 1
        if base_coin_moon_id not in self.base_coins:
            self.cur.execute("""
                INSERT INTO bot_basecoin (base_coin_moon_id) VALUES (%s);""",
                base_coin_moon_id)
            self.getAllBaseCoins()
        return self.base_coins[base_coin_moon_id]['id']

    def sendData(self,rows,keys):
        keys_str = ', '.join(keys)
        vals = '%s,' * len(keys)
        vals = vals[:-1]
        querry = f"""INSERT INTO orders_orders ({keys_str})
            VALUES ({vals});"""
        self.cur.executemany(querry, rows)

    def _getSellReason(self,txt):
        reasons = {'StopLoss':'SLoss','Sell Price':'SellPr','LIQUIDATION':'Liquid',
            'TrailingStop':'Trail','Auto Price':'PrDown','Global PanicSell':'Panic',
            'JoinedSell':'Jsell','Sell by FilterCheck':'FiltCheck',}
        for reason,k in reasons.items():
            if reason in txt:
                return k
        return 'Other'

    def getSellCondition(self,txt):
        conditions = {'Joined Sell':'JoinedSell'}
        for reason,k in conditions.items():
            if reason in txt:
                return k
        return None

    def addData(self):
        r = self.r
        r['user_id'] = self.user_id
        r['bot_id'] = self.bot_id
        try:
            self.getStrat(r)
        except:
            return False
        r['strategy_id'] = self.strat_id
        #try:
        #    fact_spent_usd = r['buy_price'] * r['quantity']
        #    r['profit_percent'] = round((r['profit'] / fact_spent_usd * 100),2)
        #except:
        #    r['profit_percent'] = 0
        #r['sell_reason'] = self.getSellReason(r['SellReason'])
        #r['sell_condition'] = self.getSellCondition(r['Comment'])
        #r['buy_date'] = dtm.utcfromtimestamp(r['buy_date'])
        #r['close_date'] = dtm.utcfromtimestamp(r['close_date'])
        r['site'] = self.site
        r['base_coin_id'] = self.getBaseCoin(r['base_coin'])
        for col in ['strategy_id','base_coin_id']:
            if r.get(col,None) is None:
                return False
        r['market_type'] = self.market_type
        r['add_date'] = self.now
        if r['buy_task_create_date'] is not None:
            r['order_create_date'] = r['buy_task_create_date']
        else:
            r['order_create_date'] = r['date']
        try:
            if (self.current_strat['order_size']/r['order_size']-1) > 0.2:#если размер ордера вырос за сессию
                r['profit_percent'] = round((r['profit'] / self.current_strat['order_size'] * 100),2)
        except:
            pass
        try:
            if r['profit_percent'] is None:
                r['profit_percent'] = round((r['profit'] / r['order_size'] * 100),2)
        except:
            r['profit_percent'] = 0
        r['order_size_in_settings'] = self.current_strat['order_size']
        r['first_order_size'] = r['order_size']
        if 'sell_reason' not in r:
            r['sell_reason'] = 'NoData'
        if r['buy_date'] is None and r['moonbot_buy_date'] is not None:
            r['buy_date'] = r['moonbot_buy_date']
        if r['buy_date'] is None:
            r['buy_date'] = r['date']
        if r['sell_reason'] is None:
            r['sell_reason'] = 'NoData'
        if 'orders_list' not in r:
            r['orders_list'] = None
        if r['buy_price'] is None:
            if r['moonbot_buy_price'] is not None:
                r['buy_price'] = r['moonbot_buy_price']
            else:
                r['buy_price'] = r['sell_price'] / (1 + r['profit_percent']/100)
                #TODO удалить костыль после перехода на АлексБД
        self.r = r
        return True

    def _old__calcMultiOrder(self,spent_money):
        balance = spent_money + 0
        strat_order_size = self.current_strat['order_size']
        size_step  = self.current_strat['m_o_size_step']
        buy_price_step = self.current_strat['m_price_step']
        multi_o_cnt = self.current_strat['m_orders_cnt']
        fact_orders_in_net = 0 #куплено ордеров в сетке
        self.fact_multi_o_size = 0 #потрачено на ордера в сетке
        while fact_orders_in_net < multi_o_cnt and balance > 0:
            fact_orders_in_net += 1
            if self.current_strat['m_o_size_kind'] == 'Linear':
                order_size_k =  1 + size_step/100*(fact_orders_in_net-1)
            else:
                order_size_k =  (size_step/100)**(fact_orders_in_net-1)
            current_price = strat_order_size *(1 + buy_price_step/100*(fact_orders_in_net-1))
            self.fact_multi_o_size += current_price * order_size_k
            balance -= current_price * order_size_k
            if balance < strat_order_size * 0.3:
                balance = 0
        self.fact_orders_in_net = round(fact_orders_in_net,1)
        nets_cnt = spent_money / self.fact_multi_o_size
        self.total_order_nets = int(nets_cnt) if nets_cnt%1 < 0.2 else int(nets_cnt)+1
        if self.total_order_nets == 0:
            self.total_order_nets = 1

    def _old_addMultiorderData(self):
        r = self.r
        multi_o_cnt = self.current_strat.get('m_orders_cnt',1)
        add_null_value = False
        if multi_o_cnt != 1 and r['buy_date'] > self.current_strat['last_edit_date']:
            try:
                r['order_size_in_settings'] = self.current_strat['order_size']
            except:
                pass
            if (r['sell_reason']=='Jsell' or r['sell_condition']=='JoinedSell'):
                try:
                    fact_spent_usd = r['buy_price'] * r['quantity']
                    r['joined_sell_k'] = round(fact_spent_usd / self.current_strat['order_size'],1)
                    self.calcMultiOrder(fact_spent_usd)                            
                    r['orders_in_net'] = self.fact_orders_in_net
                    r['total_order_nets'] = self.total_order_nets                    
                except:
                    add_null_value = True
        else:
            add_null_value = True
        if add_null_value:
            for col in ['joined_sell_k','orders_in_net','total_order_nets','order_size_in_settings']:
                if col not in r:
                    r[col] = None
        self.r = r

    def roundData(self):
        keys = ['profit']
        old = ['market24h','market1h','bvsv_current','btc24h','btc1h',
            'btc5m','btc1m','pump1h','dump1h','c24h','c3h','c1h','c15m','c5m','c1m']
        for k in keys:
            self.r[k] = round(self.r[k],2)
        keys = ['moonbot_buy_price','quantity','buy_price','sell_price']
        if self.r['buy_price'] is None:
            self.r['buy_price'] = self.r['moonbot_buy_price']        
        for k in keys:
            if self.r[k] is None:
                continue
            if self.r[k] < 0.01:
                points = 8
            elif self.r[k] < 1:
                points = 6
            else:
                points = 4          
            self.r[k] = round(self.r[k],points)

    def createDataSet(self,data,keys,columns):
        rows = []
        self.now = dtm.now()
        for d in data:
            self.r = dict(zip(columns,d))
            #if 'SellFromAssets' in self.r['SellReason']:
            #    continue #ручные продажи не учитываем
            if not self.addData():
                bot_order_id = self.r.get('bot_order_id',-55555)
                info = ''
                for col in ['strategy_id','base_coin','base_coin_id']:
                    val = self.r.get(col,'?None?')
                    info = f'{col}:{val},{info}'
                self.logger.debug(f'Cant send to server bad row {bot_order_id}. {info}')
                continue
            #self.addMultiorderData()
            self.roundData()
            q = ([self.r.get(k,None) for k in keys])
            rows.append(q)
        return rows

    def saveData(self,data,columns,market_type,logger,sleep):
        self.market_type = market_type
        self.logger = logger
        keys = ['user_id','bot_id','strategy_id','base_coin_id','site','market_type',
            'coin','profit_percent','sell_reason','sell_condition','add_date','ex_order_id','bot_order_id',
            'buy_date','close_date','quantity','buy_price','sell_price','profit','is_short','joined_sell_k',
            'orders_in_net','total_order_nets','order_size_in_settings','total_spent',
            'moonbot_buy_date','moonbot_buy_price','first_order_size','emulator','local_row_create_date',
            'orders_list','order_create_date'
            ]
        dataset = self.createDataSet(data,keys,columns)
        poz = 0
        saved = 0
        while poz < len(dataset):
            part = dataset[poz:poz+self.limit]
            saved += len(part)
            try:
                self.sendData(part,keys)
            except:
                #self.logger.debug(keys)
                path = r'c:\_saver\my_err.txt'
                with open(path, 'wb') as fp:
                    pickle.dump(part, fp)
                sys.exittt()
            self.logger.debug(f'Sended to bd: {len(part)}')
            poz += self.limit
            time.sleep(sleep)
        self.logger.debug(f'Total sended to bd: {saved}')
        #except sqlite3.Error as error:
        #print("Ошибка при работе с SQLite", error)
#%%
