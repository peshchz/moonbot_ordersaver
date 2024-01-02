#%%
from glob import glob
import os
import os.path
from datetime import datetime as dtm
# %%
class ParseStratFile():
    def __init__(self,path):
        self.path = path
        self.getFilesFromPath()
        self.readFile()
        self.replaceTxt()
    
    def readFile(self):
        with open(self.filename, encoding='utf8') as f:
            self.txt = f.read()

    def replaceTxt(self):
        rep_list = ['# Moon Bot Strategies','##Begin_Strategy']
        for rep in rep_list:
            self.txt = self.txt.replace(rep,'')
    
    def parceStrats(self):
        strats = self.txt.split('##End_Strategy')
        self.parsed_strats = []
        folder = ''
        for strat in strats:
            strat_settings = {}
            values = list(filter(None, strat.split('\n')))
            if len(values) < 10:
                continue
            for index,val in enumerate(values):
                if val.find('#End_Folder') > -1:
                    folder = ''
                    continue
                if val.find('#Begin_Folder') > -1:
                    folder = val.replace('#Begin_Folder ','')
                    continue
                poz = val.find('=')
                name = val[:poz].strip()
                v = val[poz+1:]
                strat_settings[name] = {'value':v,'sort_order':index}
            strat_settings['folder'] = {'value':folder,'sort_order':-1}
            #strat_settings['LastEditDate']['value'] = \
            #    dtm.strptime(strat_settings['LastEditDate']['value'], '%Y-%m-%d %H:%M')            
            self.parsed_strats.append(strat_settings)
            #strat_json = json.dumps(strat_settings)
            #json.loads(strat_json)
    
    def getFilesFromPath(self):
        '''получим список свежих файлов с каталога'''
        file_mask = '{}**/*strat.txt'.format(self.path)
        all_files = glob(file_mask, recursive=True)
        last_date = dtm(1970,1,1)
        for f in all_files:
            file_date = dtm.fromtimestamp(os.path.getmtime(f))
            if file_date > last_date:
                last_date = file_date
                self.filename = f

