from django.db import models
import copy

class FuturesData(models.Model):
    timestamp = models.IntegerField(blank=True, null=True)
    symbol = models.TextField(blank=True, null=True)
    price = models.FloatField(blank=True, null=True)
    high_price = models.FloatField(blank=True, null=True)
    low_price = models.FloatField(blank=True, null=True)
    quantity = models.FloatField(blank=True, null=True)    
    buy_quantity = models.FloatField(blank=True, null=True)  
    sell_quantity = models.FloatField(blank=True, null=True)  
    quote_quantity = models.FloatField(blank=True, null=True)      
    buy_trades_count = models.IntegerField(blank=True, null=True) 
    sell_trades_count = models.IntegerField(blank=True, null=True) 
    
    class Meta:
        db_table = 'binance_futures_market_data'
        """indexes = [
            models.Index(name='bfmd_symbol_idx', fields=['symbol']),
            models.Index(name='bfmd_timestamp_idx', fields=['timestamp']),
        ]"""

class FuturesMetricsJournal(models.Model):
    date = models.DateField(blank=True, null=True)
    seconds_ema = models.BooleanField(blank=True, default=False, \
        verbose_name='seconds ema,mavg')
    h_m_ema = models.BooleanField(blank=True, default=False, \
        verbose_name='minutes ema,mavg,min,max')
    class Meta:
        db_table = 'binance_futures_metrics_journal'

class FuturesParsedFiles(models.Model):
    filename = models.TextField(blank=True, null=True)
    class Meta:
        db_table = 'binance_futures_market_data_parsed_files'

def getDurationTxt(duration,time_shift):
    if duration < 1:
        return f'{int(duration*60)}m'
    if 'd' in time_shift:
        return f'{duration}d'
    return f'{duration}h'

class FuturesMetricsSecondsEMA(models.Model):
    date = models.DateTimeField(blank=True, null=True)
    coin = models.TextField(blank=True, null=True)
    #periods_s = [1,3,5,7,10,15,20,25,30,40,60,90,120,150,180,240,300]
    periods_s = [1,3,10,30,60,90,180,300]
    for i in periods_s:
        exec(f"ema{i}s = models.FloatField(blank=True, null=True)")
    ema2m = models.FloatField(blank=True, null=True)
    ema3m = models.FloatField(blank=True, null=True)
    ema4m = models.FloatField(blank=True, null=True)
    ema5m = models.FloatField(blank=True, null=True)
    ema6m = models.FloatField(blank=True, null=True)
    delta_vol3h = models.FloatField(blank=True, null=True)
    vltlt1m = models.FloatField(blank=True, null=True)
    vltlt5m = models.FloatField(blank=True, null=True)
    periods_s = [30,90,300]
    for i in periods_s:
        exec(f"bvsv{i}s = models.FloatField(blank=True, null=True)")

    class Meta:
        db_table = 'binance_futures_metrics_seconds_ema'
        indexes = [
            
            models.Index(name='bfmse_date_idx', fields=['date']),
        ]#models.Index(name='bfmse_coin_idx', fields=['coin']),

class FuturesMetricsMinutesEMA(models.Model):
    date = models.DateTimeField(blank=True, null=True)
    coin = models.TextField(blank=True, null=True)
    periods_m = [5,10,15,20,30,45,60,75,90]
    for i in periods_m:
        for name in ['max','min','ema']:#
            if name == 'ema' and i <= 6:
                continue
            exec(f"{name}{i}m = models.FloatField(blank=True, null=True)")
    ema_periods_h = [1,2,3,4,5,6,9,12,15,18,24,30,36]
    for i in ema_periods_h:
        for name in ['ema']:#'max','min',
            exec(f"{name}{i}h = models.FloatField(blank=True, null=True)")
    ema0h = models.FloatField(blank=True, null=True)
    ema_periods_d = [1,2,3,4,5,6,7]
    for i in ema_periods_d:
        for name in ['ema']:#'max','min',
            exec(f"{name}{i}d = models.FloatField(blank=True, null=True)")
    ema0d = models.FloatField(blank=True, null=True)
    metrics = ['vltlt','cndl_size','cndl_size_med','peaks_cnt','bvsv','vol',
        'trade_mean','trade_cnt']
    periods_m = [5,15,30]
    for i in periods_m:
        for name in metrics:
            exec(f"{name}{i}m = models.FloatField(blank=True, null=True)")
    periods_h = [1,2,3,6,9,15,24,36]
    for i in periods_h:
        for name in metrics:
            exec(f"{name}{i}h = models.FloatField(blank=True, null=True)")
    #shifted metrics
    metrics.extend(['max','min'])
    base_shift_sets = {'15m':[0.25,0.5,1,2,3],'1h':[1,2,3],'2h':[1,2,3,6,9,15],
        '6h':[3,6,9,15,24,36],'12h':[6,9,15,24,36]}
    max_min_shift_set = {'1h':ema_periods_h,'1d':ema_periods_d}
    for name in metrics:
        shift_sets = copy.deepcopy(base_shift_sets)
        if name in ['max','min']:
            shift_sets.update(max_min_shift_set)
        for time_shift,dur_list in shift_sets.items():
            for duration in dur_list:
                duration_txt = getDurationTxt(duration,time_shift)
                main_col = f'{name}{duration_txt}'
                col_name = f'{main_col}_{time_shift}'
                exec(f"{col_name} = models.FloatField(blank=True, null=True)")
    manual_cols = ['vltlt4h','pump5m','pump1h','dump1h','value1h','value24h','proportions']
    for col_name in manual_cols:
        exec(f"{col_name} = models.FloatField(blank=True, null=True)")

    class Meta:
        db_table = 'binance_futures_metrics_minutes_ema'
        indexes = [
            models.Index(name='bfmme_coin_idx', fields=['coin']),
            models.Index(name='bfmme_date_idx', fields=['date']),
        ]

class FuturesCandles(models.Model):
    symbol = models.TextField(blank=True, null=True)
    open_time = models.IntegerField(blank=True, null=True)
    close_time = models.IntegerField(blank=True, null=True)
    open_price = models.FloatField(blank=True, null=True)
    close_price = models.FloatField(blank=True, null=True)
    high_price = models.FloatField(blank=True, null=True)
    low_price = models.FloatField(blank=True, null=True)
    quantity = models.FloatField(blank=True, null=True)    
    buy_quantity = models.FloatField(blank=True, null=True)  
    sell_quantity = models.FloatField(blank=True, null=True)  
    quote_quantity = models.FloatField(blank=True, null=True)      
    buy_trades_count = models.IntegerField(blank=True, null=True) 
    sell_trades_count = models.IntegerField(blank=True, null=True) 
    
    class Meta:
        db_table = 'binance_futures_market_data_candles_1m'
        """indexes = [
            models.Index(name='bfmd_candles_1m_symbol_idx', fields=['symbol']),
            models.Index(name='bfmd_candles_1m_open_time_idx', fields=['open_time']),
        ]"""

class SpotData(models.Model):
    timestamp = models.IntegerField(blank=True, null=True)
    symbol = models.TextField(blank=True, null=True)
    price = models.FloatField(blank=True, null=True)
    high_price = models.FloatField(blank=True, null=True)
    low_price = models.FloatField(blank=True, null=True)
    quantity = models.FloatField(blank=True, null=True)    
    buy_quantity = models.FloatField(blank=True, null=True)  
    sell_quantity = models.FloatField(blank=True, null=True)  
    quote_quantity = models.FloatField(blank=True, null=True)      
    buy_trades_count = models.IntegerField(blank=True, null=True) 
    sell_trades_count = models.IntegerField(blank=True, null=True) 
    
    class Meta:
        db_table = 'binance_spot_market_data'
        """indexes = [
            models.Index(name='bsmd_symbol_idx', fields=['symbol']),
            models.Index(name='bsmd_timestamp_idx', fields=['timestamp']),
        ]"""
        
class SpotCandles(models.Model):
    symbol = models.TextField(blank=True, null=True)
    open_time = models.IntegerField(blank=True, null=True)
    close_time = models.IntegerField(blank=True, null=True)
    open_price = models.FloatField(blank=True, null=True)
    close_price = models.FloatField(blank=True, null=True)
    high_price = models.FloatField(blank=True, null=True)
    low_price = models.FloatField(blank=True, null=True)
    quantity = models.FloatField(blank=True, null=True)    
    buy_quantity = models.FloatField(blank=True, null=True)  
    sell_quantity = models.FloatField(blank=True, null=True)  
    quote_quantity = models.FloatField(blank=True, null=True)      
    buy_trades_count = models.IntegerField(blank=True, null=True) 
    sell_trades_count = models.IntegerField(blank=True, null=True) 
    
    class Meta:
        db_table = 'binance_spot_market_data_candles_1m'
        """indexes = [
            models.Index(name='bsmd_candles_1m_symbol_idx', fields=['symbol']),
            models.Index(name='bsmd_candles_1m_open_time_idx', fields=['open_time']),
        ]"""
# %%
class SpotParsedFiles(models.Model):
    filename = models.TextField(blank=True, null=True)
    class Meta:
        db_table = 'binance_spot_market_data_parsed_files'
        
class SpotMetricsJournal(models.Model):
    date = models.DateField(blank=True, null=True)
    seconds_ema = models.BooleanField(blank=True, default=False, \
        verbose_name='seconds ema,mavg')
    h_m_ema = models.BooleanField(blank=True, default=False, \
        verbose_name='minutes ema,mavg,min,max')
    class Meta:
        db_table = 'binance_spot_metrics_journal'

class SpotMetricsSecondsEMA(models.Model):
    date = models.DateTimeField(blank=True, null=True)
    coin = models.TextField(blank=True, null=True)
    #periods_s = [1,3,5,7,10,15,20,25,30,40,60,90,120,150,180,240,300]
    periods_s = [1,3,10,30,60,90,180,300]
    for i in periods_s:
        exec(f"ema{i}s = models.FloatField(blank=True, null=True)")
    ema2m = models.FloatField(blank=True, null=True)
    ema3m = models.FloatField(blank=True, null=True)
    ema4m = models.FloatField(blank=True, null=True)
    ema5m = models.FloatField(blank=True, null=True)
    ema6m = models.FloatField(blank=True, null=True)
    delta_vol3h = models.FloatField(blank=True, null=True)
    vltlt1m = models.FloatField(blank=True, null=True)
    vltlt5m = models.FloatField(blank=True, null=True)
    periods_s = [30,90,300]
    for i in periods_s:
        exec(f"bvsv{i}s = models.FloatField(blank=True, null=True)")
    class Meta:
        db_table = 'binance_spot_metrics_seconds_ema'
        indexes = [
            models.Index(name='bsmse_coin_idx', fields=['coin']),
            models.Index(name='bsmse_date_idx', fields=['date']),
        ]


class SpotMetricsMinutesEMA(models.Model):
    date = models.DateTimeField(blank=True, null=True)
    coin = models.TextField(blank=True, null=True)
    periods_m = [5,10,15,20,30,45,60,75,90]
    for i in periods_m:
        for name in ['max','min','ema']:#
            if name == 'ema' and i <= 6:
                continue
            exec(f"{name}{i}m = models.FloatField(blank=True, null=True)")
    ema_periods_h = [1,2,3,4,5,6,9,12,15,18,24,30,36]
    for i in ema_periods_h:
        for name in ['ema']:#'max','min',
            exec(f"{name}{i}h = models.FloatField(blank=True, null=True)")
    ema0h = models.FloatField(blank=True, null=True)
    ema_periods_d = [1,2,3,4,5,6,7]
    for i in ema_periods_d:
        for name in ['ema']:#'max','min',
            exec(f"{name}{i}d = models.FloatField(blank=True, null=True)")
    ema0d = models.FloatField(blank=True, null=True)
    metrics = ['vltlt','cndl_size','cndl_size_med','peaks_cnt','bvsv','vol',
        'trade_mean','trade_cnt']
    periods_m = [5,15,30]
    for i in periods_m:
        for name in metrics:
            exec(f"{name}{i}m = models.FloatField(blank=True, null=True)")
    periods_h = [1,2,3,6,9,15,24,36]
    for i in periods_h:
        for name in metrics:
            exec(f"{name}{i}h = models.FloatField(blank=True, null=True)")
    #shifted metrics
    metrics.extend(['max','min'])
    base_shift_sets = {'15m':[0.25,0.5,1,2,3],'1h':[1,2,3],'2h':[1,2,3,6,9,15],
        '6h':[3,6,9,15,24,36],'12h':[6,9,15,24,36]}
    max_min_shift_set = {'1h':ema_periods_h,'1d':ema_periods_d}
    for name in metrics:
        shift_sets = copy.deepcopy(base_shift_sets)
        if name in ['max','min']:
            shift_sets.update(max_min_shift_set)
        for time_shift,dur_list in shift_sets.items():
            for duration in dur_list:
                duration_txt = getDurationTxt(duration,time_shift)
                main_col = f'{name}{duration_txt}'
                col_name = f'{main_col}_{time_shift}'
                exec(f"{col_name} = models.FloatField(blank=True, null=True)")
    manual_cols = ['vltlt4h','pump5m','pump1h','dump1h','value1h','value24h','proportions']
    for col_name in manual_cols:
        exec(f"{col_name} = models.FloatField(blank=True, null=True)")

    class Meta:
        db_table = 'binance_spot_metrics_minutes_ema'
        indexes = [
            models.Index(name='bsmme_coin_idx', fields=['coin']),
            models.Index(name='bsmme_date_idx', fields=['date']),
        ]
