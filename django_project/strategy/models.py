from django.db import models
from bot.models import User,Bot

class StrategyType(models.Model):
    name = models.CharField(max_length=100, verbose_name='Тип стратегии. Shot,Strike, тд.')

class Strategy(models.Model):
    name = models.CharField(max_length=100)
    bot = models.ForeignKey(Bot, on_delete=models.CASCADE, default=None, blank=True, null=True)
    last_edit_date = models.DateTimeField(blank=True, null=True)
    create_date = models.DateField(blank=True, null=True, verbose_name='Дата создания')
    user = models.ForeignKey(User, related_name='user_id',on_delete=models.CASCADE, default=None, blank=True, null=True)
    strategy_type = models.ForeignKey(StrategyType, on_delete=models.CASCADE, default=None, blank=True, null=True)
    market_type = models.CharField(max_length=20, verbose_name='Спот, Фьючи, Кварталки', default=None, blank=True, null=True)
    site = models.CharField(max_length=10, verbose_name='Биржа', blank=True, null=True)
    order_size = models.FloatField(blank=True, null=True)
    join_sell_key = models.CharField(max_length=10, blank=True, null=True)
    trigger_key = models.CharField(max_length=10, blank=True, null=True)
    trigger_by_key = models.CharField(max_length=10, blank=True, null=True)
    is_active = models.NullBooleanField(blank=True, null=True, default=None)
    settings = models.TextField(blank=True, null=True)
    m_orders_cnt = models.IntegerField(verbose_name='OrdersCount', blank=True, null=True)
    m_price_step = models.FloatField(verbose_name='BuyPriceStep', blank=True, null=True)
    m_o_size_step =models.IntegerField(verbose_name='OrderSizeStep', blank=True, null=True)    
    m_o_size_kind = models.CharField(max_length=20, verbose_name='OrderSizeKind.Linear,Exponential', default=None, blank=True, null=True)
    need_reload = models.NullBooleanField(blank=True, null=True, default=None)
    path = models.CharField(max_length=100, blank=True, null=True)

class StrategySettings(models.Model):
    name = models.CharField(max_length=100)
    sort_order = models.IntegerField(blank=True, null=True)