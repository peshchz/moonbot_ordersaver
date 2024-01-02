from django.db import models
from bot.models import User,Bot,BaseCoin
from strategy.models import StrategyType,Strategy
from datetime import datetime

class Orders(models.Model):
    user = models.ForeignKey(User, on_delete=models.CASCADE, default=None, blank=True, null=True)
    bot = models.ForeignKey(Bot, on_delete=models.CASCADE, default=None, blank=True, null=True)
    base_coin = models.ForeignKey(BaseCoin, on_delete=models.CASCADE)
    coin = models.CharField(max_length=30, verbose_name='Монета')
    strategy = models.ForeignKey(Strategy, on_delete=models.CASCADE, default=None, blank=True, null=True)
    site = models.CharField(max_length=10, verbose_name='Биржа')
    order_create_date = models.DateTimeField(blank=True, null=True, verbose_name='Дата создания ордера')
    buy_date = models.DateTimeField(blank=True, null=True, verbose_name='Дата покупки')
    moonbot_buy_date = models.DateTimeField(blank=True, null=True, verbose_name='Дата покупки')
    close_date = models.DateTimeField(blank=True, null=True, verbose_name='Дата продажи')
    quantity = models.FloatField(verbose_name='Количество')
    buy_price = models.FloatField()
    moonbot_buy_price = models.FloatField(blank=True, null=True)
    sell_price = models.FloatField()
    profit = models.FloatField(verbose_name='Прибыль в долларах')
    profit_percent = models.FloatField()
    total_spent = models.FloatField(blank=True, null=True)
    first_order_size = models.FloatField(verbose_name='Размер ордера из логово', default=None, blank=True, null=True)
    order_size_in_settings = models.FloatField(verbose_name='Размер ордера в настройках. для мультиордеров', default=None, blank=True, null=True)
    sell_reason = models.CharField(max_length=30, verbose_name='Причина продажи')
    sell_condition = models.CharField(max_length=30, verbose_name='Условия продажи', default=None, blank=True, null=True)
    joined_sell_k = models.FloatField(verbose_name='Коэффициент joined sell', default=None, blank=True, null=True)
    orders_in_net = models.IntegerField(verbose_name='Куплено ордеров в мультисетке', blank=True, null=True)
    total_order_nets = models.IntegerField(verbose_name='Выставлено сеток с ордерами', blank=True, null=True)
    is_short = models.BooleanField(default=0, blank=True)
    market_type = models.CharField(max_length=20, verbose_name='Спот, Фьючи, Кварталки', default=None, blank=True, null=True)
    ex_order_id = models.CharField(max_length=30, verbose_name='ID ордера на бирже',blank=True, null=True)
    bot_order_id = models.IntegerField(blank=True, null=True)
    add_date = models.DateTimeField(verbose_name='Дата добавления записи', blank=True, null=True)
    local_row_create_date = models.DateTimeField(verbose_name='Дата создания записи в локальной БД', blank=True, null=True)
    emulator = models.NullBooleanField(default=0, null=True)
    orders_list = models.TextField(verbose_name='Список ордеров в сетке', default=None, blank=True, null=True)

    market24h = models.FloatField(blank=True, null=True)
    market1h = models.FloatField(blank=True, null=True)
    bvsv_current = models.FloatField(verbose_name='bvsv для текущих настроек страты',blank=True, null=True)
    btc24h = models.FloatField(blank=True, null=True)
    btc1h = models.FloatField(blank=True, null=True)
    btc5m = models.FloatField(blank=True, null=True)
    btc1m = models.FloatField(blank=True, null=True)
    pump1h = models.FloatField(blank=True, null=True)
    dump1h = models.FloatField(blank=True, null=True)
    c24h = models.FloatField(blank=True, null=True)
    c3h = models.FloatField(blank=True, null=True)
    c1h = models.FloatField(blank=True, null=True)
    c15m = models.FloatField(blank=True, null=True)
    c5m = models.FloatField(blank=True, null=True)
    c1m = models.FloatField(blank=True, null=True)

    class Meta:
        indexes = [
            models.Index(fields=['user','market_type','base_coin','buy_date']),
            models.Index(fields=['user','market_type']),
            models.Index(fields=['user']),
        ]

class OrderTask(models.Model):
    strategy = models.ForeignKey(Strategy, on_delete=models.CASCADE)
    quantity = models.IntegerField()
    add_date = models.DateTimeField(blank=True, null=True)
    user = models.ForeignKey(User, on_delete=models.CASCADE, default=None, blank=True, null=True)