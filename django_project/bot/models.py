from django.db import models

class User(models.Model):
    name = models.CharField(max_length=100, verbose_name='Имя')
    telegram_id = models.IntegerField(verbose_name='id в Телеграме', default=None, blank=True, null=True)

class Account(models.Model):
    name = models.CharField(max_length=100, verbose_name='Имя')

class Bot(models.Model):
    name = models.CharField(max_length=100, verbose_name='Имя')
    user = models.ForeignKey(User, on_delete=models.CASCADE, default=None, blank=True, null=True)
    account = models.ForeignKey(Account, on_delete=models.CASCADE, default=None, blank=True, null=True)
    ip = models.GenericIPAddressField(default=None, blank=True, null=True)
    correct_timezone_date = models.DateTimeField(blank=True, null=True, \
        verbose_name='Дата выставления корректного час пояса')
    saver_version = models.IntegerField(default=None, blank=True, null=True)
    need_reload_all_data = models.BooleanField(default=False, blank=True)
    need_reload_txt_log = models.BooleanField(default=False, blank=True)
    last_work_date = models.DateTimeField(blank=True, null=True, 
        verbose_name='Время последнего запуска')
    last_reload_date = models.DateTimeField(blank=True, null=True, 
        verbose_name='Время последней перегрузки')
    is_main_create_settings_bot = models.BooleanField(default=False, blank=True, null=True)
    need_check_work_status = models.BooleanField(default=True, blank=True, null=True)
    telegram_token = models.CharField(max_length=100, verbose_name='Токен телеграма', blank=True, null=True)
    telegram_chat_id = models.CharField(max_length=50, verbose_name='ID группы в телеге', blank=True, null=True)

class BaseCoin(models.Model):
    base_coin_moon_id = models.IntegerField(verbose_name='ID монеты в БД муна')
    name = models.CharField(max_length=30, verbose_name='Базовая Монета', default=None, blank=True, null=True)

class OrderSaver(models.Model):
    param = models.CharField(max_length=100, verbose_name='param')
    value = models.CharField(max_length=100, verbose_name='value')

class Overload(models.Model):
    bot = models.ForeignKey(Bot, on_delete=models.CASCADE, default=None, blank=True, null=True)
    date = models.DateTimeField(blank=True, null=True)
    cpu = models.IntegerField(default=None, blank=True, null=True)
    request = models.IntegerField(default=None, blank=True, null=True)
    orders_1m = models.IntegerField(default=None, blank=True, null=True)
    orders_10s = models.IntegerField(default=None, blank=True, null=True)

class Bans(models.Model):
    bot = models.ForeignKey(Bot, on_delete=models.CASCADE, default=None, blank=True, null=True)
    user_id = models.ForeignKey(User, on_delete=models.CASCADE, default=None, blank=True, null=True)
    date = models.DateTimeField(blank=True, null=True)
    moon = models.IntegerField(default=None, blank=True, null=True)
    binance = models.IntegerField(default=None, blank=True, null=True)

class Balance(models.Model):
    account = models.CharField(max_length=10)
    date = models.DateTimeField(blank=True, null=True)
    balance = models.IntegerField(default=None, blank=True, null=True)
    available = models.IntegerField(default=None, blank=True, null=True)
    
    class Meta:
        indexes = [
            models.Index(fields=['date']),
        ]
