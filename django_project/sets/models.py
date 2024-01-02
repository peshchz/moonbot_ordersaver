from django.db import models
from strategy.models import StrategyType,Strategy
from bot.models import User,Bot

class Sets(models.Model):
    name = models.CharField(max_length=100)
    bot = models.ForeignKey(Bot, on_delete=models.CASCADE, default=None, blank=True, null=True)
    user_id = models.ForeignKey(User, related_name='set_user_id',on_delete=models.CASCADE, default=None, blank=True, null=True)
    manual_switch = models.BooleanField(blank=True, default=True, verbose_name='Ручное вкл/выкл')
    current_manual_status = models.BooleanField(blank=True, default=True, verbose_name='Текущее состояние бота')
    manual_change_date = models.DateTimeField(blank=True, null=True, verbose_name='Дата смены статуса руками')
    last_connection_date = models.DateTimeField(blank=True, null=True, verbose_name='Дата последней отправки команд боту')
    last_breaking_date = models.DateTimeField(blank=True, null=True, verbose_name='Дата последнего паник селла')
    telegram_token = models.CharField(max_length=100, verbose_name='Токен телеграма', blank=True, null=True)
    telegram_chat_id = models.CharField(max_length=50, verbose_name='ID группы в телеге', blank=True, null=True)
    order_size = models.IntegerField()
    turbo_order_size1 = models.IntegerField()
    turbo_order_size2 = models.IntegerField()
    top_strats_cnt = models.IntegerField(verbose_name='Количество страт в Группе сетов')
    top_strats_calc_interval = models.CharField(max_length=50, \
        verbose_name='Интервал для расчета ТОП стратегий')
    top_coins_calc_interval = models.CharField(max_length=50, \
        verbose_name='Интервал для расчета ТОП монет')
    top_strats_update_freq = models.IntegerField(verbose_name='Частота пересчета ТОП. Через каждый Х запусков', default=1)
    top_strats_min_profit1 = models.FloatField(
        verbose_name='Минимальный профит1 для суммы по всем ТОП стратам', default=0)
    min_profit_percent1 = models.FloatField(
        verbose_name='Минимальный профит1 для страты. Для BestStrats', default=0)
    profit_interval1 = models.CharField(max_length=50, \
        verbose_name='Интервал для расчета Профита1 для топа и страт', blank=True, null=True)
    top_strats_min_profit2 = models.FloatField(
        verbose_name='Минимальный профит2 для суммы по всем ТОП стратам', default=0)
    min_profit_percent2 = models.FloatField(
        verbose_name='Минимальный профит2 для страты. Для BestStrats', default=0)
    profit_interval2 = models.CharField(max_length=50, \
        verbose_name='Интервал для расчета Профита2', blank=True, null=True)
    launch_cnt = models.IntegerField(verbose_name='Количество запусков скрипта', default=0)
    group = models.IntegerField(verbose_name='Группа сетов', blank=True, null=True, default=None)
    root_set = models.BooleanField(blank=True, default=False, verbose_name='Главный сет в группе. С него настройки берутся')
    full_top_strats_list = models.TextField(verbose_name='Список всех топ страт', blank=True, null=True)
    active_top_strats_list = models.TextField(verbose_name='Список активных страт. Справочно', blank=True, null=True)
    all_set_strats_list = models.TextField(verbose_name='Список всех доступных страт', blank=True, null=True)
    coins_list = models.TextField(verbose_name='Белый и Черный список монет', blank=True, null=True)
    max_coins_quantity = models.IntegerField()
    strategy_types = models.TextField(verbose_name='Типы стратегий', blank=True, null=True)
    top_pr_factor1 = models.FloatField(
        verbose_name='Минимальный профит фактор1 по ТОПу', default=0)
    top_pr_factor2 = models.FloatField(
        verbose_name='Минимальный профит фактор2 по ТОПу', default=0)
    min_pr_factor1 = models.FloatField(
        verbose_name='Минимальный профит фактор1 по страте', default=0)
    min_pr_factor2 = models.FloatField(
        verbose_name='Минимальный профит фактор2 по страте', default=0)
    top_loss_avg1 = models.FloatField(
        verbose_name='Мин. средний убыточный ордер1 по ТОПу', default=-100)
    top_loss_avg2 = models.FloatField(
        verbose_name='Мин. средний убыточный ордер2 по ТОПу', default=-100)
    min_loss_avg1 = models.FloatField(
        verbose_name='Мин. средний убыточный ордер1 по страте', default=-100)
    min_loss_avg2 = models.FloatField(
        verbose_name='Мин. средний убыточный ордер2 по страте', default=-100)
    disp_avg_loss_top1 = models.FloatField(
        verbose_name='Мин. средняя дисперсия по отриц ордерам1 по ТОПу', default=-100)
    bl_max_percent = models.IntegerField()
    break_hours = models.CharField(max_length=20, verbose_name='Часы запуска паник села. В 00 мин', blank=True, null=True)
    #Топ2 и по стратам не стал добавлять, т.к. не работает

class StratsInSet(models.Model):
    strat_set = models.ForeignKey(Sets, on_delete=models.CASCADE, default=None, blank=True, null=True)
    strategy = models.ForeignKey(Strategy, on_delete=models.CASCADE, default=None, blank=True, null=True)
    strat_set_name = models.CharField(max_length=100)
    settings = models.TextField()
    manual_switch = models.BooleanField(blank=True, default=True, verbose_name='Ручное вкл/выкл')
    current_manual_status = models.BooleanField(blank=True, default=True, verbose_name='Текущее состояние работы')
    current_order_size = models.FloatField(verbose_name='Установленный размер ордера',blank=True, null=True)
    manual_change_date = models.DateTimeField(blank=True, null=True, verbose_name='Дата смены статуса руками')
    current_active_status = models.BooleanField(blank=True, default=False)
    auto_change_date = models.DateTimeField(blank=True, null=True, verbose_name='Дата авто смены статуса')
    penalty_coins = models.TextField()
    reason = models.TextField()
    in_top = models.BooleanField(blank=True, default=False, verbose_name='Находится в ТОП')
    trigger_key = models.IntegerField(default=0)
    trigger_by_key = models.IntegerField(default=0)