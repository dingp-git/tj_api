# @Copyright(C), OldFive, 2020.
# @Date : 2021/3/11 10:04:21
# @Author : OldFive
# @Version : 0.1
# @Description : 
# @History :
# @Other:
#  ▒█████   ██▓    ▓█████▄   █████▒██▓ ██▒   █▓▓█████
# ▒██▒  ██▒▓██▒    ▒██▀ ██▌▓██   ▒▓██▒▓██░   █▒▓█   ▀
# ▒██░  ██▒▒██░    ░██   █▌▒████ ░▒██▒ ▓██  █▒░▒███
# ▒██   ██░▒██░    ░▓█▄   ▌░▓█▒  ░░██░  ▒██ █░░▒▓█  ▄
# ░ ████▓▒░░██████▒░▒████▓ ░▒█░   ░██░   ▒▀█░  ░▒████▒
# ░ ▒░▒░▒░ ░ ▒░▓  ░ ▒▒▓  ▒  ▒ ░   ░▓     ░ ▐░  ░░ ▒░ ░
#   ░ ▒ ▒░ ░ ░ ▒  ░ ░ ▒  ▒  ░      ▒ ░   ░ ░░   ░ ░  ░
# ░ ░ ░ ▒    ░ ░    ░ ░  ░  ░ ░    ▒ ░     ░░     ░
#     ░ ░      ░  ░   ░            ░        ░     ░  ░
#                   ░                      ░
#
"""
各种小工具集合
"""

# Standard library imports
import decimal
import datetime
import math
from dateutil.relativedelta import relativedelta
# Third party imports
from loguru import logger


# Local application imports

def get_now_date_time():
    """
        获取当前时间  "2020-11-20 22:00:00"
    """
    now_date_time= datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return now_date_time

def get_before_date_time():
    """
        获取 当前时间前一天   "2020-11-19 22:00:00"
    """
    before_date_time = (datetime.datetime.now()+datetime.timedelta(days=-1)).strftime("%Y-%m-%d %H:%M:%S")
    return before_date_time

def get_now_date():
    """
        获取当前日期  "2020-11-20"
    """
    now_date = datetime.datetime.now().strftime("%Y-%m-%d")
    return now_date

def get_before_date():
    """
        获取当前日期前一天  "2020-11-19"
    """
    before_date = (datetime.datetime.now()+datetime.timedelta(days=-1)).strftime("%Y-%m-%d")
    return before_date

def get_before_month():
    """
        获取当前日期前一月  "2020-10-20"
    """
    before_month = datetime.date.today()-relativedelta(months=+1)
    return before_month

def data_processing(data_list,max):
    """
        @param:
            data_list:  待处理的数据列表  list
            max:        最大数据量        int
        @return:
            数据列表
    """
    new_list = []
    length = len(data_list)
    if length > max:
        num = math.ceil(length/max)
        for i in range(0,length,num):
            new_list.append(data_list[i])
    else:
        new_list = data_list
    return new_list

def round_up_x(val, x=2):
    """数值取有效小数位(使用 round 函数) [解决部分小数遇 5 不进的情况]
    @params:
        val : 需要取精确值的数据
        x   : 需要保留的有效的小数位个数, 默认 2 位
    @return:
        返回 float 类型数据
    """
    return round(val * (10 ** x)) / (10.0 ** x)

def decimal_up_x(val, x=2):
    """数值取有效小数位(使用 decimal 模块, 速度慢) [解决部分小数遇 5 不进的情况]
    @params:
        val : 需要取精确值的数据 decimal.Decimal
        x   : 需要保留的有效的小数位个数, 默认 2 位
    @return:
        返回 decimal.Decimal 类型数据
    """
    #                     注意此处使用 str
    #                     decimal.Decimal(float) 与 decimal.Decimal(str) 有差异
    return decimal.Decimal(str(val)).quantize(decimal.Decimal(('0.' + "0" * x)),
                                                rounding=decimal.ROUND_HALF_UP)

def print_logo():
    """打印logo"""
    logger.info(' ▒█████   ██▓    ▓█████▄   █████▒██▓ ██▒   █▓▓█████')
    logger.info('▒██▒  ██▒▓██▒    ▒██▀ ██▌▓██   ▒▓██▒▓██░   █▒▓█   ▀')
    logger.info('▒██░  ██▒▒██░    ░██   █▌▒████ ░▒██▒ ▓██  █▒░▒███')
    logger.info('▒██   ██░▒██░    ░▓█▄   ▌░▓█▒  ░░██░  ▒██ █░░▒▓█  ▄')
    logger.info('░ ████▓▒░░██████▒░▒████▓ ░▒█░   ░██░   ▒▀█░  ░▒████▒')
    logger.info('░ ▒░▒░▒░ ░ ▒░▓  ░ ▒▒▓  ▒  ▒ ░   ░▓     ░ ▐░  ░░ ▒░ ░')
    logger.info('  ░ ▒ ▒░ ░ ░ ▒  ░ ░ ▒  ▒  ░      ▒ ░   ░ ░░   ░ ░  ░')
    logger.info('░ ░ ░ ▒    ░ ░    ░ ░  ░  ░ ░    ▒ ░     ░░     ░')
    logger.info('    ░ ░      ░  ░   ░            ░        ░     ░  ░')
    logger.info('                  ░                      ░')
