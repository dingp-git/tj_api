# @Copyright(C), OldFive, 2020.
# @Date : 2021/3/11 0011 10:48:37
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
公共接口
"""

# Standard library imports

# Third party imports
from fastapi import APIRouter, Depends
from starlette.status import *
# Local application imports
from apps.utils import resp_code
from apps.utils.comm_ret import comm_ret
import math
import datetime

public = APIRouter()

# 当前时间  "2020-11-20 22:00:00"
NOW_DATE_TIME = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
# 当前时间减一天   "2020-11-19 22:00:00"
BEFORE_DATE_TIME = (datetime.datetime.now()+datetime.timedelta(days=-1)).strftime("%Y-%m-%d %H:%M:%S")

@public.get("/")
async def get_info():
    return comm_ret(data={'test': 'test'})


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

# ret = data_processing(data_list,30)
# print(ret)




