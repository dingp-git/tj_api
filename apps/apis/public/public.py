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
from typing import Optional
from apps.utils.mysql_conn_pool.mysql_helper import MySqLHelper

public = APIRouter()

# 当前时间  "2020-11-20 22:00:00"
NOW_DATE_TIME = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
# 当前时间减一天   "2020-11-19 22:00:00"
BEFORE_DATE_TIME = (datetime.datetime.now()+datetime.timedelta(days=-1)).strftime("%Y-%m-%d %H:%M:%S")

@public.get("/")
async def get_info():
    return comm_ret(data={'test': 'test'})

@public.get('/alarm/real_time', summary = "获取系统实时告警")
async def get_real_time_alarm(start_time:Optional[str]=BEFORE_DATE_TIME, end_time:Optional[str]=NOW_DATE_TIME):
    """
        ## **param**:
            start_time:    开始时间(可选参数)      str    默认  当前时间前一天
            end_time:      结束时间(可选参数)      str    默认  当前时间
        ## **return**:
            [
                {
                    "alarm_id": "1368738843720818688",
                    "alarm_location": "10.40.10.0",
                    "alarm_system": "ipsy",
                    "alarm_level": 1,
                    "alarm_type": 0,
                    "alarm_text": "告警文字",
                    "happen_times": 1,
                    "happen_d_time": "2021-03-08 09:42:24",
                    "end_d_time": "2021-03-08 09:42:24"
                },
                ...
            ]
    """
    db = MySqLHelper()
    sql = """SELECT
                alarm_id,
                alarm_location,
                alarm_system,
                alarm_level,
                alarm_type,
                alarm_text,
                happen_times,
                happen_d_time,
                end_d_time 
            FROM
                t_alarm 
            WHERE
                happen_d_time BETWEEN '{}' AND '{}'""".format(start_time, end_time)
    rows = db.selectall(sql=sql)
    data_list = [list(row) for row in rows]
    temp_data = data_processing(data_list, 2000)
    result = []
    for item in temp_data:
        temp_dict = {}
        temp_dict['alarm_id'] = item[0]
        temp_dict['alarm_location'] = item[1]
        temp_dict['alarm_system'] = item[2]
        temp_dict['alarm_level'] = item[3]
        temp_dict['alarm_type'] = item[4]
        temp_dict['alarm_text'] = item[5]
        temp_dict['happen_times'] = item[6]
        temp_dict['happen_d_time'] = item[7]
        temp_dict['end_d_time'] = item[8]
        result.append(temp_dict)
    return comm_ret(data = result)


@public.get('/alarm/history', summary = "获取系统历史告警")
async def get_history_alarm(start_time:Optional[str]=BEFORE_DATE_TIME, end_time:Optional[str]=NOW_DATE_TIME):
    """
        ## **param**:
            start_time:    开始时间(可选参数)      str    默认  当前时间前一天
            end_time:      结束时间(可选参数)      str    默认  当前时间
        ## **return**:
            [
                {
                    "alarm_id": "1368738843720818688",
                    "alarm_location": "10.40.10.0",
                    "alarm_system": "ipsy",
                    "alarm_level": 1,
                    "alarm_type": 0,
                    "alarm_text": "告警文字",
                    "happen_times": 1,
                    "happen_d_time": "2021-03-08 09:42:24",
                    "end_d_time": "2021-03-08 09:42:24"
                },
                ...
            ]
    """
    db = MySqLHelper()
    sql = """SELECT
                alarm_id,
                alarm_location,
                alarm_system,
                alarm_level,
                alarm_type,
                alarm_text,
                happen_times,
                happen_d_time,
                end_d_time 
            FROM
                t_alarm_history 
            WHERE
                happen_d_time BETWEEN '{}' AND '{}'""".format(start_time, end_time)
    rows = db.selectall(sql=sql)
    data_list = [list(row) for row in rows]
    temp_data = data_processing(data_list, 2000)
    result = []
    for item in temp_data:
        temp_dict = {}
        temp_dict['alarm_id'] = item[0]
        temp_dict['alarm_location'] = item[1]
        temp_dict['alarm_system'] = item[2]
        temp_dict['alarm_level'] = item[3]
        temp_dict['alarm_type'] = item[4]
        temp_dict['alarm_text'] = item[5]
        temp_dict['happen_times'] = item[6]
        temp_dict['happen_d_time'] = item[7]
        temp_dict['end_d_time'] = item[8]
        result.append(temp_dict)
    return comm_ret(data = result)



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




