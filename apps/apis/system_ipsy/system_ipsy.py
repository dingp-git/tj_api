# @Copyright(C), OldFive, 2020.
# @Date : 2021/3/11 0011 10:48:27
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
ip溯源系统相关接口
"""

# Standard library imports

# Third party imports
from fastapi import APIRouter, Depends
from starlette.status import *
# Local application imports
from apps.utils import resp_code
from apps.utils.comm_ret import comm_ret
from apps.apis.public.public import data_processing, NOW_DATE_TIME, BEFORE_DATE_TIME, NOW_DATE, BEFORE_DATE
from typing import Optional
from apps.utils.mysql_conn_pool.mysql_helper import MySqLHelper
system_ipsy = APIRouter()


@system_ipsy.get("/log/new", summary="最近一天  每家运营商上报日志条数")
async def get_log_new(start_time: Optional[str] = BEFORE_DATE, end_time: Optional[str] = NOW_DATE, 
                        index: Optional[str] = ''):
    """
        ## **param**:
            start_time:    开始时间(可选参数)    str    默认  当前时间前一天
            end_time:      结束时间(可选参数)    str    默认  当前时间
            index:         指标名称(可选参数)    str    默认  ''
        ## **return**:
            [
                {
                    "lt_gw": 2623574,
                    "d_time": "2021-04-24"
                },
                ...
            ]
    """
    db = MySqLHelper()
    if index:
        sql = """SELECT 
                    {},
                    d_time
                FROM
                    t_ipsy_log_nums 
                WHERE
                    d_time BETWEEN '{}' 
                    AND '{}'
                ORDER BY
                    d_time""".format(index, start_time, end_time)
    else:
        sql = """SELECT
                    lt_yw,
                    yd_yw,
                    dx_yw,
                    lt_gw,
                    yd_gw,
                    dx_gw,
                    d_time 
                FROM
                    t_ipsy_log_nums 
                WHERE
                    d_time BETWEEN '{}' 
                    AND '{}' 
                ORDER BY
                    d_time""".format(start_time, end_time)
    rows = db.selectall(sql=sql)
    data_list = [list(row) for row in rows]
    temp_data = data_processing(data_list, 2000)
    result = []
    for item in temp_data:
        temp_dict = {}
        if len(item) == 2:
            temp_dict[index] = item[0]
            temp_dict['d_time'] = item[1]
            result.append(temp_dict)
        else:
            temp_dict['lt_yw'] = item[0]
            temp_dict['yd_yw'] = item[1]
            temp_dict['dx_yw'] = item[2]
            temp_dict['lt_gw'] = item[3]
            temp_dict['yd_gw'] = item[4]
            temp_dict['dx_gw'] = item[5]
            temp_dict['d_time'] = item[6]
            result.append(temp_dict)
    return comm_ret(data = result)

@system_ipsy.get("/log/increment", summary="最近一天  每家运营商上报日志条数的增量")
async def get_log_increment(start_time: Optional[str] = BEFORE_DATE, end_time: Optional[str] = NOW_DATE, 
                        index: Optional[str] = ''):
    """
        ## **param**:
            start_time:    开始时间(可选参数)    str    默认  当前时间前一天
            end_time:      结束时间(可选参数)    str    默认  当前时间
            index:         指标名称(可选参数)    str    默认  ''
        ## **return**:
            [
                {
                    "lt_gw_diff": 2623574,
                    "d_time": "2021-04-24"
                },
                ...
            ]
    """
    db = MySqLHelper()
    if index:
        sql = """SELECT 
                    {},
                    d_time
                FROM
                    t_ipsy_log_increment 
                WHERE
                    d_time BETWEEN '{}' 
                    AND '{}'
                ORDER BY
                    d_time""".format(index, start_time, end_time)
    else:
        sql = """SELECT
                    lt_yw_diff,
                    yd_yw_diff,
                    dx_yw_diff,
                    lt_gw_diff,
                    yd_gw_diff,
                    dx_gw_diff,
                    d_time 
                FROM
                    t_ipsy_log_increment 
                WHERE
                    d_time BETWEEN '{}' 
                    AND '{}' 
                ORDER BY
                    d_time""".format(start_time, end_time)
    rows = db.selectall(sql=sql)
    data_list = [list(row) for row in rows]
    temp_data = data_processing(data_list, 2000)
    result = []
    for item in temp_data:
        temp_dict = {}
        if len(item) == 2:
            temp_dict[index] = item[0]
            temp_dict['d_time'] = item[1]
            result.append(temp_dict)
        else:
            temp_dict['lt_yw_diff'] = item[0]
            temp_dict['yd_yw_diff'] = item[1]
            temp_dict['dx_yw_diff'] = item[2]
            temp_dict['lt_gw_diff'] = item[3]
            temp_dict['yd_gw_diff'] = item[4]
            temp_dict['dx_gw_diff'] = item[5]
            temp_dict['d_time'] = item[6]
            result.append(temp_dict)
    return comm_ret(data = result)