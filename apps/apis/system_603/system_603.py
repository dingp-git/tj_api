# @Copyright(C), OldFive, 2020.
# @Date : 2021/3/11 0011 10:48:20
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
603系统相关接口
"""

# Standard library imports

# Third party imports
from fastapi import APIRouter, Depends
from starlette.status import *
# Local application imports
from apps.utils import resp_code
from apps.utils.comm_ret import comm_ret
from apps.utils.mysql_conn_pool.mysql_helper import MySqLHelper
import datetime,time
from loguru import logger
from apps.apis.public.public import data_processing,NOW_DATE_TIME,BEFORE_DATE_TIME
from typing import Optional

system_603 = APIRouter()

# 短信接收数据量
@system_603.get("/sms/rcv")
async def get_sms_rcv(start_time:Optional[str] = BEFORE_DATE_TIME, end_time: Optional[str] = NOW_DATE_TIME):
    """
        @param:
            start_time:    开始时间(可选参数)    str    默认  当前时间前一天
            end_time:      结束时间(可选参数)    str    默认  当前时间
        @return:
            [
                {
                "date": "2021-03-01 14:57:00",
                "value": 75775
                },
                ...
            ]
    """
    sql = """SELECT
                d_time,
                SUM( sjjs_1m ) 
            FROM
                t_603_sms_sjjs 
            WHERE
                d_time BETWEEN '{}' 
                AND '{}' 
            GROUP BY
                d_time 
            ORDER BY
                d_time """.format(start_time, end_time)
    print(sql)
    result = get_msg_data(sql)
    return comm_ret(data=result)

# 短信加载数据量
@system_603.get("/sms/load")
async def get_sms_load(start_time:Optional[str] = BEFORE_DATE_TIME, end_time:Optional[str] = NOW_DATE_TIME):
    """
        @param:
            start_time:    开始时间(可选参数)    str    默认  当前时间前一天
            end_time:      结束时间(可选参数)    str    默认  当前时间
        @return:
            [
                {
                "date": "2021-03-01 14:57:00",
                "value": 75775
                },
                ...
            ]
    """
    sql = """SELECT
                d_time,
                SUM( load_1m ) 
            FROM
                t_603_sms_load 
            WHERE
                d_time BETWEEN '{}' 
                AND '{}' 
            GROUP BY
                d_time 
            ORDER BY
                d_time """.format(start_time, end_time)
    print(5555,sql)
    result = get_msg_data(sql)
    return comm_ret(data = result)

# 短信前后端数据对比
@system_603.get('/sms/datas')
async def get_sms_datas(starlette:Optional[str] = BEFORE_DATE_TIME, end_time:Optional[str] = NOW_DATE_TIME):
    """
        @param:
            start_time:    开始时间(可选参数)    str    默认  当前时间前一天
            end_time:      结束时间(可选参数)    str    默认  当前时间
        @return:
            [
                {
                "date": "2021-03-01 15:07:00",
                "rcv": 217,
                "load": 384
                },
                ...
            ]
    """
    sql = """SELECT
                t1.d_time,
                t1.rcv,
                t2.lod 
            FROM
                ( SELECT d_time, sum( sjjs_1m ) AS rcv FROM t_603_sms_sjjs GROUP BY d_time ) AS t1,
                ( SELECT d_time, sum( load_1m ) AS lod FROM t_603_sms_load GROUP BY d_time ) AS t2 
            WHERE
                t1.d_time = t2.d_time 
                AND t1.d_time BETWEEN '{}' AND '{}' """.format(start_time, end_time)
    print(sql)
    result = get_msg_data(sql)
    return comm_ret(data = result)

# 彩信接收数据量
@system_603.get("/mms/rcv")
async def get_mms_rcv(start_time:Optional[str] = BEFORE_DATE_TIME, end_time:Optional[str] = NOW_DATE_TIME):
    """
        @param:
            start_time:    开始时间(可选参数)    str    默认  当前时间前一天
            end_time:      结束时间(可选参数)    str    默认  当前时间
        @return:
            [
                {
                "date": "2021-03-01 14:57:00",
                "value": 75775
                },
                ...
            ]
    """
    sql = """SELECT
                d_time,
                SUM( sjjs_1m ) 
            FROM
                t_603_mms_sjjs 
            WHERE
                d_time BETWEEN '{}' 
                AND '{}' 
            GROUP BY
                d_time 
            ORDER BY
                d_time """.format(start_time, end_time)
    print(sql)
    result = get_msg_data(sql)
    return comm_ret(data = result)

# 彩信加载数据量
@system_603.get("/mms/load")
async def get_mms_load(start_time:Optional[str] = BEFORE_DATE_TIME, end_time:Optional[str] = NOW_DATE_TIME):
    """
        @param:
            start_time:    开始时间(可选参数)    str    默认  当前时间前一天
            end_time:      结束时间(可选参数)    str    默认  当前时间
        @return:
            [
                {
                "date": "2021-03-01 14:57:00",
                "value": 75775
                },
                ...
            ]
    """
    sql = """SELECT
                d_time,
                SUM( load_1m ) 
            FROM
                t_603_mms_load 
            WHERE
                d_time BETWEEN '{}' 
                AND '{}' 
            GROUP BY
                d_time 
            ORDER BY
                d_time """.format(start_time, end_time)
    print(sql)
    result = get_msg_data(sql)
    return comm_ret(data = result)

# 彩信前后端数据对比
@system_603.get('/mms/datas')
async def get_mms_datas(start_time:Optional[str] = BEFORE_DATE_TIME, end_time:Optional[str] = NOW_DATE_TIME):
    """
        @param:
            start_time:    开始时间(可选参数)    str    默认  当前时间前一天
            end_time:      结束时间(可选参数)    str    默认  当前时间
        @return:
            [
                {
                "date": "2021-03-01 15:07:00",
                "rcv": 217,
                "load": 384
                },
                ...
            ]
    """
    sql = """SELECT
                t3.d_time,
                t3.rcv,
                t4.lod 
            FROM
                ( SELECT d_time, sum( sjjs_1m ) AS rcv FROM t_603_mms_sjjs GROUP BY d_time ) AS t3,
                ( SELECT d_time, sum( load_1m ) AS lod FROM t_603_mms_load GROUP BY d_time ) AS t4 
            WHERE
                t3.d_time = t4.d_time 
                AND t3.d_time BETWEEN '{}' AND '{}' """.format(start_time, end_time)
    print(sql)
    result = get_msg_data(sql)
    return comm_ret(data = result)

# 读取短彩信数据 返回相同格式数据
def get_msg_data(sql:str):
    """
        @param:
            sql:    sql语句    str
        @return:
            [
                {
                "date": "2021-03-01 14:57:00",
                "value": 75775
                },
                或者
                {
                "date": "2021-03-01 15:07:00",
                "rcv": 217,
                "load": 384
                }
            ]
    """
    db = MySqLHelper()
    rows = db.selectall(sql=sql)
    data_list = [list(row) for row in rows]
    temp_data = data_processing(data_list, 2000)
    ret = []
    for item in temp_data:
        if len(item) == 2:
            temp_dict = {}
            temp_dict["date"] = item[0]
            temp_dict["value"] = item[1]
        else:
            temp_dict = {}
            temp_dict["date"] = item[0]
            temp_dict["rcv"] = item[1]
            temp_dict["load"] = item[2]
        ret.append(temp_dict)
    return ret
