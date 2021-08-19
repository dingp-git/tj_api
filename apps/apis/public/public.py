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
import json
import requests
from fastapi import APIRouter, Depends, Query
# Local application imports
from apps.utils import resp_code
from apps.utils.comm_ret import comm_ret
import math
import datetime
from typing import Optional
from apps.utils.mysql_conn_pool.mysql_helper import MySqLHelper
from apps.utils.tools import data_processing, get_now_date_time, get_before_date_time

public = APIRouter()

@public.get('/alarm/real_time', summary="获取系统实时告警")
async def get_real_time_alarm(start_time: Optional[str] = None, end_time: Optional[str] = None):
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
    if start_time == None:
        start_time = get_before_date_time()
    if end_time == None:
        end_time = get_now_date_time()
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
    return comm_ret(data=result)


@public.get('/alarm/history', summary="获取系统历史告警")
async def get_history_alarm(start_time: Optional[str] = None, end_time: Optional[str] = None):
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
    if start_time == None:
        start_time = get_before_date_time()
    if end_time == None:
        end_time = get_now_date_time()
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
    return comm_ret(data=result)


@public.get('/topo/info', summary="获取地图显示信息")
async def get_topo_info(system_name: str, model_name: str, type: str):
    db = MySqLHelper()
    sql = """
        SELECT
            ip,
            system_name,
            model_name,
            type,
            config,
            crcity,
            crlacpoint,
            crprovince_name,
            dfcoding,
            dmodel_name,
            dname,
            dstatus_name,
            maindept_name,
            mperson_name,
            rname
        FROM
            t_public_topo
        WHERE
            system_name = '{}' 
            AND model_name = '{}'
            AND type = '{}'
    """.format(system_name, model_name, type)
    rows = db.selectall(sql=sql)
    data_list = [list(row) for row in rows]
    temp_data = data_processing(data_list, 2000)
    result = []
    for item in temp_data:
        temp_dict = {}
        temp_dict['ip'] = item[0]
        temp_dict['system_name'] = item[1]
        temp_dict['model_name'] = item[2]
        temp_dict['type'] = item[3]
        temp_dict['config'] = item[4]
        temp_dict['crcity'] = item[5]
        temp_dict['crlacpoint'] = item[6]
        temp_dict['crprovince_name'] = item[7]
        temp_dict['dfcoding'] = item[8]
        temp_dict['dmodel_name'] = item[9]
        temp_dict['dname'] = item[10]
        temp_dict['dstatus_name'] = item[11]
        temp_dict['maindept_name'] = item[12]
        temp_dict['mperson_name'] = item[13]
        temp_dict['rname'] = item[14]
        result.append(temp_dict)
    return comm_ret(data=result)

@public.get('/topo/infos', summary="获取地图显示信息")
async def get_topo_infos():
    db = MySqLHelper()
    sql = """
        SELECT
            ip,
            system_name,
            model_name,
            type,
            config,
            crcity,
            crlacpoint,
            crprovince_name,
            dfcoding,
            dmodel_name,
            dname,
            dstatus_name,
            maindept_name,
            mperson_name,
            rname
        FROM
            t_public_topo
    """
    rows = db.selectall(sql=sql)
    data_list = [list(row) for row in rows]
    temp_data = data_processing(data_list, 2000)
    print(temp_data)
    result = {}
    for item in temp_data:
        temp_dict = {}
        temp_dict['ip'] = item[0]
        temp_dict['config'] = item[4]
        temp_dict['crcity'] = item[5]
        temp_dict['crlacpoint'] = item[6]
        temp_dict['crprovince_name'] = item[7]
        temp_dict['dfcoding'] = item[8]
        temp_dict['dmodel_name'] = item[9]
        temp_dict['dname'] = item[10]
        temp_dict['dstatus_name'] = item[11]
        temp_dict['maindept_name'] = item[12]
        temp_dict['mperson_name'] = item[13]
        temp_dict['rname'] = item[14]
        if item[1] not in result.keys():
            t1 = result.setdefault(item[1], {})
            t2 = t1.setdefault(item[2], {})
            t3 = t2.setdefault(item[3], [])
            t3.append(temp_dict)
        else:
            if item[2] not in result[item[1]].keys():
                t4 = result[item[1]].setdefault(item[2], {})
                t5 = t4.setdefault(item[3], [])
                t5.append(temp_dict)
            else:
                if item[3] not in result[item[1]][item[2]].keys():
                    t6 =  result[item[1]][item[2]].setdefault(item[3], [])
                    t6.append(temp_dict)
                else:
                    result[item[1]][item[2]][item[3]].append(temp_dict)
    return comm_ret(data=result)