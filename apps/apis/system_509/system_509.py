# @Copyright(C), OldFive, 2020.
# @Date : 2021/3/11 0011 10:48:09
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
509系统相关接口
"""

# Standard library imports

# Third party imports
from fastapi import APIRouter, Depends, Query
from starlette.status import *
# Local application imports
from apps.utils import resp_code
from apps.utils.comm_ret import comm_ret
from apps.utils.mysql_conn_pool.mysql_helper import MySqLHelper
from typing import Optional, List
from apps.utils.tools import data_processing, get_before_date_time, get_now_date_time
import datetime
import time


system_509 = APIRouter()


@system_509.get('/hive/new', summary="获取hive数据库、表最近一组数据量")
async def get_hive_new(end_time: Optional[str] = None):
    """
        ## **param**:
            end_time:      结束时间(可选参数)    str    默认  当前时间
        ## **return**:
            {
                "dams": {
                            "value": 226683990941262,
                            "date": "2021-03-01 16:00:00"
                        },
                ...
            }
    """
    if end_time == None:
        end_time = get_now_date_time()
    # 开始时间：end_time 减去 60分钟
    start_time = datetime.datetime.strptime(
        end_time, "%Y-%m-%d %H:%M:%S") + datetime.timedelta(minutes=-60)
    db = MySqLHelper()
    sql = """SELECT
                db_desc,
                data,
                d_time 
            FROM
                t_509_hive_db,
                t_509_hive_db_base 
            WHERE
                t_509_hive_db.db_id = t_509_hive_db_base.id 
                AND d_time BETWEEN '{}' 
                AND '{}' """.format(start_time, end_time)
    print(sql)
    rows = db.selectall(sql=sql)
    data_list = [list(row) for row in rows]
    temp_data = data_processing(data_list, 2000)
    result = {}
    for item in temp_data:
        temp_dict = {}
        temp_dict['value'] = item[1]
        temp_dict['date'] = item[2]
        result[item[0]] = temp_dict
    return comm_ret(data=result)


@system_509.get('/hive/datas', summary="获取hive中 某数据库、表的存储量")
async def get_hive_datas(db_name: str, start_time: Optional[str] = None, end_time: Optional[str] = None):
    """
        ## **param**:
            db_name:       数据库、表名(必传参数)  str    格式   dams
            start_time:    开始时间(可选参数)      str    默认  当前时间前一天
            end_time:      结束时间(可选参数)      str    默认  当前时间
        ## **return**:
            [
                {
                    "value": 141227937372889,
                    "date": "2021-03-21 16:00:00"
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
                db_desc,
                data,
                d_time 
            FROM
                t_509_hive_db,
                t_509_hive_db_base 
            WHERE
                t_509_hive_db.db_id = t_509_hive_db_base.id 
                AND db_desc = '{}' 
                AND d_time BETWEEN '{}' AND '{}'""".format(db_name, start_time, end_time)
    print(sql)
    rows = db.selectall(sql=sql)
    data_list = [list(row) for row in rows]
    temp_data = data_processing(data_list, 2000)
    result = []
    for item in temp_data:
        temp_dict = {}
        temp_dict['value'] = item[1]
        temp_dict['date'] = item[2]
        result.append(temp_dict)
    return comm_ret(data=result)

@system_509.get('/hive/data', summary="获取hive中 某数据库、表的存储量")
async def get_hive_data(db_name: str, start_time: Optional[str] = None, end_time: Optional[str] = None):
    """
        ## **param**:
            db_name:       数据库、表名(必传参数)  list   格式   'dams, fms'
            start_time:    开始时间(可选参数)      str    默认  当前时间前一天
            end_time:      结束时间(可选参数)      str    默认  当前时间
        ## **return**:
            [
                {
                    "value": 141227937372889,
                    "date": "2021-03-21 16:00:00"
                },
                ...
            ]
    """
    if start_time == None:
        start_time = get_before_date_time()
    if end_time == None:
        end_time = get_now_date_time()
    db_name = db_name.split(',')
    print(db_name)
    # print(db_name2)
    db = MySqLHelper()
    sql = """SELECT
                db_desc,
                data,
                d_time 
            FROM
                t_509_hive_db,
                t_509_hive_db_base 
            WHERE
                t_509_hive_db.db_id = t_509_hive_db_base.id 
                AND db_desc IN {} 
                AND d_time BETWEEN '{}' AND '{}'""".format(tuple(db_name), start_time, end_time)
    rows = db.selectall(sql=sql)
    print(sql)
    print(rows)
    data_list = [list(row) for row in rows]
    print(data_list)
    temp_data = data_processing(data_list, 2000)
    result = {}
    for item in temp_data:
        if item[0] not in result.keys():
            result[item[0]] = {}
            value_list = []
            date_list = []
            value_list.append(item[1])
            date_list.append(item[2])
            result[item[0]]['value'] = value_list
            result[item[0]]['date'] = date_list
        else:
            result[item[0]]['value'].append(item[1])
            result[item[0]]['date'].append(item[2])
    return comm_ret(data=result)


@system_509.get('/hive/increment', summary="获取hive中 某数据库、表的存储增量")
async def get_hive_increment(db_name: str, start_time: Optional[str] = None, end_time: Optional[str] = None):
    """
        ## **param**:
            db_name:       数据库、表名(必传参数)  str    格式   dams
            start_time:    开始时间(可选参数)      str    默认  当前时间前一天
            end_time:      结束时间(可选参数)      str    默认  当前时间
        ## **return**:
            [
                {
                    "increment": 55154278329,
                    "date": "2021-03-21 16:00:00"
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
                db_desc,
                increment,
                d_time 
            FROM
                t_509_hive_db_increment,
                t_509_hive_db_base 
            WHERE
                t_509_hive_db_increment.db_id = t_509_hive_db_base.id 
                AND db_desc = '{}' 
                AND d_time BETWEEN '{}' 
                AND '{}'""".format(db_name, start_time, end_time)
    rows = db.selectall(sql=sql)
    print(sql)
    data_list = [list(row) for row in rows]
    temp_data = data_processing(data_list, 2000)
    result = []
    for item in temp_data:
        temp_dict = {}
        temp_dict['increment'] = item[1]
        temp_dict['date'] = item[2]
        result.append(temp_dict)
    return comm_ret(data=result)


@system_509.get('/loading_rate/new', summary="获取最近一组加载率数据")
async def get_loading_rate_new(end_time: Optional[str] = None):
    """
        ## **param**:
            end_time:      结束时间(可选参数)    str    默认  当前时间
        ## **return**:
            {
                "10.238.1.1:9100": {
                                    "date": "2021-03-05 15:50:00",
                                    "value": 159447792
                                },
                ...
            }
    """
    if end_time == None:
        end_time = get_now_date_time()
    # 开始时间：end_time 减去 5分钟
    start_time = datetime.datetime.strptime(
        end_time, "%Y-%m-%d %H:%M:%S") + datetime.timedelta(minutes=-5)
    db = MySqLHelper()
    sql = """SELECT
                ip_port,
                d_time,
            DATA 
            FROM
                t_509_loading_rate 
            WHERE
                d_time BETWEEN '{}' 
                AND '{}'""".format(start_time, end_time)
    print(sql)
    rows = db.selectall(sql=sql)
    data_list = [list(row) for row in rows]
    temp_data = data_processing(data_list, 2000)
    result = {}
    for item in temp_data:
        temp_dict = {}
        temp_dict['date'] = item[1]
        temp_dict['value'] = item[2]
        result[item[0]] = temp_dict
    return comm_ret(data=result)


@system_509.get('/loading_rate/datas', summary="获取某ip_port 的加载率数据")
async def get_loading_rate_datas(ip_port: str, start_time: Optional[str] = None, end_time: Optional[str] = None):
    """
        ## **param**:
            ip_port:       ip和端口号(必传参数)    str    格式  10.238.1.1:9100
            start_time:    开始时间(可选参数)      str    默认  当前时间前一天
            end_time:      结束时间(可选参数)      str    默认  当前时间
        ## **return**:
            [
                {
                    "value": 141227937372889,
                    "date": "2021-03-21 16:00:00"
                },
                ...
            ]
    """
    if start_time == None:
        start_time = get_before_date_time()
    if end_time == None:
        end_time = get_now_date_time()
    print(ip_port, start_time, end_time)
    print(type(ip_port), start_time, end_time)
    db = MySqLHelper()
    sql = """SELECT
                ip_port,
                d_time,
                data
            FROM
                t_509_loading_rate 
            WHERE
                ip_port = '{}' 
                AND d_time BETWEEN '{}' 
                AND '{}'""".format(ip_port, start_time, end_time)
    rows = db.selectall(sql=sql)
    data_list = [list(row) for row in rows]
    temp_data = data_processing(data_list, 2000)
    result = []
    for item in temp_data:
        temp_dict = {}
        temp_dict['date'] = item[1]
        temp_dict['value'] = item[2]
        result.append(temp_dict)
    return comm_ret(data=result)


@system_509.get('/loading_rate/increment', summary="获取某ip_port 的加载率数据增量")
async def get_loading_rate_increment(ip_port: str, start_time: Optional[str] = None, end_time: Optional[str] = None):
    """
        ## **param**:
            ip_port:       ip和端口号(必传参数)    str    格式  10.238.1.1:9100
            start_time:    开始时间(可选参数)      str    默认  当前时间前一天
            end_time:      结束时间(可选参数)      str    默认  当前时间
        ## **return**:
            [
                {
                    "date": "2021-03-21 16:05:00",
                    "increment": 2338816
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
                ip_port,
                d_time,
                increment 
            FROM
                t_509_loading_rate_increment 
            WHERE
                ip_port = '{}' 
                AND d_time BETWEEN '{}' 
                AND '{}'""".format(ip_port, start_time, end_time)
    rows = db.selectall(sql=sql)
    data_list = [list(row) for row in rows]
    temp_data = data_processing(data_list, 2000)
    result = []
    for item in temp_data:
        temp_dict = {}
        temp_dict['date'] = item[1]
        temp_dict['increment'] = item[2]
        result.append(temp_dict)
    return comm_ret(data=result)


@system_509.get('/row_flow/isp_data', summary="获取原始流量 根据各运营商进行数据分组")
async def get_isp_data(operator: Optional[str] = None, start_time: Optional[str] = None,
                       end_time: Optional[str] = None):
    """
        ## **param**:
            operator:      运营商(可选参数)        str    格式  移动
            start_time:    开始时间(可选参数)      str    默认  当前时间前一天
            end_time:      结束时间(可选参数)      str    默认  当前时间
        ## **return**:
            {
                "电信": [
                            {
                                "date": "2021-03-04 14:21:00",
                                "ibps": 0,
                                "obps": 44912148
                            },
                            ...
                        ],
                ...
            }
    """
    if start_time == None:
        start_time = get_before_date_time()
    if end_time == None:
        end_time = get_now_date_time()
    db = MySqLHelper()
    sql = """
            SELECT
                operator,
                d_time, 
                SUM( ibps ),
                SUM( obps )
            FROM
                `t_509_row_flow`
            WHERE 
                ip_addr = '10.148.255.7' AND """
    if operator:
        sql += "operator='{}' AND ".format(operator)
    sql += """d_time BETWEEN '{}' AND '{}' 
                GROUP BY operator, d_time""".format(start_time, end_time)
    rows = db.selectall(sql=sql)
    data_list = [list(row) for row in rows]
    temp_data = data_processing(data_list, 2000)
    result = {}
    for item in temp_data:
        temp_dict = {}
        temp_dict['date'] = item[1]
        temp_dict['ibps'] = item[2]
        temp_dict['obps'] = item[3]
        if item[0] not in result.keys():
            t1 = result.setdefault(item[0], [])
            t1.append(temp_dict)
        else:
            result[item[0]].append(temp_dict)
    return comm_ret(data=result)


@system_509.get('/row_flow/datas', summary="获取原始流量各运营商 接收和加载数据量")
async def get_row_flow_datas(ip_addr: Optional[str] = None, operator: Optional[List[str]] = Query([]), dev_port: Optional[List[str]] = Query([]),
                             start_time: Optional[str] = None, end_time: Optional[str] = None):
    """
        ## **param**:
            ip_addr:       ip地址(可选参数)        str    格式  10.148.255.7
            dev_port:      设备端口号(可选参数)    str    格式  xgei-0/1/0/15
            operator:      运营商(可选参数)        str    格式  移动
            start_time:    开始时间(可选参数)      str    默认  当前时间前一天
            end_time:      结束时间(可选参数)      str    默认  当前时间
        ## **return**:
            {
                "电信": [
                            {
                                "date": "2021-03-05 16:55:00",
                                "ibps": 0,
                                "obps": 44912148
                            },
                            ...
                        ],
                ...
            }
    """
    if start_time == None:
        start_time = get_before_date_time()
    if end_time == None:
        end_time = get_now_date_time()
    print(ip_addr, operator, dev_port, start_time, end_time)
    db = MySqLHelper()
    if ip_addr == None and len(operator) == 0 and len(dev_port) == 0:
        sql = """
            SELECT
                ip_addr,
                d_time,
                SUM(ibps),
                SUM(obps)
            FROM
                `t_509_row_flow` 
            WHERE
                d_time BETWEEN '{}' 
                AND '{}' 
            GROUP BY
                ip_addr, 
                d_time
        """.format(start_time, end_time)
    if ip_addr != None and len(operator) == 0 and len(dev_port) == 0:
        sql = """
            SELECT
                operator,
                d_time,
                SUM( ibps ),
                SUM( obps ) ,
                ip_addr
            FROM
                `t_509_row_flow` 
            WHERE
                ip_addr = '{}' 
                AND d_time BETWEEN '{}' 
                AND '{}' 
            GROUP BY
                operator,
                d_time
        """.format(ip_addr, start_time, end_time)
    sql2 = """
        SELECT
            dev_port,
            d_time,
            ibps,
            obps,
            ip_addr,
            operator
        FROM
            `t_509_row_flow` 
        WHERE
    """
    if ip_addr != None and len(operator) != 0 and len(dev_port) == 0:
        if len(operator) == 1:
            sql = sql2 + """
                ip_addr = '{}' 
                AND operator = '{}'
                AND d_time BETWEEN '{}' 
                AND '{}'
            """.format(ip_addr, operator[0], start_time, end_time)
        else:
            sql = sql2 + """
                ip_addr = '{}' 
                AND operator IN {}
                AND d_time BETWEEN '{}' 
                AND '{}' 
            """.format(ip_addr, tuple(operator), start_time, end_time)
    if ip_addr != None and len(operator) != 0 and len(dev_port) != 0:
        if len(operator) == 1:
            if len(dev_port) == 1:
                sql = sql2 + """
                        ip_addr = '{}' 
                        AND operator = '{}'
                        AND dev_port = '{}'
                        AND d_time BETWEEN '{}' 
                        AND '{}' 
                """.format(ip_addr, operator[0], dev_port[0], start_time, end_time)
            else:
                sql = sql2 + """
                        ip_addr = '{}' 
                        AND operator = '{}'
                        AND dev_port IN {}
                        AND d_time BETWEEN '{}' 
                        AND '{}' 
                """.format(ip_addr, operator[0], tuple(dev_port), start_time, end_time)
        else:
            if len(dev_port) == 1:
                sql = sql2 + """
                        ip_addr = '{}' 
                        AND operator IN {}
                        AND dev_port = '{}'
                        AND d_time BETWEEN '{}' 
                        AND '{}' 
                """.format(ip_addr, tuple(operator), dev_port[0], start_time, end_time)
            else:
                sql = sql2 + """
                        ip_addr = '{}' 
                        AND operator IN {}
                        AND dev_port IN {}
                        AND d_time BETWEEN '{}' 
                        AND '{}' 
                """.format(ip_addr, tuple(operator), tuple(dev_port), start_time, end_time)
    rows = db.selectall(sql=sql)
    # print(sql)
    data_list = [list(row) for row in rows]
    temp_data = data_processing(data_list, 2000)
    # print(len(rows), data_list)
    result = {}
    for item in temp_data:
        temp_dict = {}
        temp_dict['date'] = item[1]
        temp_dict['ibps'] = item[2]
        temp_dict['obps'] = item[3]
        if item[0] not in result.keys():
            t1 = result.setdefault(item[0], [])
            t1.append(temp_dict)
        else:
            result[item[0]].append(temp_dict)
    return comm_ret(data=result)


@system_509.get('/row_flow/params', summary="获取原始流参数 数据")
async def get_row_flow_params():
    db = MySqLHelper()
    sql1 = """
            SELECT DISTINCT
                dev_port 
            FROM
                t_509_row_flow 
            WHERE
                ip_addr = '10.148.255.7' 
                AND operator = '电信'
        """
    rows = db.selectall(sql=sql1)
    dx_data_list = [list(row)[0] for row in rows]
    sql2 = """
            SELECT DISTINCT
                dev_port 
            FROM
                t_509_row_flow 
            WHERE
                ip_addr = '10.148.255.7' 
                AND operator = '联通'
        """
    rows = db.selectall(sql=sql2)
    lt_data_list = [list(row)[0] for row in rows]
    sql3 = """
            SELECT DISTINCT
                dev_port 
            FROM
                t_509_row_flow 
            WHERE
                ip_addr = '10.148.255.7' 
                AND operator = '移动'
        """
    rows = db.selectall(sql=sql3)
    yd_data_list = [list(row)[0] for row in rows]
    data_dict = {
        'ip': '10.148.255.7',
        '电信': dx_data_list,
        '联通': lt_data_list,
        '移动': yd_data_list,
    }
    return comm_ret(data=data_dict)


@system_509.get('/collect_flow/new', summary="获取天津网安监测域名采集机流量 最近一天的数据总量")
async def get_collect_flow_new(start_time: Optional[str] = None, end_time: Optional[str] = None):
    """
        ## **param**:
            start_time:    开始时间(可选参数)      str    默认  当前时间前一天
            end_time:      结束时间(可选参数)      str    默认  当前时间
        ## **return**:
            [
                {
                    "date": "2021-03-21 16:05:00",
                    "ibps": 2338816,
                    "obps": 2338816
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
                SUM( ibps ) AS ibps,
                SUM( obps ) AS obps,
                d_time 
            FROM
                t_509_collect_flow 
            WHERE
                d_time BETWEEN '{}' 
                AND '{}' 
            GROUP BY
                d_time""".format(start_time, end_time)
    print(sql)
    rows = db.selectall(sql=sql)
    data_list = [list(row) for row in rows]
    temp_list = data_processing(data_list, 2000)
    result = []
    for item in temp_list:
        temp_dict = {}
        temp_dict['date'] = item[2]
        temp_dict['ibps'] = item[0]
        temp_dict['obps'] = item[1]
        result.append(temp_dict)
    return comm_ret(data=result)


@system_509.get('/collect_flow/location', summary="获取天津网安监测域名采集机流量 以局点/运营商进行汇总数据")
async def get_collect_flow_new(start_time: Optional[str] = None, end_time: Optional[str] = None):
    """
        ## **param**:
            start_time:    开始时间(可选参数)      str    默认  当前时间前一天
            end_time:      结束时间(可选参数)      str    默认  当前时间
        ## **return**:
            {
                "TJDX": [
                            {
                                "ibps": 0,
                                "obps": 0,
                                "date": "2021-04-06 16:30:00"
                            },
                            ...
                        ]
                ...
            }
    """
    if start_time == None:
        start_time = get_before_date_time()
    if end_time == None:
        end_time = get_now_date_time()
    db = MySqLHelper()
    sql = """SELECT
                SUM( ibps ) AS ibps,
                SUM( obps ) AS obps,
                d_time,
                location 
            FROM
                t_509_collect_flow 
            WHERE
                location <> 'TJFZX' 
                AND d_time BETWEEN '{}' AND '{}' 
            GROUP BY
                location,
                d_time""".format(start_time, end_time)
    rows = db.selectall(sql=sql)
    data_list = [list(row) for row in rows]
    temp_list = data_processing(data_list, 2000)
    result = {}
    for item in temp_list:
        temp_dict = {}
        temp_dict['ibps'] = item[0]
        temp_dict['obps'] = item[1]
        temp_dict['date'] = item[2]
        if item[3] not in result.keys():
            t1 = result.setdefault(item[3], [])
            t1.append(temp_dict)
        else:
            result[item[3]].append(temp_dict)
    return comm_ret(data=result)


@system_509.get('/collect_flow/isp', summary="获取天津网安监测域名采集机流量 以运营商进行汇总数据",  deprecated=True)
async def get_collect_flow_new(start_time: Optional[str] = None, end_time: Optional[str] = None):
    """
        ## **param**:
            start_time:    开始时间(可选参数)      str    默认  当前时间前一天
            end_time:      结束时间(可选参数)      str    默认  当前时间
        ## **return**:
            {
                "1": [
                        {
                            "ibps": 936,
                            "obps": 0,
                            "date": "2021-04-07 11:00:00"
                        },
                        ...
                    ]
                ...
            }
    """
    if start_time == None:
        start_time = get_before_date_time()
    if end_time == None:
        end_time = get_now_date_time()
    db = MySqLHelper()
    sql = """SELECT
                SUM( ibps ) AS ibps,
                SUM( obps ) AS obps,
                d_time,
                isp 
            FROM
                t_509_collect_flow 
            WHERE
                isp <> 0 
                AND d_time BETWEEN '{}' 
                AND '{}' 
            GROUP BY
                isp,
                d_time""".format(start_time, end_time)
    rows = db.selectall(sql=sql)
    data_list = [list(row) for row in rows]
    temp_list = data_processing(data_list, 2000)
    result = {}
    for item in temp_list:
        temp_dict = {}
        temp_dict['ibps'] = item[0]
        temp_dict['obps'] = item[1]
        temp_dict['date'] = item[2]
        if item[3] not in result.keys():
            t1 = result.setdefault(item[3], [])
            t1.append(temp_dict)
        else:
            result[item[3]].append(temp_dict)
    return comm_ret(data=result)


@system_509.get('/flow_monitoring/new', summary="获取流监测流量 最近一天的数据总量")
async def get_flow_monitoring_new(start_time: Optional[str] = None, end_time: Optional[str] = None):
    """
        ## **param**:
            start_time:    开始时间(可选参数)      str    默认  当前时间前一天
            end_time:      结束时间(可选参数)      str    默认  当前时间
        ## **return**:
            [
                {
                    "ibps": 5467892,
                    "obps": 87128000,
                    "date": "2021-04-07 11:40:00"
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
                SUM( ibps ) AS ibps,
                SUM( obps ) AS obps,
                d_time 
            FROM
                t_509_flow_monitoring 
            WHERE
                d_time BETWEEN '{}' 
                AND '{}' 
            GROUP BY
                d_time""".format(start_time, end_time)
    rows = db.selectall(sql=sql)
    data_list = [list(row) for row in rows]
    temp_list = data_processing(data_list, 2000)
    result = []
    for item in temp_list:
        temp_dict = {}
        temp_dict['ibps'] = item[0]
        temp_dict['obps'] = item[1]
        temp_dict['date'] = item[2]
        result.append(temp_dict)
    return comm_ret(data=result)


@system_509.get('/flow_monitoring/isp', summary="获取流监测流量 以局点/运营商进行汇总数据")
async def get_flow_monitoring_isp(start_time: Optional[str] = None, end_time: Optional[str] = None):
    """
        ## **param**:
            start_time:    开始时间(可选参数)      str    默认  当前时间前一天
            end_time:      结束时间(可选参数)      str    默认  当前时间
        ## **return**:
            {
                "2": [
                        {
                            "ibps": 11722376,
                            "obps": 0,
                            "date": "2021-04-06 16:30:00"
                        },
                        ...
                    ],
                ...
            }
    """
    if start_time == None:
        start_time = get_before_date_time()
    if end_time == None:
        end_time = get_now_date_time()
    db = MySqLHelper()
    sql = """SELECT
                SUM( ibps ) AS ibps,
                SUM( obps ) AS obps,
                d_time,
                isp 
            FROM
                t_509_flow_monitoring 
            WHERE
                isp <> 0 
                AND d_time BETWEEN '{}' 
                AND '{}' 
            GROUP BY
                isp,
                d_time""".format(start_time, end_time)
    rows = db.selectall(sql=sql)
    data_list = [list(row) for row in rows]
    temp_list = data_processing(data_list, 2000)
    result = {}
    for item in temp_list:
        temp_dict = {}
        temp_dict['ibps'] = item[0]
        temp_dict['obps'] = item[1]
        temp_dict['date'] = item[2]
        if item[3] not in result.keys():
            t1 = result.setdefault(item[3], [])
            t1.append(temp_dict)
        else:
            result[item[3]].append(temp_dict)
    return comm_ret(data=result)
