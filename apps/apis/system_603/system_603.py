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
import datetime
import time
from loguru import logger
from apps.apis.public.public import data_processing, NOW_DATE_TIME, BEFORE_DATE_TIME
from typing import Optional

system_603 = APIRouter()


@system_603.get("/sms/rcv")
async def get_sms_rcv(start_time: Optional[str] = BEFORE_DATE_TIME, end_time: Optional[str] = NOW_DATE_TIME):
    """
        短信接收数据量
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


@system_603.get("/sms/load")
async def get_sms_load(start_time: Optional[str] = BEFORE_DATE_TIME, end_time: Optional[str] = NOW_DATE_TIME):
    """
        短信加载数据量
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
    print(5555, sql)
    result = get_msg_data(sql)
    return comm_ret(data=result)


@system_603.get('/sms/datas')
async def get_sms_datas(starlette: Optional[str] = BEFORE_DATE_TIME, end_time: Optional[str] = NOW_DATE_TIME):
    """
        短信前后端数据对比
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
    return comm_ret(data=result)


@system_603.get("/mms/rcv")
async def get_mms_rcv(start_time: Optional[str] = BEFORE_DATE_TIME, end_time: Optional[str] = NOW_DATE_TIME):
    """
        彩信接收数据量
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
    return comm_ret(data=result)


@system_603.get("/mms/load")
async def get_mms_load(start_time: Optional[str] = BEFORE_DATE_TIME, end_time: Optional[str] = NOW_DATE_TIME):
    """
        彩信加载数据量
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
    return comm_ret(data=result)


@system_603.get('/mms/datas')
async def get_mms_datas(start_time: Optional[str] = BEFORE_DATE_TIME, end_time: Optional[str] = NOW_DATE_TIME):
    """
        彩信前后端数据对比
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
    return comm_ret(data=result)


def get_msg_data(sql: str):
    """
        读取短彩信数据 返回相同格式数据
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


@system_603.get('/relation/location/new')
async def get_relation_new(end_time: Optional[str] = NOW_DATE_TIME):
    """
        返回各机房关联率信息
        @param:
            start_time:    开始时间(可选参数)    str    默认  当前时间前20分钟
            end_time:      结束时间(可选参数)    str    默认  当前时间
        @return:
            [
                {
                    "date": "2021-03-01 14:34:40",
                    "value": 665746.87,
                    "location": "lt_hld"
                },
                ...
            ]
    """
    start_time = datetime.datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S") + datetime.timedelta(minutes=-20)
    db = MySqLHelper()
    sql = """SELECT
                d_time,
                AVG( relate_rate ),
                location,
                abbr 
            FROM
                t_603_relate_rate,
                t_603_relate_rate_base
            WHERE
                INET_NTOA( ip_addr_dec ) = ip_addr 
                AND d_time BETWEEN '{}' AND '{}' 
            GROUP BY
                location """.format(start_time, end_time)
    print(sql)
    rows = db.selectall(sql=sql)
    data_list = [list(row) for row in rows]
    temp_data = data_processing(data_list, 2000)
    result = []
    for item in temp_data:
        temp_dict = {}
        temp_dict["date"] = item[0]
        temp_dict["value"] = item[1]
        temp_dict["location"] = "_".join(item[3].split("_")[0:2])
        result.append(temp_dict)
    return comm_ret(data=result)


@system_603.get('/relation/location/ip')
async def get_relation_ip(location: str, start_time: Optional[str] = BEFORE_DATE_TIME, end_time: Optional[str] = NOW_DATE_TIME):
    """
        返回某机房各ip关联率信息
        @param:
            start_time:    开始时间(可选参数)    str    默认  当前时间前一天
            end_time:      结束时间(可选参数)    str    默认  当前时间
            location:      机房位置(必传参数)    str    格式  dx_ds
        @return
        [
            {
                "0.1": [
                            {
                                "date": "2021-03-01 15:34:17",
                                "value": 84.39
                            },
                            ...
                        ],
                ...
            }
        ]
    """
    db = MySqLHelper()
    sql = """SELECT
                d_time,
                relate_rate,
                abbr 
            FROM
                t_603_relate_rate,
                t_603_relate_rate_base 
            WHERE
                INET_NTOA( ip_addr_dec ) = ip_addr 
                AND abbr LIKE '{}_%'
                AND d_time BETWEEN '{}' AND '{}' """.format(location, start_time, end_time)
    print(sql)
    rows = db.selectall(sql=sql)
    data_list = [list(row) for row in rows]
    temp_data = data_processing(data_list, 2000)
    result = {}
    for item in temp_data:
        temp = item[2].split('_')[2]
        temp_dict = {}
        temp_dict['date'] = item[0]
        temp_dict['value'] = item[1]
        if temp not in result.keys():
            result.setdefault(temp, []).append(temp_dict)
        else:
            result[temp].append(temp_dict)
    return comm_ret(data=result)


@system_603.get('/up_down/all')
async def get_up_down_all(start_time:Optional[str] = BEFORE_DATE_TIME, end_time:Optional[str] = NOW_DATE_TIME):
    """
        获取各机房中 各运营商 的上下行速率
        @param:
            start_time:    开始时间(可选参数)    str    默认  当前时间前一天
            end_time:      结束时间(可选参数)    str    默认  当前时间
        @return:
                "Gn/A11": {
                        "Gn1": [
                                    {
                                        "date": "2021-03-01 10:00:00",
                                        "req": 0.9966,
                                        "rsp": 1
                                    },
                                    ...
                                ]
                        "Gn2": [
                                    {
                                        "date": "2021-03-01 10:00:00",
                                        "req": 0.9966,
                                        "rsp": 1
                                    },
                                    ...
                                ],
                ...
                }
    """
    db = MySqLHelper()
    sql = """SELECT
                isp,
                protocol,
                d_time,
                req_match_rate,
                rsp_match_rate 
            FROM
                t_603_req_rsp_match 
            WHERE
                d_time BETWEEN '{}' AND '{}' """.format(start_time, end_time)
    rows = db.selectall(sql=sql)
    data_list = [list(row) for row in rows]
    temp_data = data_processing(data_list, 2000)
    result = {}
    for item in temp_data:
        temp_dict = {}
        temp_dict['date'] = item[2]
        temp_dict['req'] = item[3]
        temp_dict['rsp'] = item[4]
        if not (item[1] == 'Gn' or item[1] == 'A11'):
            if item[1] not in result.keys():
                t1 = result.setdefault(item[1],{})
                t2 = t1.setdefault(item[0],[])
                t2.append(temp_dict)
            else:
                if item[0] not in result[item[1]].keys():
                    t3 = result[item[1]].setdefault(item[0],[])
                    t3.append(temp_dict)
                else:
                    result[item[1]][item[0]].append(temp_dict)
        else:
            if not (item[1] == 'Gn' and item[0] == 3):
                t4 = result.setdefault("Gn/A11",{})
                t5 = t4.setdefault((item[1]+str(item[0])),[])
                t5.append(temp_dict)
    return comm_ret(data = result)


@system_603.get('/up_down/datas')
async def get_up_down_datas(isp:str, protocol:str, start_time:Optional[str] = BEFORE_DATE_TIME, end_time:Optional[str] = NOW_DATE_TIME):
    """
    获取某机房中 某运营商 的上下行速率
    @param:
        isp:           运营商(必传参数)      str    格式  1 或 Gn1
        protocol:      机房位置(必传参数)    str    格式  MC 或 Gn/A11
        start_time:    开始时间(可选参数)    str    默认  当前时间前一天
        end_time:      结束时间(可选参数)    str    默认  当前时间
    @return:
        [
            {
                "date": "2021-03-01 00:00:00",
                "req": 0.9245,
                "rsp": 0.925
            },
            ...
        ]
    """
    if len(isp) > 1:
        protocol = isp[:len(isp)-1]
        isp = isp[-1]
    db = MySqLHelper()
    sql = """SELECT
                d_time,
                req_match_rate,
                rsp_match_rate 
            FROM
                t_603_req_rsp_match 
            WHERE
                protocol = '{}' 
                AND isp = {} """.format(protocol, isp)
    print(55555555,sql)
    rows = db.selectall(sql=sql)
    data_list = [list(row) for row in rows]
    temp_list = data_processing(data_list, 2000)
    result = []
    for item in temp_list:
        temp_dict = {}
        temp_dict['date'] = item[0]
        temp_dict['req'] = item[1]
        temp_dict['rsp'] = item[2]
        result.append(temp_dict)
    return comm_ret(data = result)


@system_603.get('/five_code/all')
async def get_five_code_all(start_time:Optional[str] = BEFORE_DATE_TIME, end_time:Optional[str] = NOW_DATE_TIME):
    """
        获取五码比率
        @param:
            start_time:    开始时间(可选参数)    str    默认  当前时间前一天
            end_time:      结束时间(可选参数)    str    默认  当前时间
        @return:
            [
                "imsi": {
                        "1": [
                                {
                                    "value": 90.87,
                                    "date": "2021-03-02 14:00:00"
                                },
                                    ...
                            ],
                        ...
                        },
                ...
            ]
    """
    db = MySqLHelper()
    sql = """SELECT
                SUM(imsi_count),
                SUM(user_num_count),
                SUM(imei_count),
                SUM(areacode_count),
                SUM(uli_count),
                SUM(cdr_count),
                isp,
                d_time 
            FROM
                t_603_cdr 
            WHERE
                d_time BETWEEN '{}' AND '{}' 
            GROUP BY
                isp,
                d_time """.format(start_time, end_time)
    # sql = """SELECT
    #             FORMAT( SUM( imsi_count )/ SUM( cdr_count )* 100, 2 ) AS imsi,
    #             FORMAT( SUM( user_num_count )/ SUM( cdr_count )* 100, 2 ) AS user_num,
    #             FORMAT( SUM( imei_count )/ SUM( cdr_count )* 100, 2 ) AS imei,
    #             FORMAT( SUM( areacode_count )/ SUM( cdr_count )* 100, 2 ) AS areacode,
    #             FORMAT( SUM( uli_count )/ SUM( cdr_count )* 100, 2 ) AS uli,
    #             isp,
    #             d_time 
    #         FROM
    #             t_603_cdr 
    #         WHERE
    #             d_time BETWEEN '{}' AND '{}' 
    #         GROUP BY
    #             isp,
    #             d_time """.format(start_time, end_time)
    rows = db.selectall(sql=sql)
    # print(sql)
    data_list = [list(row) for row in rows]
    temp_data = data_processing(data_list, 2000)
    # for item in temp_data:
    #     temp_dict = {}
    #     temp_dict['imsi'] = item[0]
    #     temp_dict['user_num'] = item[1]
    #     temp_dict['imei'] = item[2]
    #     temp_dict['areacode'] = item[3]
    #     temp_dict['uli'] = item[4]
    #     temp_dict['isp'] = item[5]
    #     temp_dict['date'] = item[6]
    #     result.append(temp_dict)
    result = {
        "imsi":{},
        "user_num":{},
        "imei":{},
        "areacode":{},
        "uli":{}
    }
    for item in temp_data:
        flag = 0
        for j in result.keys():
            temp_dict = {}
            temp_dict['value'] = round((item[flag]/item[5])*100,2)
            temp_dict['date'] = item[7]
            flag += 1
            if item[6] not in result[j].keys():
                t1 = result[j].setdefault(item[6],[])
                t1.append(temp_dict)
            else:
                result[j][item[6]].append(temp_dict)
    return comm_ret(data = result)


@system_603.get('/five_code/datas')
async def get_five_code_datas(isp:str, code:str, start_time:Optional[str] = BEFORE_DATE_TIME, end_time:Optional[str] = NOW_DATE_TIME):
    """
        获取某码中 某运营商 的比率
        @param:
            isp:           运营商(必传参数)      str    格式  1
            code:          指标码(必传参数)      str    格式  imsi
            start_time:    开始时间(可选参数)    str    默认  当前时间前一天
            end_time:      结束时间(可选参数)    str    默认  当前时间
        @return:
            [
                {
                    "rate": 92.66,
                    "date": "2021-03-01 17:00:00"
                },
                ...
            ]
    """
    db = MySqLHelper()
    sql = """SELECT
                {}_count,
                cdr_count,
                d_time 
            FROM
                t_603_cdr 
            WHERE
                d_time BETWEEN '{}' AND '{}' 
            GROUP BY
                isp,
                d_time """.format(code, start_time, end_time)
    print(sql)
    rows = db.selectall(sql=sql)
    data_list = [list(row) for row in rows]
    temp_data = data_processing(data_list, 2000)
    result = []
    for item in temp_data:
        temp_dict = {}
        temp_dict['rate'] = round((item[0]/item[1])*100,2)
        temp_dict['date'] = item[2]
        result.append(temp_dict)
    return comm_ret(data = result)


@system_603.get('/cdr/datas')
async def get_cdr_datas(start_time:Optional[str] = BEFORE_DATE_TIME, end_time:Optional[str] = NOW_DATE_TIME):
    """
        获取t_603_cdr表中全部数据
        @param:
            start_time:    开始时间(可选参数)    str    默认  当前时间前一天
            end_time:      结束时间(可选参数)    str    默认  当前时间
        @return:
            [
                {
                    "cdr_type": 8,
                    "net_type": 4,
                    "cdr_count": 143122,
                    "imsi_count": 104319,
                    "user_num_count": 104034,
                    "imei_count": 104747,
                    "areacode_count": 122746,
                    "uli_count": 143117,
                    "isp": 1,
                    "ip_addr": "72.12",
                    "d_time": "2021-03-01 17:00:00"
                },
                ...
            ]
    """
    db = MySqLHelper()
    sql = """SELECT
                    cdr_type,
                    net_type,
                    cdr_count,
                    imsi_count,
                    user_num_count,
                    imei_count,
                    areacode_count,
                    uli_count,
                    isp,
                    ip_addr,
                    d_time 
                FROM
                    t_603_cdr
                WHERE
                    (cdr_count <> 0 
                    OR imsi_count <> 0 
                    OR user_num_count <> 0 
                    OR imei_count <> 0 
                    OR areacode_count <> 0 
                    OR uli_count <> 0 )
                    AND d_time BETWEEN '{}' AND '{}' """.format(start_time, end_time)
    rows = db.selectall(sql=sql)
    data_list = [list(row) for row in rows]
    temp_data = data_processing(data_list, 2000)
    result = []
    for item in temp_data:
        temp_dict = {}
        temp_dict['cdr_type'] = item[0]
        temp_dict['net_type'] = item[1]
        temp_dict['cdr_count'] = item[2]
        temp_dict['imsi_count'] = item[3]
        temp_dict['user_num_count'] = item[4]
        temp_dict['imei_count'] = item[5]
        temp_dict['areacode_count'] = item[6]
        temp_dict['uli_count'] = item[7]
        temp_dict['isp'] = item[8]
        temp_dict['ip_addr'] = '.'.join(item[9].split('.')[2:])
        temp_dict['d_time'] = item[10]
        result.append(temp_dict)
    return comm_ret(data = result)
