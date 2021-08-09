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
# from apps.utils.tools import data_processing, NOW_DATE_TIME, BEFORE_DATE_TIME, NOW_DATE, BEFORE_DATE, BEFORE_MONTH
from apps.utils.tools import data_processing, get_now_date_time, get_before_date_time, get_now_date, get_before_date, get_before_month
from typing import Optional
from apps.utils.mysql_conn_pool.mysql_helper import MySqLHelper

system_ipsy = APIRouter()

@system_ipsy.get("/log/new", summary="最近一天  每家运营商上报日志条数")
async def get_log_new(start_time: Optional[str] = None, end_time: Optional[str] = None, 
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
    if start_time == None:
        start_time = get_before_date()
    if end_time == None:
        end_time = get_now_date()
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
    print(sql)
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
async def get_log_increment(start_time: Optional[str] = None, end_time: Optional[str] = None, 
                        index: Optional[str] = ''):
    """
        ## **param**:
            start_time:    开始时间(可选参数)    str    默认  当前时间前一天
            end_time:      结束时间(可选参数)    str    默认  当前时间
            index:         指标名称(可选参数)    str    默认  ''   '移动'
        ## **return**:
            [
                {
                    "lt_gw_diff": 2623574,
                    "d_time": "2021-04-24"
                },
                ...
            ]
    """
    if start_time == None:
        start_time = get_before_date()
    if end_time == None:
        end_time = get_now_date()
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
    print(sql)
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

@system_ipsy.get('/bc/datas', summary="最近一组拨测数据(一月一次)")
async def get_bc_data(start_time: Optional[str] = None, end_time: Optional[str] = None, isp: Optional[str] = ''):
    """
        ## **param**:
            start_time:    开始时间(可选参数)    str    默认  当前时间前一月
            end_time:      结束时间(可选参数)    str    默认  当前时间
            isp:           运营商(可选参数)      str    默认  '', '移动'(测试)
        ## **return**:
            [
                {
                    "移动": {
                        "省份": "天津市",
                        "运营商": "移动",
                        "上线上报比例": "46%",
                        "下线上报比例": "55%",
                        "访问上报比例": "36%",
                        "日志符合规范比例": "100%",
                        "日志正常加载比例": "100%",
                        "日志正常查询比例": "51%",
                        "外网代理上报比例": "0%",
                        "公网IP上报比例": "100%",
                        "公网IP准确性比例": "28%",
                        "IMEI上报正确性比例": "99%",
                        "IMSI上报正确性比例": "100%",
                        "LAC上报正确性比例": "99%",
                        "Ci上报正确性比例": "95%",
                        "结果条数": "1940",
                        "时间": "2021-07-28"
                    }
                },
                ...
    """
    if start_time == None:
        start_time = get_before_month()
    if end_time == None:
        end_time = get_now_date()
    db = MySqLHelper()
    sql = """
            SELECT
                province,
                isp,
                online_report_rate,
                offline_report_rate,
                access_report_rate,
                log_standard_rate,
                log_loading_rate,
                log_query_rate,
                extranet_report_rate,
                ip_report_rate,
                ip_accurate_rate,
                imei_correct_rate,
                imsi_correct_rate,
                lac_correct_rate,
                ci_correct_rate,
                total_nums,
                d_time
            FROM
                t_ipsy_bc 
            WHERE
        """
    if isp:
        sql += """
                isp = '{}'
                AND d_time BETWEEN '{}' 
                AND '{}' 
            ORDER BY
                d_time DESC 
                LIMIT 1
        """.format(isp, start_time, end_time)
    else:
        sql += """
                d_time BETWEEN '{}' 
                AND '{}' 
            ORDER BY
                d_time DESC 
                LIMIT 3
        """.format(start_time, end_time)
    print(sql)
    rows = db.selectall(sql=sql)
    data_list = [list(row) for row in rows]
    temp_data = data_processing(data_list, 2000)
    result = {}
    for item in temp_data:
        temp_dict = {}
        temp_dict['province'] = item[0]
        temp_dict['isp'] = item[1]
        temp_dict['online_report_rate'] = item[2]
        temp_dict['offline_report_rate'] = item[3]
        temp_dict['access_report_rate'] = item[4]
        temp_dict['log_standard_rate'] = item[5]
        temp_dict['log_loading_rate'] = item[6]
        temp_dict['log_query_rate'] = item[7]
        temp_dict['extranet_report_rate'] = item[8]
        temp_dict['ip_report_rate'] = item[9]
        temp_dict['ip_accurate_rate'] = item[10]
        temp_dict['imei_correct_rate'] = item[11]
        temp_dict['imsi_correct_rate'] = item[12]
        temp_dict['lac_correct_rate'] = item[13]
        temp_dict['ci_correct_rate'] = item[14]
        temp_dict['total_nums'] = item[15]
        temp_dict['d_time'] = item[16]
        result[item[1]] = temp_dict
    return comm_ret(data = result)

@system_ipsy.get('/bc/history', summary = "拨测历史数据")
async def get_bc_history(start_time: Optional[str] = None, end_time: Optional[str] = None, isp: Optional[str] = ''):
    """
        ## **param**:
            start_time:    开始时间(可选参数)    str    默认  当前时间前一月
            end_time:      结束时间(可选参数)    str    默认  当前时间
            isp:           运营商(可选参数)      str    默认  '', '移动'(测试)
        ## **return**:
            [
                {
                    "移动": {
                        "省份": "天津市",
                        "运营商": "移动",
                        "上线上报比例": "46%",
                        "下线上报比例": "55%",
                        "访问上报比例": "36%",
                        "日志符合规范比例": "100%",
                        "日志正常加载比例": "100%",
                        "日志正常查询比例": "51%",
                        "外网代理上报比例": "0%",
                        "公网IP上报比例": "100%",
                        "公网IP准确性比例": "28%",
                        "IMEI上报正确性比例": "99%",
                        "IMSI上报正确性比例": "100%",
                        "LAC上报正确性比例": "99%",
                        "Ci上报正确性比例": "95%",
                        "结果条数": "1940",
                        "时间": "2021-07-28"
                    }
                },
                ...
    """
    if start_time == None:
        start_time = get_before_month()
    if end_time == None:
        end_time = get_now_date()
    db = MySqLHelper()
    sql = """
            SELECT
                province,
                isp,
                online_report_rate,
                offline_report_rate,
                access_report_rate,
                log_standard_rate,
                log_loading_rate,
                log_query_rate,
                extranet_report_rate,
                ip_report_rate,
                ip_accurate_rate,
                imei_correct_rate,
                imsi_correct_rate,
                lac_correct_rate,
                ci_correct_rate,
                total_nums,
                d_time
            FROM
                t_ipsy_bc 
            WHERE
        """
    if isp:
        sql += """
                isp = '{}'
                AND d_time BETWEEN '{}' 
                AND '{}' 
        """.format(isp, start_time, end_time)
    else:
        sql += """
                d_time BETWEEN '{}' 
                AND '{}' 
        """.format(start_time, end_time)
    print(sql)
    rows = db.selectall(sql=sql)
    data_list = [list(row) for row in rows]
    temp_data = data_processing(data_list, 2000)
    result = {}
    for item in temp_data:
        temp_dict = {}
        temp_dict['province'] = item[0]
        temp_dict['isp'] = item[1]
        temp_dict['online_report_rate'] = item[2]
        temp_dict['offline_report_rate'] = item[3]
        temp_dict['access_report_rate'] = item[4]
        temp_dict['log_standard_rate'] = item[5]
        temp_dict['log_loading_rate'] = item[6]
        temp_dict['log_query_rate'] = item[7]
        temp_dict['extranet_report_rate'] = item[8]
        temp_dict['ip_report_rate'] = item[9]
        temp_dict['ip_accurate_rate'] = item[10]
        temp_dict['imei_correct_rate'] = item[11]
        temp_dict['imsi_correct_rate'] = item[12]
        temp_dict['lac_correct_rate'] = item[13]
        temp_dict['ci_correct_rate'] = item[14]
        temp_dict['total_nums'] = item[15]
        temp_dict['d_time'] = item[16]
        if item[1] not in result.keys():
            temp_list = result.setdefault(item[1], [])
            temp_list.append(temp_dict)
        else:
            result[item[1]].append(temp_dict)
    return comm_ret(data = result)

# TODO api
# @system_ipsy.get('/proxy_ip_data', summary = "代理服务器数据是否正常入库 数据")

# @system_ipsy.get('/database_produce_data', summary = "当天库表产生情况 数据")
