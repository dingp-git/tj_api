import time
import datetime

# 当前时间  2020-11-23 11:45:34
NOW_DATE_TIME = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
# NOW_DATE_TIME = "2020-11-20 23:00:00"

# 当前时间减去15分钟
MIN_DATE_TIME = (datetime.datetime.now() + datetime.timedelta(minutes=-15)).strftime("%Y-%m-%d %H:%M:%S")
# MIN_DATE_TIME = "2020-11-20 22:00:00"
# 当前时间减去1小时
HOUR_DATE_TIME = (datetime.datetime.now() + datetime.timedelta(minutes=-60)).strftime("%Y-%m-%d %H:%M:%S")
# HOUR_DATE_TIME = "2020-11-20 22:00:00"

# 当前时间减去1天  2020-11-22 11:45:34
DEC_DATE_TIME = (datetime.datetime.now() + datetime.timedelta(days=-1)).strftime("%Y-%m-%d %H:%M:%S")
# 当前时间减去7天
WEEK_DATE_TIME = (datetime.datetime.now() + datetime.timedelta(weeks=-1)).strftime("%Y-%m-%d %H:%M:%S")
# WEEK_DATE_TIME = "2020-11-10 22:00:00"


# 获取当前日期  2020-11-23
NOW_DATE = datetime.date.today()
# 格式化当前日期  20201123
FORMATE_NOW_DATE = ''.join(str(NOW_DATE).split('-'))

ERROR_MSG = {
    "-100": "系统错误",
    "0": "操作成功",
    "400": "参数错误",
    "404": "资源不存在"
}

RESULT = {
    "code": "0",
    "ret": ERROR_MSG.get("0"),
    "message": []
}

# ************************************

# location = "dx_ds"
# startTime = "2020-12-01 7:00:00"
# endTime = "2020-12-3 7:00:00"

# select d_time,inet_ntoa( ip_addr ) AS IP, gll_E from gll,tj_server where msg like "% {} %"

rows = ((datetime.datetime(2020, 12, 1, 14, 44, 17), '10.78.0.1', 87.61),
        (datetime.datetime(2020, 12, 1, 14, 54, 17), '10.78.0.1', 87.05),
        (datetime.datetime(2020, 12, 1, 14, 44, 18), '10.78.0.2', 86.75),
        (datetime.datetime(2020, 12, 1, 14, 54, 18), '10.78.0.2', 86.72),
        (datetime.datetime(2020, 12, 1, 14, 44, 19), '10.78.0.3', 69.78),
        (datetime.datetime(2020, 12, 1, 14, 54, 19), '10.78.0.3', 66.81),
        (datetime.datetime(2020, 12, 1, 14, 44, 20), '10.78.0.4', 56.26),
        (datetime.datetime(2020, 12, 1, 14, 54, 20), '10.78.0.4', 57.17))
data_list = [list(i) for i in rows]
print(data_list)

data_dict = {}
for j in data_list:
    k = ".".join(j[1].split(".")[2:])
    if k not in data_dict.keys():
        data_dict.setdefault(k, [])
        temp_dict = {}
        temp_dict['date'] = j[0]
        temp_dict['value'] = j[2]
        data_dict[k].append(temp_dict)
    else:
        temp_dict = {}
        temp_dict['date'] = j[0]
        temp_dict['value'] = j[2]
        data_dict[k].append(temp_dict)
print(data_dict)
