from rest_framework.views import APIView
from rest_framework.response import Response
from django.http.response import JsonResponse
from django.db import connections, connection
from ..utils import get_logger
from ..GlobalParam import *
from config import *
import traceback
logger = get_logger("HiveDatabase")

# 整理数据格式
def format_data(rows):
    data_list = [list(row) for row in rows]
    data_dict = {}
    for item in data_list:
        item[0] = item[0].split('/')[-1]
        value_dict = {}
        value_dict['date'] = item[2]
        value_dict['value'] = item[1]
        if item[0] not in data_dict.keys():
            data_dict.setdefault(item[0], []).append(value_dict)
        else:
            data_dict[item[0]].append(value_dict)
    return data_dict

# 获取数据库/表 多组数据
def get_multip_data(start_time, end_time, multip_name):
    with connections['tianjin'].cursor() as cursor:
        sql = """
            SELECT
                db_name,
                data_volume,
                d_time 
            FROM
                509hive_db 
            WHERE d_time > '{}' 
                AND d_time <= '{}' """.format(start_time, end_time)
        if multip_name == "total":
            sql += "AND db_name LIKE '%_hive.db';"
        else:
            sql += "AND db_name LIKE '%{}/%';".format(multip_name)
        cursor.execute(sql)
        rows = cursor.fetchall()
        print(sql, "************")
    data_dict = format_data(rows)
    RESULT["message"] = data_dict
    return RESULT

# 获取数据库/表 单组数据
def get_single_data(start_time, end_time, single_name):
    with connections['tianjin'].cursor() as cursor:
        sql = """
            SELECT
                db_name,
                data_volume,
                d_time 
            FROM
                509hive_db 
            WHERE d_time > '{}' 
                AND d_time <= '{}' 
                AND ( db_name LIKE "%{}" ) ;""".format(start_time, end_time, single_name)
        cursor.execute(sql)
        rows = cursor.fetchall()
        print(sql, "************")
    data_dict = format_data(rows)
    RESULT["message"] = data_dict
    return RESULT


# 数据库/表 多组数据总量对比
class MultipltData(APIView):
    def get(self, request):
        start_time = request.GET.get('startTime')
        end_time = request.GET.get('endTime')
        multip_name = request.GET.get('multip_name')
        if not start_time:
            start_time = WEEK_DATE_TIME
        if not end_time:
            end_time = NOW_DATE_TIME
        if multip_name is None:
            return Response({"code":"400","ret":ERROR_MSG.get("400")})
        try:
            result = get_multip_data(start_time, end_time, multip_name)
            return Response(result)
        except Exception as e:
            logger.error(traceback.format_exc())
            return Response({"code": "-100", "ret": ERROR_MSG.get("-100")})


# 数据库/表  历史单个数据
class SingleData(APIView):
    def get(self, request):
        start_time = request.GET.get('startTime')
        end_time = request.GET.get('endTime')
        single_name = request.GET.get('single_name')
        if not start_time:
            start_time = WEEK_DATE_TIME
        if not end_time:
            end_time = NOW_DATE_TIME
        try:
            result = get_single_data(start_time, end_time, single_name)
            return Response(result)
        except Exception as e:
            logger.error(traceback.format_exc())
            return Response({"code": "-100", "ret": ERROR_MSG.get("-100")})


# 告警信息
class DatabaseWarning(APIView):
    def get(self,request):
        start_time = request.GET.get('startTime')
        end_time = request.GET.get('endTime')
        if not start_time:
            start_time = WEEK_DATE_TIME
        if not end_time:
            end_time = NOW_DATE_TIME
        try:
            with connections['tianjin'].cursor() as cursor:
                sql = """
                    SELECT
                        db_name,
                        data_volume,
                        d_time,
                        type 
                    FROM
                        509hive_db_warning 
                    WHERE
                        d_time > '{}' 
                        AND d_time <= '{}'
                    ORDER BY
                        d_time DESC;""".format(start_time,end_time)
                cursor.execute(sql)
                rows = cursor.fetchall()
            # print(sql,1111111)
            data_list = [list(row) for row in rows]
            json_list = []
            for item in data_list:
                data_dict = {}
                data_dict['db_name'] = item[0]
                data_dict['data_volume'] = item[1]
                data_dict['d_time'] = item[2]
                data_dict['type'] = item[3]
                json_list.append(data_dict)
            print(len(json_list))
            RESULT['message'] = json_list
            return Response(RESULT)
        except Exception as e:
            logger.error(traceback.format_exc())
            return Response({"code":"-100","ret":ERROR_MSG.get("-100")})



# TODO DEL
# 三个数据库总量对比
class TotalDatabase(APIView):
    # 返回三个数据库总量的数据对比
    def get(self, request):
        # 前端传参：startTime,endTime,index_name(全部"all")
        start_time = request.GET.get('startTime')
        end_time = request.GET.get('endTime')
        index_name = request.GET.get('index_name')
        if not start_time:
            start_time = WEEK_DATE_TIME
        if not end_time:
            end_time = NOW_DATE_TIME
        try:
            with connections['tianjin'].cursor() as cursor:
                sql = """
                    SELECT
                        db_name,
                        data_volume,
                        d_time 
                    FROM
                        509hive_db """
                if index_name == "all":
                    sql += """WHERE d_time > '{}' 
                                AND d_time <= '{}' 
                                AND ( db_name LIKE "%wa_dams_iie_hive.db" 
                                OR db_name LIKE "%wa_fms_iie_hive.db" 
                                OR db_name LIKE "%wa_zfd_tytt_hive.db" ) ;""".format(start_time, end_time)
                else:
                    sql += """WHERE d_time > '{}' 
                                AND d_time <= '{}' 
                                AND db_name LIKE '%{}';""".format(start_time, end_time, index_name)
                cursor.execute(sql)
                rows = cursor.fetchall()
                print(sql, "************")
            data_list = [list(row) for row in rows]
            data_dict = {}
            for item in data_list:
                item[0] = item[0].split('/')[-1]
                if item[0] not in data_dict.keys():
                    value_dict = {}
                    value_dict['date'] = item[2]
                    value_dict['value'] = item[1]
                    data_dict.setdefault(item[0], []).append(value_dict)
                else:
                    value_dict = {}
                    value_dict['date'] = item[2]
                    value_dict['value'] = item[1]
                    data_dict[item[0]].append(value_dict)
            RESULT["message"] = data_dict
            return Response(RESULT)
        except Exception as e:
            logger.error(traceback.format_exc())
            return Response({"code": "-100", "ret": ERROR_MSG.get("-100")})


# TODO DEL
class HiveDatabase(APIView):
    # 默认1天数据，截止日期参数选填
    def get(self, request):
        deadline = request.GET.get("deadline")
        if deadline is "" or deadline is None:
            deadline = NOW_DATE_TIME
        try:
            str_to_date = datetime.datetime.strptime(
                deadline, "%Y-%m-%d %H:%M:%S")
            day_date_time = (
                str_to_date + datetime.timedelta(days=-1)).strftime("%Y-%m-%d %H:%M:%S")
            result = {
                "code": "0",
                "ret": ERROR_MSG.get("0"),
                "message": []
            }
            with connections['tianjin'].cursor() as cursor:
                sql = """
                    SELECT
                        db_name,
                        data_volume,
                        d_time 
                    FROM
                        509hive_db 
                    WHERE
                        d_time > "{}" 
                        AND d_time <= "{}";""".format(day_date_time, deadline)
                cursor.execute(sql)
                rows = cursor.fetchall()
            print(sql, '***********', len(rows))
            data_list = [list(row) for row in rows]
            json_list = []
            for item in data_list:
                item[0] = item[0].split("/")[-1]
                flag = 0
                for targe in json_list:
                    if targe['date'] == item[2]:
                        targe[item[0]] = item[1]
                        flag = 1
                        break
                if flag == 0:
                    data_dict = {}
                    data_dict['date'] = item[2]
                    data_dict[item[0]] = item[1]
                    json_list.append(data_dict)
            result["message"] = json_list
            return Response(result)
        except Exception as e:
            logger.error(traceback.format_exc())
            return Response({"code": "-100", "ret": ERROR_MSG.get("-100")})

    # 返回接收各个数据库的数据   默认返回7天的数据
    def post(self, request):
        db_name = request.data.get("db_name")
        start_time = request.data.get("startTime", WEEK_DATE_TIME)
        end_time = request.data.get("endTime", NOW_DATE_TIME)
        if db_name is None:
            return Response({"code": "400", "ret": ERROR_MSG.get("400")})
        try:
            result = {
                "code": "0",
                "ret": ERROR_MSG.get("0"),
                "message": []
            }
            with connections['tianjin'].cursor() as cursor:
                sql = """
                    SELECT
                        db_name,
                        data_volume,
                        d_time 
                    FROM
                        509hive_db 
                    WHERE
                        db_name LIKE '%{}' 
                    AND
                        d_time > "{}" 
                        AND d_time <= "{}";""".format(db_name, start_time, end_time)
                cursor.execute(sql)
                rows = cursor.fetchall()
            print(sql, '*****************', len(rows))
            data_list = []
            for j in rows:
                data_dict = {}
                data_dict['date'] = j[2]
                data_dict['value'] = j[1]
                data_list.append(data_dict)
            result["message"] = data_list
            return Response(result)
        except Exception as e:
            logger.error(traceback.format_exc())
            return Response({"code": "-100", "ret": ERROR_MSG.get("-100")})
