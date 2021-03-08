from rest_framework.views import APIView
from rest_framework.response import Response
from django.http.response import JsonResponse
from django.db import connections, connection
from ..utils import get_logger
from ..GlobalParam import *
import re
import traceback

logger = get_logger("CorrelationRate")


# 存放gll数据  
class GetCorrelationRate(APIView):
    # 默认1小时数据，截止日期参数
    def get(self, request):
        # 1、接收参数
        deadline = request.GET.get("deadline")
        if deadline is "" or deadline is None:
            deadline = NOW_DATE_TIME
        # 接口测试数据
        # deadline = "2020-12-01 15:00:00"
        try:
            str_to_date = datetime.datetime.strptime(deadline, "%Y-%m-%d %H:%M:%S")
            hour_date_time = (str_to_date + datetime.timedelta(minutes=-60)).strftime("%Y-%m-%d %H:%M:%S")
            result = {
                "code": "0",
                "ret": ERROR_MSG.get("0"),
                "message": []
            }
            # 2、数据查询
            with connections['tianjin'].cursor() as cursor:
                sql = """SELECT
                        d_time,
                        inet_ntoa(ip_addr) AS ip,
                        gll_E,
                        msg
                    FROM
                        gll,
                        tj_server
                    WHERE
                        ip = inet_ntoa( ip_addr )
                        AND d_time BETWEEN '{}' AND '{}' ORDER BY d_time ASC;""".format(hour_date_time, deadline)
                cursor.execute(sql)
                rows = cursor.fetchall()
                print(sql, '*****************', len(rows))
            # 3、组织数据格式
            data_dict = {}
            for item in rows:
                data_dict[item[3]] = {
                    "date": item[0],
                    "value": item[2]
                }
            ret_dict = {}
            for k, v in data_dict.items():
                i = "_".join(k.split("_")[0:2])
                if i not in ret_dict.keys():
                    ret_dict.setdefault(i, [])
                    ret_dict[i].append(v)
                else:
                    ret_dict[i].append(v)
            res_dict = {}
            for k, v in ret_dict.items():
                res_dict.setdefault(k, {})
                length = len(ret_dict[k])
                value = 0
                for i in v:
                    value += i['value']
                temp = {}
                temp['date'] = v[length - 1]['date']
                temp['value'] = round(value / length, 2)
                res_dict[k] = temp
            # print(5555,rows)
            # data_list = [list(i) for i in rows]
            # print(6666,data_list)
            # ret_list = []
            # for i in data_list:
            #     temp_dict = {}s
            #     print(777,i[0],type(i[0]))
            #     temp_dict['value'] = round(i[0],2)
            #     temp_dict['location'] = i[1]
            #     ret_list.append(temp_dict)
            # print(ret_list)
            result["message"] = res_dict
            # 4、返回结果
            return Response(result)
        except Exception as e:
            logger.error(traceback.format_exc())
            return Response({"code": "-100", "ret": ERROR_MSG.get("-100")})

    # 返回接收IP的数据   默认返回7天的数据
    def post(self, request):
        # msg = request.data.get("ip")
        location = request.data.get("location")
        # 接口测试数据  
        # location = "dx_ds"
        start_time = request.data.get("startTime", WEEK_DATE_TIME)
        end_time = request.data.get("endTime", NOW_DATE_TIME)
        if location is None:
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
                        d_time,
                        inet_ntoa( ip_addr ) AS IP,
                        gll_E
                    FROM
                        gll,
                        tj_server
                    WHERE
                        msg LIKE '{}%' 
                        AND ip = inet_ntoa( ip_addr ) 
                        AND d_time BETWEEN '{}' AND '{}';""".format(location, start_time, end_time)
                cursor.execute(sql)
                rows = cursor.fetchall()
            print(sql, '*****************', len(rows))
            data_list = [list(i) for i in rows]
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
            # print(rows)
            # data_list = []
            # for j in rows:
            #     data_dict = {}
            #     data_dict['date'] = j[0]
            #     data_dict['value'] = j[2]
            #     data_list.append(data_dict)
            result["message"] = data_dict
            return Response(result)
        except Exception as e:
            logger.error(traceback.format_exc())
            return Response({"code": "-100", "ret": ERROR_MSG.get("-100")})
