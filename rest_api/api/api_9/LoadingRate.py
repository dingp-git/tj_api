from rest_framework.views import APIView
from rest_framework.response import Response
from django.http.response import JsonResponse
from django.db import connections, connection
from ..utils import get_logger
from ..GlobalParam import *
from config import *
import traceback
logger = get_logger("LoadingRate")

class LoadingRateWarning(APIView):
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
                        device,
                        instance,
                        d_time,
                        speed,
                        type 
                    FROM
                        loading_rate_warning 
                    WHERE
                        d_time > '{}' 
                        AND d_time <= '{}' 
                    ORDER BY
                        d_time DESC;""".format(start_time,end_time)
                cursor.execute(sql)
                rows = cursor.fetchall()
            print(sql,2222)
            data_list = [list(row) for row in rows]
            json_list = []
            for item in data_list:
                data_dict = {}
                data_dict['device'] = item[0]
                data_dict['instance'] = item[1]
                data_dict['d_time'] = item[2]
                data_dict['speed'] = item[3]
                data_dict['type'] = item[4]
                json_list.append(data_dict)
            print(len(json_list))
            RESULT['message'] = json_list
            return Response(RESULT)
        except Exception as e:
            logger.error(traceback.format_exc())
            return Response({'code':'-100','ret':ERROR_MSG.get('-100')})



class LoadingRate(APIView):
    # 默认1天数据，截止日期参数选填
    def get(self,request):
        deadline = request.GET.get("deadline")
        if deadline is "" or deadline is None:
            deadline = NOW_DATE_TIME
        try:
            str_to_date = datetime.datetime.strptime(deadline,"%Y-%m-%d %H:%M:%S")
            day_date_time = (str_to_date + datetime.timedelta(days=-1)).strftime("%Y-%m-%d %H:%M:%S")
            result = {
                "code":"0",
                "ret":ERROR_MSG.get("0"),
                "message":[]
            }
            with connections['tianjin'].cursor() as cursor:
                sql = """
                    SELECT
                        instance,
                        speed,
                        d_time 
                    FROM
                        loading_rate 
                    WHERE
                        d_time > "{}" 
                        AND d_time <= "{}";""".format(day_date_time,deadline)
                cursor.execute(sql)
                rows = cursor.fetchall()
            print(sql,'***********',len(rows))
            data_list = [list(row) for row in rows]
            json_list = []
            for item in data_list:
                item[0] = item[0].split(".")[-1]
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
            return Response({"code":"-100","ret":ERROR_MSG.get("-100")})

    # 返回接收各个ip的数据   默认返回7天的数据
    def post(self, request):
        # 接收参数"1.9100或2.9100"
        ip = request.data.get("ip")
        start_time = request.data.get("startTime",WEEK_DATE_TIME)
        end_time = request.data.get("endTime",NOW_DATE_TIME) 
        if ip is None:
            return Response({"code":"400","ret":ERROR_MSG.get("400")})
        try:
            result = {
                "code":"0",
                "ret":ERROR_MSG.get("0"),
                "message":[]
            }
            with connections['tianjin'].cursor() as cursor:
                sql = """
                    SELECT
                        instance,
                        speed,
                        d_time
                    FROM
                        loading_rate 
                    WHERE
                        instance LIKE '%{}' 
                    AND
                        d_time > "{}" 
                        AND d_time <= "{}";""".format(ip,start_time,end_time)
                cursor.execute(sql)
                rows = cursor.fetchall()
            print(sql,'*****************',len(rows))
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
            return Response({"code":"-100","ret":ERROR_MSG.get("-100")})
