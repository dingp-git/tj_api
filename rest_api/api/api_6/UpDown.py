from rest_framework.views import APIView
from rest_framework.response import Response
from django.http.response import JsonResponse
from django.db import connections, connection
from ..utils import get_logger
from ..GlobalParam import *
import traceback

logger = get_logger("UpDown")


# 上下行速率
class RateVsReqrsp(APIView):
    # 默认15分钟数据，截止日期参数，运营商-协议-req,rsp
    def get(self, request):
        # 1、获取参数
        deadline = request.GET.get("deadline")
        if deadline is "" or deadline is None:
            deadline = NOW_DATE_TIME
        try:
            str_to_date = datetime.datetime.strptime(deadline, "%Y-%m-%d %H:%M:%S")
            min_date_time = (str_to_date + datetime.timedelta(minutes=-15)).strftime("%Y-%m-%d %H:%M:%S")
            result = {
                "code": "0",
                "ret": ERROR_MSG.get("0"),
                "message": []
            }
            # 2、查询数据
            with connections['tianjin'].cursor() as cursor:
                sql = """
                    SELECT
                        isp,
                        stat_time,
                        protocol,
                        FORMAT( SUM( match_count_sum )/ SUM( req_count_sum )* 100, 2 ) AS req_rate,
                        FORMAT( SUM( match_count_sum )/ SUM( rsp_count_sum )* 100, 2 ) AS rsp_rate 
                    FROM
                        msg_stat 
                    WHERE
                        stat_time > "{}" AND stat_time <="{}" 
                    GROUP BY
                        protocol,
                        isp,
                        stat_time;""".format(min_date_time, deadline)
                cursor.execute(sql)
                rows = cursor.fetchall()
            print(111111, sql)
            # 3、组织数据格式
            data_list = [list(row) for row in rows]
            data = {}
            for i in data_list:
                if (i[2] in data.keys()):
                    if (i[0] in data[i[2]].keys()):
                        d3 = data.setdefault(data[i[2]][i[0]], {})
                        d3['req_rate'] = i[3]
                        d3['rsp_rate'] = i[4]
                    else:
                        data_dict = {"req_rate": i[3], "rsp_rate": i[4]}
                        data[i[2]][i[0]] = data_dict
                else:
                    d1 = data.setdefault(i[2], {})
                    d2 = d1.setdefault(i[0], {})
                    d2["req_rate"] = i[3]
                    d2["rsp_rate"] = i[4]
            data.setdefault("Gn-A11", {})
            if "Gn" in data.keys() and "A11" in data.keys():
                for i in data["Gn"].keys():
                    # data["Gn-A11"]["Gn" + i] = data["Gn"][i]
                    data["Gn-A11"][i] = data["Gn"][i]
                for j in data["A11"].keys():
                    # data["Gn-A11"]["A11" + j] = data["A11"][j]
                    data["Gn-A11"][3] = data["A11"][j]
                data.pop("Gn")
                data.pop("A11")
            print(data)
            result["message"] = data
            # 4、返回结果
            return Response(result)
        except Exception as e:
            logger.error(traceback.format_exc())
            return Response({"code": "-100", "ret": ERROR_MSG.get("-100")})

    # 二级弹窗 时间范围参数可选，默认7天，必填参数isp,protocol
    def post(self, request):
        # 1、获取参数
        start_time = request.data.get("startTime", WEEK_DATE_TIME)
        end_time = request.data.get("endTime", NOW_DATE_TIME)
        isp = request.data.get("isp")
        protocol = request.data.get("protocol")
        if isp is None or protocol is None:
            return Response({"code": "400", "ret": ERROR_MSG.get("400")})
        if protocol == "Gn-A11":
            protocol = isp[:-1]
            isp = isp[-1]
        print(666666, protocol, isp)
        try:
            result = {
                "code": "0",
                "ret": ERROR_MSG.get("0"),
                "message": []
            }
            # 2、查询数据
            with connections['tianjin'].cursor() as cursor:
                sql = """
                    SELECT
                        isp,
                        stat_time,
                        protocol,
                        FORMAT( SUM( match_count_sum )/ SUM( req_count_sum )* 100, 2 ) AS req_rate,
                        FORMAT( SUM( match_count_sum )/ SUM( rsp_count_sum )* 100, 2 ) AS rsp_rate 
                    FROM
                        msg_stat 
                    WHERE
                        isp = '{}' 
                        AND protocol = '{}' 
                        AND stat_time BETWEEN '{}' AND '{}' 
                    GROUP BY 
                        protocol,
                        isp,
                        stat_time;""".format(isp, protocol, start_time, end_time)
                cursor.execute(sql)
                rows = cursor.fetchall()
            print(122221, sql)
            # 3、组织数据格式
            data_list = [list(row) for row in rows]
            json_list = []
            for j in data_list:
                json_dict = {}
                json_dict['stat_time'] = j[1]
                json_dict['req_rate'] = j[3]
                json_dict['rsp_rate'] = j[4]
                json_list.append(json_dict)
            print(json_list)
            result["message"] = json_list
            # 4、返回结果
            return Response(result)
        except Exception as e:
            logger.error(traceback.format_exc())
            return Response({"code": "-100", "ret": ERROR_MSG.get("-100")})
