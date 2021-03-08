from rest_framework.views import APIView
from rest_framework.response import Response
from django.http.response import JsonResponse
from django.db import connections, connection
from ..utils import get_logger
from ..GlobalParam import *
import traceback

logger = get_logger("Message")


# D信各时间段前端接收数据和后端加载数据对比
class DxFrontVsLoad(APIView):
    # 无参数  默认返回一天内数据
    def get(self, request):
        # 接口测试数据
        # DEC_DATE_TIME = "2020-12-01 9:00:00"
        # NOW_DATE_TIME = "2020-12-02 15:57:10"
        try:
            result = {
                "code": "0",
                "ret": ERROR_MSG.get("0"),
                "message": []
            }
            with connections['tianjin'].cursor() as cursor:
                sql = """
                    SELECT
                        SMSC.d_time,
                        SMSC.smsc,
                        DX.dx 
                    FROM
                        ( SELECT d_time, sum( SMSC_1M ) AS smsc FROM SJJS_SMSC GROUP BY d_time ) AS SMSC,
                        ( SELECT d_time, sum( DX_1M ) AS dx FROM LOAD_DX GROUP BY d_time ) AS DX 
                    WHERE
                        SMSC.d_time = DX.d_time 
                        AND SMSC.d_time BETWEEN '{}' AND '{}';""".format(DEC_DATE_TIME, NOW_DATE_TIME)
                cursor.execute(sql)
                rows = cursor.fetchall()
                print(sql, '*****************', len(rows))
            data_list = [list(row) for row in rows]
            json_list = []
            for j in data_list:
                json_dict = {}
                json_dict['date'] = j[0]
                json_dict['dxReceiveData'] = j[1]
                json_dict['dxLoadData'] = j[2]
                json_list.append(json_dict)
            result["message"] = json_list
            return Response(result)
        except Exception as e:
            logger.error(traceback.format_exc())
            return Response({"code": "-100", "ret": ERROR_MSG.get("-100")})

    # 日期参数，查询指定范围内数据
    def post(self, request):
        start_time = request.data.get("startTime")
        end_time = request.data.get("endTime")
        if start_time is None or end_time is None:
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
                        SMSC.d_time,
                        SMSC.smsc,
                        DX.dx 
                    FROM
                        ( SELECT d_time, sum( SMSC_1M ) AS smsc FROM SJJS_SMSC GROUP BY d_time ) AS SMSC,
                        ( SELECT d_time, sum( DX_1M ) AS dx FROM LOAD_DX GROUP BY d_time ) AS DX 
                    WHERE
                        SMSC.d_time = DX.d_time 
                        AND SMSC.d_time BETWEEN '{}' AND '{}';""".format(start_time, end_time)
                cursor.execute(sql)
                rows = cursor.fetchall()
            data_list = [list(row) for row in rows]
            json_list = []
            for j in data_list:
                json_dict = {}
                json_dict['date'] = j[0]
                json_dict['dxReceiveData'] = j[1]
                json_dict['dxLoadData'] = j[2]
                json_list.append(json_dict)
            result["message"] = json_list
            return Response(result)
        except Exception as e:
            logger.error(traceback.format_exc())
            return Response({"code": "-100", "ret": ERROR_MSG.get("-100")})


# C信各时间段前端接收数据和后端加载数据对比
class CxFrontVsLoad(APIView):
    def get(self, request):
        # 接口测试数据
        # DEC_DATE_TIME = "2020-12-01 17:45:10"
        # NOW_DATE_TIME = "2020-12-02 17:56:10"
        try:
            result = {
                "code": "0",
                "ret": ERROR_MSG.get("0"),
                "message": []
            }
            with connections['tianjin'].cursor() as cursor:
                sql = """
                    SELECT
                        MMS.d_time,
                        MMS.mms,
                        CX.cx   
                    FROM
                        ( SELECT d_time, sum( MMS_1M ) AS mms FROM SJJS_MMS GROUP BY d_time ) AS MMS,
                        ( SELECT d_time, sum( AvroCxMin ) AS cx FROM LOAD_CX GROUP BY d_time ) AS CX 
                    WHERE
                        MMS.d_time = CX.d_time 
                        AND MMS.d_time BETWEEN '{}' AND '{}';""".format(DEC_DATE_TIME, NOW_DATE_TIME)
                cursor.execute(sql)
                rows = cursor.fetchall()
                print(sql, '*****************', len(rows))
            data_list = [list(row) for row in rows]
            json_list = []
            for j in data_list:
                json_dict = {}
                json_dict['date'] = j[0]
                json_dict['cxReceiveData'] = j[1]
                json_dict['cxLoadData'] = j[2]
                json_list.append(json_dict)
            result["message"] = json_list
            return Response(result)
        except Exception as e:
            logger.error(traceback.format_exc())
            return Response({"code": "-100", "ret": ERROR_MSG.get("-100")})

    def post(self, request):
        start_time = request.data.get("startTime")
        end_time = request.data.get("endTime")
        if start_time is None or end_time is None:
            return Response({"code": "400", "ret": ERROR_MSG.get("400")})
        # 接口测试数据
        # start_time = "2020-10-27 17:56:10"
        # end_time = "2020-10-29 17:56:10"
        try:
            result = {
                "code": "0",
                "ret": ERROR_MSG.get("0"),
                "message": []
            }
            with connections['tianjin'].cursor() as cursor:
                sql = """
                    SELECT
                        MMS.d_time,
                        MMS.mms,
                        CX.cx 
                    FROM
                        ( SELECT d_time, sum( MMS_1M ) AS mms FROM SJJS_MMS GROUP BY d_time ) AS MMS,
                        ( SELECT d_time, sum( AvroCxMin ) AS cx FROM LOAD_CX GROUP BY d_time ) AS CX 
                    WHERE
                        MMS.d_time = CX.d_time 
                        AND MMS.d_time BETWEEN '{}' AND '{}';""".format(start_time, end_time)
                cursor.execute(sql)
                rows = cursor.fetchall()
                print(sql, '*****************', len(rows))
            data_list = [list(row) for row in rows]
            json_list = []
            for j in data_list:
                json_dict = {}
                json_dict['date'] = j[0]
                json_dict['cxReceiveData'] = j[1]
                json_dict['cxLoadData'] = j[2]
                json_list.append(json_dict)
            result["message"] = json_list
            return Response(result)
        except Exception as e:
            logger.error(traceback.format_exc())
            return Response({"code": "-100", "ret": ERROR_MSG.get("-100")})
