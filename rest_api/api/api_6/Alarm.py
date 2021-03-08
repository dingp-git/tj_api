import json

from rest_framework.response import Response
from django.http.response import JsonResponse
from rest_framework.views import APIView
from django.db import connections, connection
from ..utils import get_logger
from ..GlobalParam import *
from config import *
import traceback

logger = get_logger("Alarm")


class GetAlarmHistory(APIView):
    def get(self, request):
        alarm_level = request.GET.get("alarmLevel")
        alarm_type = request.GET.get("alarmType")
        try:
            result = {
                "code": "0",
                "ret": ERROR_MSG.get("0"),
                "message": []
            }
            with connections['tianjin'].cursor() as cursor:
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
                                t_alarm_history"""
                if alarm_level:
                    sql += """ WHERE alarm_level = {}""".format(alarm_level)
                if alarm_type:
                    sql += """ AND alarm_type = {}""".format(alarm_type)
                print(1111, sql)
                cursor.execute(sql)
                rows = cursor.fetchall()
                print(rows, len(rows))
            data_list = [list(row) for row in rows]
            json_list = []
            for i in data_list:
                json_dict = {}
                json_dict["alarm_id"] = i[0]
                json_dict["alarm_location"] = i[1]
                json_dict["alarm_system"] = i[2]
                json_dict["alarm_level"] = i[3]
                json_dict["alarm_type"] = i[4]
                json_dict["alarm_text"] = i[5]
                json_dict["happen_times"] = i[6]
                json_dict["happen_d_time"] = i[7]
                json_dict["end_d_time"] = i[8]
                json_list.append(json_dict)
            result["message"] = json_list
            return Response(result)
        except Exception as e:
            logger.error(traceback.format_exc())
            return Response({"code": "-100", "ret": ERROR_MSG.get("-100")})


from dwebsocket.decorators import accept_websocket, require_websocket


@accept_websocket
def test_websocket(request):
    if request.is_websocket():
        while 1:
            time.sleep(1)  ## 向前端发送时间
            dit = {
                'time': time.strftime('%Y.%m.%d %H:%M:%S', time.localtime(time.time()))
            }
            request.websocket.send(json.dumps(dit))
