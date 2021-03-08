from rest_framework.views import APIView
from rest_framework.response import Response
from django.http.response import JsonResponse
from django.db import connections, connection
from ..utils import  get_logger
from ..GlobalParam import *
from config import *
import traceback

logger = get_logger("CdrData")

# XL
class CdrCountVsCentre(APIView):
    # 无参数，查询cdr_count列数据，仅有长安数据
    def get(self,request):
        # 接口测试数据
        FORMATE_NOW_DATE = 20201101
        try:
            result = {
                "code":"0",
                "ret":ERROR_MSG.get("0"),
                "message":[]
            }
            with connections['tianjin'].cursor() as cursor:
                sql = """
                    SELECT
                        cdr_count,
                        stat_time 
                    FROM
                        cdr_qua_stat_{};""".format(FORMATE_NOW_DATE)
                cursor.execute(sql)
                rows = cursor.fetchall()
            print(sql)
            data_list = [list(row) for row in rows]
            json_list = []
            for i in data_list:
                json_dict = {}
                json_dict['cdr_count'] =i[0]
                json_dict['stat_time'] = i[1]
                json_list.append(json_dict)
            # RESULT["message"] = json_list
            # return Response(RESULT)
            result["message"] = json_list
            return Response(result)
        except Exception as e:
            # logger.error(e)
            logger.error(traceback.format_exc())
            return Response({"code":"-100","ret":ERROR_MSG.get("-100")})

    # 参数cdr类型，查询指定cdr类型的一天数据，若参数为空，则默认返回全部cdr类型数据
    def post(self,request):
        cdr_type = request.POST.get("cdrType")
        # 接口测试数据
        FORMATE_NOW_DATE = 20201101
        DEC_DATE_TIME = "2020-11-01 00:00:00"
        NOW_DATE_TIME = "2020-11-01 18:00:00"
        try:
            result = {
                "code":"0",
                "ret":ERROR_MSG.get("0"),
                "message":[]
            }
            with connections['tianjin'].cursor() as cursor:
                sql = """
                    SELECT
                        cdr_type,
                        stat_time 
                    FROM
                        cdr_qua_stat_{} 
                """.format(FORMATE_NOW_DATE)
                if cdr_type:
                    sql += " WHERE cdr_type = '{}' AND stat_time BETWEEN '{}' AND '{}';".format(cdr_type,DEC_DATE_TIME,NOW_DATE_TIME)
                else:
                    sql += " WHERE stat_time BETWEEN '{}' AND '{}';".format(DEC_DATE_TIME,NOW_DATE_TIME)
                cursor.execute(sql)
                rows = cursor.fetchall()
            print(22222222222,sql)
            data_list = [list(row) for row in rows]
            json_list = []  
            for j in data_list:
                json_dict = {}
                json_dict['cdr_count'] =j[0]
                json_dict['stat_time'] = j[1]
                json_list.append(json_dict)
            result["message"] = json_list
            return Response(result)
        except Exception as e:
            # logger.error(e)
            logger.error(traceback.format_exc())
            return Response({"code":"-100","ret":ERROR_MSG.get("-100")})



# 五码
class CodeVsCentre(APIView):
    # 默认1小时数据，截止日期参数，指标-运营商-比值
    def get(self,request):
        # 接口测试数据
        # HOUR_DATE_TIME = "2020-12-01 13:10:00"
        # NOW_DATE_TIME = "2020-12-02 06:10:00"
        deadline = request.GET.get("deadline")
        if deadline is "" or deadline is None:
            deadline = NOW_DATE_TIME
        try:
            result = {
                "code":"0",
                "ret":ERROR_MSG.get("0"),
                "message":[]
            }
            str_to_date = datetime.datetime.strptime(deadline,"%Y-%m-%d %H:%M:%S")
            hour_date_time = (str_to_date + datetime.timedelta(minutes=-60)).strftime("%Y-%m-%d %H:%M:%S")
            with connections['tianjin'].cursor() as cursor:
                sql = """
                    SELECT
                        FORMAT( SUM( imsi_count )/ SUM( cdr_count )* 100, 2 ) AS imsi,
                        FORMAT( SUM( user_num_count )/ SUM( cdr_count )* 100, 2 ) AS user_num,
                        FORMAT( SUM( imei_count )/ SUM( cdr_count )* 100, 2 ) AS imei,
                        FORMAT( SUM( areacode_count )/ SUM( cdr_count )* 100, 2 ) AS areacode,
                        FORMAT( SUM( uli_count )/ SUM( cdr_count )* 100, 2 ) AS uli,
                        isp,
                        stat_time
                    FROM
                        cdr_qua_stat
                    WHERE 
                        stat_time BETWEEN "{}" AND "{}"
                    GROUP BY
                        isp,
                        stat_time;""".format(hour_date_time,deadline)
                cursor.execute(sql)
                rows = cursor.fetchall()
            print(sql)
            data_list = [list(row) for row in rows]
            data_dict = {}
            for i in data_list:
                data_dict.setdefault("imsi",{})[i[5]] = i[0]
                data_dict.setdefault("user_num",{})[i[5]] = i[1]
                data_dict.setdefault("imei",{})[i[5]] = i[2]
                data_dict.setdefault("areacode",{})[i[5]] = i[3]
                data_dict.setdefault("uli",{})[i[5]] = i[4]
            result["message"] = data_dict
            return Response(result)
        except Exception as e:
            logger.error(traceback.format_exc())
            return Response({"code":"-100","ret":ERROR_MSG.get("-100")})
    
    # 二级弹窗 时间范围参数可选，默认7天，必填参数isp,protocol
    def post(self,request):
        start_time = request.data.get('startTime',WEEK_DATE_TIME)
        end_time = request.data.get('endTime',NOW_DATE_TIME)
        isp = request.data.get('isp')
        protocol = request.data.get('protocol')
        if isp is None or protocol is None:
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
                        FORMAT( SUM( {0}_count )/ SUM( cdr_count )* 100, 2 ) AS {0},
                        isp,
                        stat_time 
                    FROM
                        cdr_qua_stat
                    WHERE stat_time BETWEEN "{1}" AND "{2}"
                    GROUP BY
                        isp,
                        stat_time order by stat_time limit 30;""".format(protocol,start_time,end_time)
                cursor.execute(sql)
                rows = cursor.fetchall()
                print(sql)
            data_list = [list(row) for row in rows]
            # print(sql,"*********",len(rows))
            json_list = []
            json_dict = {}
            flag = 1
            for i in data_list:
                json_dict.setdefault('stat_time')
                if i[2] == json_dict['stat_time']:
                    json_dict[i[1]] = i[0]
                    flag = 0
                else:
                    json_dict = {}
                    json_dict['stat_time'] = i[2]
                    json_dict[i[1]] = i[0]
                    flag = 1
                if flag:
                    json_list.append(json_dict)
            result["message"] = json_list
            return Response(result)
        except Exception as e:
            logger.error(traceback.format_exc())
            return Response({"code":"-100","ret":ERROR_MSG.get("-100")})


# 获取cdr_qua_stat表数据  DELETE
class GetTableData(APIView):
    # 默认当前时间一天内数据 
    def get(self, request):
        # 接口测试数据
        # DEC_DATE_TIME = "2020-11-01 00:00:00"
        # NOW_DATE_TIME = "2020-11-01 17:00:00"
        # FORMATE_NOW_DATE = "20201101"
        with connections['tianjin'].cursor() as cursor:
            sql = """
                SELECT
                    cdr_type,
                    net_type,
                    cdr_count,
                    imsi_count,
                    user_num_count,
                    imsi_miss_rate,
                    user_miss_rate,
                    imei_count,
                    areacode_count,
                    uli_count,
                    imei_miss_rate,
                    areacode_miss_rate,
                    uli_miss_rate,
                    isp,
                    server_ip,
                    stat_time 
                FROM
                    cdr_qua_stat_{} 
                WHERE
                    (cdr_count <> 0 
                    OR imsi_count <> 0 
                    OR user_num_count <> 0 
                    OR imsi_miss_rate IS NOT NULL 
                    OR user_miss_rate IS NOT NULL 
                    OR imei_count <> 0 
                    OR areacode_count <> 0 
                    OR uli_count <> 0 
                    OR imei_miss_rate IS NOT NULL 
                    OR areacode_miss_rate IS NOT NULL 
                    OR uli_miss_rate IS NOT NULL)
                    AND stat_time BETWEEN '{}' AND '{}';""".format(FORMATE_NOW_DATE,DEC_DATE_TIME,NOW_DATE_TIME)
            cursor.execute(sql)
            rows = cursor.fetchall()
            print(sql,len(rows))
        data_list = [list(row) for row in rows]
        json_list = []  
        for j in data_list:
            json_dict = {}
            json_dict['cdr类型'] =j[0]
            json_dict['网络类型'] = j[1]
            json_dict['cdr总数'] = j[2]
            json_dict['含imsi总数'] =j[3]
            json_dict['含user_num总数'] = j[4]
            json_dict['imsi_miss_rate'] = j[5]
            json_dict['user_miss_rate'] = j[6]
            json_dict['imei总数'] = j[7]
            json_dict['areacode总数'] = j[8]
            json_dict['uli总数'] = j[9]
            json_dict['imei_miss_rate'] = j[10]
            json_dict['areacode_miss_rate'] = j[11]
            json_dict['uli_miss_rate'] = j[12]
            json_dict['运营商'] = j[13]
            json_dict['服务器ip'] = j[14]
            json_dict['统计时间'] = j[15]
            json_list.append(json_dict)

        return Response({'message': json_list, 'code': '0'})

    def post(self, request):
        isp = request.data.get("isp")
        start_time = request.data.get("startTime",DEC_DATE_TIME)
        end_time = request.data.get("endTime",NOW_DATE_TIME)
        # 接口测试数据
        # isp = "1"
        # start_time = "2020-11-01 00:00:00"
        # end_time = "2020-11-01 18:00:00"
        # FORMATE_NOW_DATE = "20201101"
        with connections['tianjin'].cursor() as cursor:
            sql = """
                SELECT
                    cdr_type,
                    net_type,
                    cdr_count,
                    imsi_count,
                    user_num_count,
                    imsi_miss_rate,
                    user_miss_rate,
                    imei_count,
                    areacode_count,
                    uli_count,
                    imei_miss_rate,
                    areacode_miss_rate,
                    uli_miss_rate,
                    isp,
                    server_ip,
                    stat_time 
                FROM
                    cdr_qua_stat_{} 
                WHERE 
                    (cdr_count <> 0 
                    OR imsi_count <> 0 
                    OR user_num_count <> 0 
                    OR imsi_miss_rate IS NOT NULL 
                    OR user_miss_rate IS NOT NULL 
                    OR imei_count <> 0 
                    OR areacode_count <> 0 
                    OR uli_count <> 0 
                    OR imei_miss_rate IS NOT NULL 
                    OR areacode_miss_rate IS NOT NULL 
                    OR uli_miss_rate IS NOT NULL) """.format(FORMATE_NOW_DATE)
            if isp:
                sql += "AND isp = '{}' AND stat_time BETWEEN '{}' AND '{}';".format(isp,start_time,end_time)
            else:
                sql += "AND stat_time BETWEEN '{}' AND '{}';".format(start_time,end_time)
 
            cursor.execute(sql)
            rows = cursor.fetchall()
            print(sql,len(rows))
        data_list = [list(row) for row in rows]
        json_list = []  
        for j in data_list:
            json_dict = {}
            json_dict['cdr类型'] =j[0]
            json_dict['网络类型'] = j[1]
            json_dict['cdr总数'] = j[2]
            json_dict['含imsi总数'] =j[3]
            json_dict['含user_num总数'] = j[4]
            json_dict['imsi_miss_rate'] = j[5]
            json_dict['user_miss_rate'] = j[6]
            json_dict['imei总数'] = j[7]
            json_dict['areacode总数'] = j[8]
            json_dict['uli总数'] = j[9]
            json_dict['imei_miss_rate'] = j[10]
            json_dict['areacode_miss_rate'] = j[11]
            json_dict['uli_miss_rate'] = j[12]
            json_dict['运营商'] = j[13]
            json_dict['服务器ip'] = j[14]
            json_dict['统计时间'] = j[15]
            json_list.append(json_dict)

        return Response({'message': json_list, 'code': '0'})
