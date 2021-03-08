'''
Created on 2020

@author: 

'''
from rest_framework.views import APIView
from rest_framework.response import Response
from django.http.response import JsonResponse
from django.db import connections, connection
from .utils import get_logger

logger = get_logger("test")


class test_api(APIView):
    def get(self, request):
        with connections['tianjin'].cursor() as cursor:
            sql = "select * from 509hive_db"
            cursor.execute(sql)
            rows = cursor.fetchall()
        logger.error("maybe is bugger")
        return Response({'message': rows, 'code': '0'})
