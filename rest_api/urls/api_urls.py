from __future__ import absolute_import
from django.urls import path
# from rest_api import api
from rest_api.api import api_9
from rest_api.api import api_6

urlpatterns = [
    # path('test_api/', api.test_api.as_view()),
    # 6x3
    # CorrelationRate  关联率
    path('getCorrelationRate/', api_6.GetCorrelationRate.as_view()),
    # Message
    path('dxFrontVsLoad/', api_6.DxFrontVsLoad.as_view()),
    path('cxFrontVsLoad/', api_6.CxFrontVsLoad.as_view()),
    # UpDown
    # path('indexVsIsp/', api_6.IndexVsIsp.as_view()),
    path('rateVsReqrsp/', api_6.RateVsReqrsp.as_view()),
    # CdrData
    # XL
    path('cdrCountVsCentre/', api_6.CdrCountVsCentre.as_view()),
    # 五码 
    path('codeVsCentre/', api_6.CodeVsCentre.as_view()),
    path('getTableData/', api_6.GetTableData.as_view()),
    # 告警
    path('getAlarmHistory/', api_6.GetAlarmHistory.as_view()),
    path('testWebsocket/', api_6.test_websocket),

    # 5x9
    # HiveDatabase
    path('databaseWarning/', api_9.DatabaseWarning.as_view()),
    path('loadingRateWarning/', api_9.LoadingRateWarning.as_view()),
    path('multipltData/', api_9.MultipltData.as_view()),
    path('singleData/', api_9.SingleData.as_view()),
    # TODO DEL
    path('totalDatabase/', api_9.TotalDatabase.as_view()),
    path('hiveDatabase/', api_9.HiveDatabase.as_view()),

    # LoadingRate
    path('loadingRate/', api_9.LoadingRate.as_view())

]
