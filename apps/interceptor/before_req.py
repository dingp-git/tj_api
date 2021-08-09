# @Copyright(C), OldFive, 2020.
# @Date : 2021/3/11 10:13:26
# @Author : OldFive
# @Version : 0.1
# @Description :
# @History :
# @Other:
#  ▒█████   ██▓    ▓█████▄   █████▒██▓ ██▒   █▓▓█████
# ▒██▒  ██▒▓██▒    ▒██▀ ██▌▓██   ▒▓██▒▓██░   █▒▓█   ▀
# ▒██░  ██▒▒██░    ░██   █▌▒████ ░▒██▒ ▓██  █▒░▒███
# ▒██   ██░▒██░    ░▓█▄   ▌░▓█▒  ░░██░  ▒██ █░░▒▓█  ▄
# ░ ████▓▒░░██████▒░▒████▓ ░▒█░   ░██░   ▒▀█░  ░▒████▒
# ░ ▒░▒░▒░ ░ ▒░▓  ░ ▒▒▓  ▒  ▒ ░   ░▓     ░ ▐░  ░░ ▒░ ░
#   ░ ▒ ▒░ ░ ░ ▒  ░ ░ ▒  ▒  ░      ▒ ░   ░ ░░   ░ ░  ░
# ░ ░ ░ ▒    ░ ░    ░ ░  ░  ░ ░    ▒ ░     ░░     ░
#     ░ ░      ░  ░   ░            ░        ░     ░  ░
#                   ░                      ░
#
"""
拦截器
"""

# Standard library imports
# Third party imports
from fastapi import Request
# Local application imports
from apps.app import app


from apps.utils.sys_access_log import sys_access_log


@app.middleware("http")
async def app_before_request(request: Request, call_next):
    # 访问日志记录 
    # (取消注释 [文件头的库导入也需打开] 则生成日志信息,但并不保存,需自行选择存储方式)
    # await sys_access_log(request)
    
    # 按要求拦截请求
    path = request.url.path
    await sys_access_log(request)
    # 文档接口不拦截
    if "/docs" == path or '/openapi.json' == path or '/redoc' == path:
        return await call_next(request)
    return await call_next(request)
