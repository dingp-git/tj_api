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
项目基本配置
"""

# Standard library imports

# Third party imports
from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware
from loguru import logger
# Local application imports
from apps.config.sys_config import (prefix_api_path, isFormalSystem,
                                    API_DOC_DESC, API_DOC_TITLE,
                                    API_DOC_VERSION, LOG_CONF)
from apps.utils import tools

# 日志初始化
if isFormalSystem:
    logger.add(LOG_CONF['LOG_FORM_PATH'] + '_{time:YYYY-MM-DD}.log', rotation='00:00',
               retention=LOG_CONF['LOG_RETENTION'], level=LOG_CONF['LOG_LEVEL'], enqueue=True, encoding='utf8')

tools.print_logo()

# 判断是否展示接口文档
docs_url = (prefix_api_path + "/docs") if isFormalSystem is False else None
redoc_url = (prefix_api_path + "/redoc") if isFormalSystem is False else None
openapi_url = (prefix_api_path + "/openapi.json")

app = FastAPI(docs_url=docs_url, redoc_url=redoc_url, openapi_url=openapi_url,
              title=API_DOC_TITLE, description=API_DOC_DESC,
              version=API_DOC_VERSION)

# 跨域处理
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 添加接口
# from apps.route.route import api
from apps import apis

app.include_router(apis.api, prefix=prefix_api_path)

# 全局自定义异常处理
import apps.interceptor.global_exception_handler
import apps.interceptor.before_req
