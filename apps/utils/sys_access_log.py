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
系统访问日志记录
"""

# Standard library imports

# Third party imports
from fastapi import Request
from loguru import logger
from starlette.requests import Message


# Local application imports
from apps.config import sys_config


async def set_body(request: Request, body: bytes):
    async def receive() -> Message:
        return {"type": "http.request", "body": body}

    request._receive = receive


# async def get_body(request: Request) -> bytes:
#     body = await request.body()
#     await set_body(request, body)
#     return body


async def sys_access_log(request: Request):
    """系统访问日志记录
    """
    _body = await request.body()
    # 获取真实的 ip (可能存在 nginx 等方式的代理)
    ip = request.client.host
    __x_forwarded_for = request.headers.getlist("X-Forwarded-For") or []
    __x_real_ip = request.headers.getlist("X-Real-Ip") or None
    if __x_forwarded_for:
        ip = __x_forwarded_for[0]
    elif __x_real_ip:
        ip = __x_real_ip

    req_log_dict = {
        'uri': request.url.path,
        'method': request.method,
        'ip': ip,
        'url': request.url.components.geturl(),
        'query_params': request.query_params._dict,
        'path_params': request.path_params,
        'headers': request.headers.items(),
    }
    try:
        # 字符类参数可以进行编码存储
        req_log_dict['body'] = _body.decode()
    except Exception as e:
        logger.error(e)
        req_log_dict['body'] = f"** It could be a byte file ({e}) **"

    if sys_config.isFormalSystem:
        logger.info(req_log_dict)

    await set_body(request, _body)
