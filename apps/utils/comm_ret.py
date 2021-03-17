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
response 统一返回封装
"""

# Standard library imports
from datetime import datetime, date
# Third party imports
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
# Local application imports
from apps.utils.resp_code import SUCCESS


def comm_ret(code: int = SUCCESS, isSuccess: bool = True,
                msg: str = "请求成功", data: object = {}):
    """
    接口统一返回模板
        @param:
            code:       http状态码      int     默认 200
            isSuccess:  请求成功状态     bool    默认 True
            msg:        描述            str     默认 请求成功
            data:       返回的数据结果集  object  默认 {}
        @return:
            return jsonify response
    """
    ret_json = {
        "code": code,
        "isSuccess": isSuccess,
        "msg": msg,
        "data": data
    }
    return JSONResponse(content=jsonable_encoder(
        ret_json,
        custom_encoder={
            datetime: lambda v: v.strftime("%Y-%m-%d %H:%M:%S"),
            date: lambda v: v.strftime("%Y-%m-%d"),
        }
    ))
