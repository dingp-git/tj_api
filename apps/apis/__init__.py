# @Copyright(C), OldFive, 2020.
# @Date : 2021/3/11 0011 13:31:44
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

"""

# Standard library imports

# Third party imports
from fastapi import APIRouter, Depends
# Local application imports
from apps.apis.system_509.system_509 import system_509
from apps.apis.system_603.system_603 import system_603
from apps.apis.system_ipsy.system_ipsy import system_ipsy
from apps.apis.public.public import public

api = APIRouter()

api.include_router(system_603, prefix='/system_603', tags=['603相关指标接口'])
api.include_router(system_509, prefix='/system_509', tags=['509相关指标接口'])
api.include_router(system_ipsy, prefix="/system_ipsy", tags=['ip溯源相关指标接口'])
api.include_router(public, prefix='/public', tags=['公共接口'])
