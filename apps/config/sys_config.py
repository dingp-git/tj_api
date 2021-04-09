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
系统配置
"""

# ********** 运行配置 ********** #
# 基本运行配置
app_run_conf = {
    "HOST": "0.0.0.0",
    "PORT": 5000,
    "RELOAD": True,
    "WORKERS": 1,
    "DEBUG": True
}

SECRET_KEY = "xxx"

# 日志相关配置
LOG_CONF = {
    'LOG_FORM_PATH': './log/runtime',  # 日志存储路径
    'LOG_RETENTION': '7 days',         # 日志存储天数
    'LOG_LEVEL': 'DEBUG',               # 日志等级
    # 'LOG_LEVEL': 'ERROR',              # 日志等级
}

# ********** 生产 与 测试 系统切换 ********** #
# True : 生产系统
# False: 测试系统
# isFormalSystem = False
isFormalSystem = True

# 接口前缀及版本控制
__version = "v1.0"
prefix_api_path = "/api/{version}".format(version=__version)

# api 文档描述
API_DOC_TITLE = "tj项目api文档"
API_DOC_DESC = ""
API_DOC_VERSION = __version
