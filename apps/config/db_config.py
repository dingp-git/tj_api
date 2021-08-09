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
数据库配置
"""

# Standard library imports

# Third party imports
import pymysql
# Local application imports


# ********** 数据库配置 ********** #
# mysql 数据库
MYSQL_CONF = {  # 生产系统使用
    'DB_TEST_HOST': '10.238.249.33',
    'DB_TEST_PORT': 3306,
    'DB_TEST_DBNAME': 'tianjin',
    'DB_TEST_USER': 'root',
    'DB_TEST_PASSWORD': 'root',
    'DB_CHARSET': "utf8mb4",     # 数据库连接编码
    'DB_MIN_CACHED': 10,         # mincached : 启动时开启的闲置连接数量(缺省值 0 开始时不创建连接)
    'DB_MAX_CACHED': 10,         # maxcached : 连接池中允许的闲置的最多连接数量(缺省值 0 代表不闲置连接池大小)
    'DB_MAX_SHARED': 20,         # maxshared : 共享连接数允许的最大数量(缺省值 0 代表所有连接都是专用的)如果达到了最大数量,被请求为共享的连接将会被共享使用
    'DB_MAX_CONNECYIONS': 100,   # maxconnecyions : 创建连接池的最大数量(缺省值 0 代表不限制)
    'DB_BLOCKING': True,         # blocking : 设置在连接池达到最大数量时的行为(缺省值 0 或 False 代表返回一个错误<toMany......> 其他代表阻塞直到连接数减少,连接被分配)
    'DB_MAX_USAGE': 0,           # maxusage : 单个连接的最大允许复用次数(缺省值 0 或 False 代表不限制的复用).当达到最大数时,连接会自动重新连接(关闭和重新打开)
    'DB_SET_SESSION': None,      # setsession : 一个可选的SQL命令列表用于准备每个会话，如["set datestyle to german", ...]
    'DB_CREATOR': pymysql,       # creator : 使用连接数据库的模块
}
MYSQL_CONF_T = {  # 测试系统使用
    'DB_TEST_HOST': '1.62.188.204',
    'DB_TEST_PORT': 3306,
    'DB_TEST_DBNAME': 'tianjin',
    'DB_TEST_USER': 'root',
    'DB_TEST_PASSWORD': 'Hlcert@123',
    # 'DB_TEST_HOST': '172.27.1.12',
    # 'DB_TEST_PORT': 3306,
    # 'DB_TEST_DBNAME': 'tianjin',
    # 'DB_TEST_USER': 'root',
    # 'DB_TEST_PASSWORD': 'root',
    'DB_CHARSET': "utf8mb4",        # 数据库连接编码
    'DB_MIN_CACHED': 10,         # mincached : 启动时开启的闲置连接数量(缺省值 0 开始时不创建连接)
    'DB_MAX_CACHED': 10,         # maxcached : 连接池中允许的闲置的最多连接数量(缺省值 0 代表不闲置连接池大小)
    'DB_MAX_SHARED': 20,         # maxshared : 共享连接数允许的最大数量(缺省值 0 代表所有连接都是专用的)如果达到了最大数量,被请求为共享的连接将会被共享使用
    'DB_MAX_CONNECYIONS': 100,   # maxconnecyions : 创建连接池的最大数量(缺省值 0 代表不限制)
    'DB_BLOCKING': True,         # blocking : 设置在连接池达到最大数量时的行为(缺省值 0 或 False 代表返回一个错误<toMany......> 其他代表阻塞直到连接数减少,连接被分配)
    'DB_MAX_USAGE': 0,           # maxusage : 单个连接的最大允许复用次数(缺省值 0 或 False 代表不限制的复用).当达到最大数时,连接会自动重新连接(关闭和重新打开)
    'DB_SET_SESSION': None,      # setsession : 一个可选的SQL命令列表用于准备每个会话，如["set datestyle to german", ...]
    'DB_CREATOR': pymysql,       # creator : 使用连接数据库的模块
}

# redis 数据库
REDIS_CONF = {                   # 生产系统使用
    "HOST": "127.0.0.1",
    "PORT": 6379,
    "AUTH": False,               # AUTH 为 True 时需要进行 用户认证
    "PASSWORD": "xxx",
    "DECODE_RESPONSES": True     # 是否对查询结果进行编码处理
}
REDIS_CONF_T = {                 # 测试系统使用
    "HOST": "127.0.0.1",
    "PORT": 6379,
    "AUTH": False,               # AUTH 为 True 时需要进行 用户认证
    "PASSWORD": "xxx",
    "DECODE_RESPONSES": True     # 是否对查询结果进行编码处理
}
