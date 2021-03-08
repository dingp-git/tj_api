#coding:utf-8
'''
Created on 2018年3月16日

@author: Jonny

    SMS_API.config
    ---------------------
    #配置文件
'''
import os
from pip._vendor.requests.auth import CONTENT_TYPE_FORM_URLENCODED
import datetime
import time
#from celery.schedules import crontab

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

class Config:
    # SECURITY WARNING: keep the secret key used in production secret!
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'kkx^_q1(fp3@$w78cs7dfz8^5z&==a7&+@3m-+g)c3&u-fq_02'
    
    # Django security setting, if your disable debug model, you should setting that
    ALLOWED_HOSTS = ['*']
    
    
    # Development env open this, when error occur display the full process track, Production disable it
    DEBUG = True

    # DEBUG, INFO, WARNING, ERROR, CRITICAL can set. See https://docs.djangoproject.com/en/1.10/topics/logging/
    LOG_LEVEL = 'DEBUG'
    LOG_DIR = os.path.join(BASE_DIR, 'logs')

    # Database setting, Support sqlite3, mysql, postgres ....
    # See https://docs.djangoproject.com/en/1.10/ref/settings/#databases

    #Project DataBase configuration
    MYDB = {
        # docker 数据库配置
        'local':{
            'ENGINE': 'django.db.backends.sqlite3',
            'NAME': os.path.join(BASE_DIR, 'db.sqlite3'),
            
        },
        'tianjin_1':{
            'ENGINE': 'django.db.backends.mysql',
            'NAME': 'tianjin',
            'USER': 'root',
            'PASSWORD': 'root',
            'HOST': '172.27.1.12',
            # 'USER': 'tianjin',
            # 'PASSWORD': 'tianjin',
            # 'HOST': 'DB',
            # 'HOST': '127.0.0.1',
            'PORT': '3306',
        },
    }
    
    # When Django start it will bind this host and port
    # ./manage.py runserver 127.0.0.1:8080
    HTTP_BIND_HOST = '0.0.0.0'
    HTTP_LISTEN_PORT = 80
    # APPEND_SLASH=False

    
    # SQLite Setting:
    #DB_ENGINE = 'sqlite3'
    #DB_NAME = os.path.join(BASE_DIR, 'data', 'db.sqlite3')
    
    
    def __init__(self):
        pass

    def __getattr__(self, item):
        return None

class DevelopmentConfig(Config):
    pass

class ProductionConfig(Config):
    pass

#
config = DevelopmentConfig()
