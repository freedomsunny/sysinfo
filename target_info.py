# !/usr/bin/python
# -*- coding:utf-8 -*-

import sys
import time

import mysql.connector
import psutil

sys.path.append('/usr/')
from BCP.Common.Env.ConfigParameter import *


# 数据库连接
def Conn():
    try:
        conn = mysql.connector.connect(user=ConfigParameter.DBUser,
                                       password=ConfigParameter.DBPasswd,
                                       host=ConfigParameter.DBHost,
                                       database=ConfigParameter.DBName,
                                       port=ConfigParameter.DBPort)
        return conn
    except Exception, ex:
        return ex


# 获取挂载点UUID
def get_target_UUID():
    return ConfigParameter.Target_UUID


# 获取目标类型
def get_target_type():
    return ConfigParameter.Target_mount_type


# 获取挂载点名称
def get_target_mount_name():
    return ConfigParameter.Target_MountName


# 获取挂载点路径
def get_target_mount_path():
    return ConfigParameter.Target_mount_path


# 获取挂载点本地路径
def get_local_mount_path():
    return ConfigParameter.Local_mount_path


# 获取挂载点空间大小
def get_mount_point_size(Path):
    return psutil.disk_usage(Path)[0] / 1024 / 1024


# 获取挂载点可用空间大小
def get_mount_point_ValidSize(Path):
    return psutil.disk_usage(Path)[2] / 1024 / 1024


# 获取挂载点所在设备UUID
def get_mount_point_UUID():
    return ConfigParameter.Myuid


# 获取挂载点状态更新时间
def get_mounted_last_updatetime():
    last_update_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    return last_update_time


# 获取挂载点状态
def get_target_state(Path):
    try:
        open(Path + os.sep + '.test.file', 'w')
        return "running"
    except IOError:
        return "closed"
