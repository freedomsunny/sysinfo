# -*- coding:utf-8 -*-

import commands
import re
import sys
import time
import uuid

import mysql.connector
import psutil

sys.path.append('/usr/')
from BCP.Common.Env.ConfigParameter import ConfigParameter
from BCP.Common.Env.DBNames import DBNames
from BCP.Common.Env.LogExceptionHelp import LogExceptionHelp


# 连接数据库
def Conn():
    try:
        conn = mysql.connector.connect(user=ConfigParameter.DBUser,
                                       password=ConfigParameter.DBPasswd,
                                       host=ConfigParameter.DBHost,
                                       database=ConfigParameter.DBName,
                                       port=ConfigParameter.DBPort)
        return conn
    except Exception as ex:
        LogExceptionHelp.logException(str(ex))
        return None


# 获取设备名
def get_phy_Name():
    return ConfigParameter.MyName


# 获取设备UUID
def get_phy_UUID():
    return ConfigParameter.Myuid


# 获取主机的UUID
def get_host_UUID():
    return str(uuid.uuid1())


# 获取设备类型
def get_phy_Type():
    return ConfigParameter.MyType


# 获取cpu的个数
def get_cpu_num():
    return psutil.cpu_count()


# 获取内存总量
def get_mem_size():
    return psutil.virtual_memory().total / 1024 / 1024


# 获取已挂载分区的磁盘总量,包括本地及远程挂载目录
def get_mounted_disk_size(path='/mnt'):
    return psutil.disk_usage(path).total / 1024 / 1024


# def get_mounted_disk_size():
#     disk_total = 0
#     all_disks = []
#     for Local in psutil.disk_partitions():
#         all_disks.append(Local.device)
#         if all_disks.count(Local.device) <= 1:
#             disk_total += psutil.disk_usage(Local.mountpoint).total / 1024 / 1024
#         else:
#             continue
#     # unit MB
#     return disk_total


# 获取CPU的使用率
def get_cpu_usage():
    return psutil.cpu_percent(interval=1)


# 获取当前内存使用量
def get_current_ram_usage():
    # unit MB
    # 实际可用内存（free + buffers + cached - shared - 100MB）,预留100MB给系统和其他进程
    available_size = (psutil.virtual_memory().free + psutil.virtual_memory().buffers +
                      psutil.virtual_memory().cached - psutil.virtual_memory().shared) / 1024 / 1024 - 100
    return psutil.virtual_memory().total / 1024 / 1024 - available_size


# 获取当前已挂载磁盘的使用量
def get_disk_usage_size():
    disk_usage_total = 0
    for Local in psutil.disk_partitions():
        disk_usage_total += psutil.disk_usage(Local[1])[1] / 1024 / 1024
    # unit MB
    return disk_usage_total


# 获取网卡信息
def get_InterFace_flow():
    InterFaces_Flow_befor = {}
    InterFaces_Flow_after = {}
    InterFaces_Flow_result = {}
    all_cards = psutil.net_if_addrs().keys()

    def _get_InterFace_flow(key):
        for i in ConfigParameter.Interfaces_type:
            if i in all_cards:
                TX = psutil.net_io_counters(pernic=True)[i][0] / 1024
                RX = psutil.net_io_counters(pernic=True)[i][1] / 1024
                key[i] = {"Outbond": TX, 'Inbond': RX}
            else:
                continue

    _get_InterFace_flow(InterFaces_Flow_befor)
    time.sleep(1)
    _get_InterFace_flow(InterFaces_Flow_after)
    # 网卡当前的流量
    for i in ConfigParameter.Interfaces_type:
        if i in all_cards:
            TX_result = InterFaces_Flow_after[i]['Outbond'] - InterFaces_Flow_befor[i]['Outbond']
            RX_result = InterFaces_Flow_after[i]['Inbond'] - InterFaces_Flow_befor[i]['Inbond']
            # 网卡ip,MAC,role,inbond/bound流量,网卡没有配置IPv4地址时或网卡为down状态时为None
            if_mac = [x[1] for x in psutil.net_if_addrs()[i] if x[0] == 17]
            if_ip = [x[1] for x in psutil.net_if_addrs()[i] if x[0] == 2]
            IF_IP = ''.join(if_ip) if if_ip else 'None'
            IF_MAC = ''.join(if_mac) if if_mac else 'None'
            IF_Role = ConfigParameter.Interfaces_type[i]
            InterFaces_Flow_result[i] = {"Ipaddr": IF_IP, "Macaddr": IF_MAC, 'Role': IF_Role, 'Outbond': TX_result,
                                         'Inbond': RX_result}
        else:
            continue
    # unit KB
    return InterFaces_Flow_result


# 获取主机状态
def get_host_state():
    sql_query_command = "select unix_timestamp(last_update_time) from %s where uid = '%s'" % (
        ConfigParameter.Mysql_phyInfo_tbName, ConfigParameter.Myuid)
    # 得到当前的时间戳
    cur_timestamp = time.mktime(time.localtime())
    # 开始连接数据库并获取值
    phy_dbconn = Conn()
    phy_cursor = phy_dbconn.cursor()
    phy_cursor.execute(sql_query_command)
    result = phy_cursor.fetchall()
    if result:
        for i in result:
            last_update_timestamp = i[0]
        if last_update_timestamp is None or \
                cur_timestamp - last_update_timestamp > ConfigParameter.Check_service_failure_time:
            return "closed"
        elif cur_timestamp - last_update_timestamp < ConfigParameter.Check_service_failure_time:
            return "running"
    else:
        return "closed"
    phy_cursor.close()
    phy_dbconn.close()


def update_host_state():
    sql_query_command = "UPDATE %s SET physical_state='closed'  where uid = '%s'" % (
        ConfigParameter.Mysql_phyInfo_tbName, ConfigParameter.Myuid)
    print sql_query_command
    tg_conn = Conn()
    cursor = tg_conn.cursor()

    try:
        cursor.execute(sql_query_command)
        tg_conn.commit()

    except Exception, e:
        print str(e)
    finally:
        cursor.close()
        tg_conn.close()
    return True


# 最后更新时间(当前时间)
def get_host_lastuptime():
    last_update_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    return last_update_time


# 获取所有无线网卡（本机）
def get_wireless_phy():
    cmd = '/usr/bin/lsusb | grep Wireless'
    status, resp = commands.getstatusoutput(cmd)
    if status:
        return {}

    result = {}
    pattern = re.compile(r"Bus (\S+) Device (\S+): ID (\S+):(\S+) .* ")
    ret = resp.split('\n')
    for line in ret:
        ret = pattern.match(line)
        bus = int(ret.group(1))
        device = int(ret.group(2))
        vendor = '0x' + ret.group(3)
        product = '0x' + ret.group(4)
        result[device] = {'Bus': bus,
                          'Device': device,
                          'Vendor': vendor,
                          'Product': product}
    return result


def get_wireless_phy2():
    all_cards = psutil.net_if_addrs()
    wireless_cards = {}
    if not all_cards:
        return {}

    for k, v in all_cards.items():
        if k.startswith('wlan'):
            if_mac = [x.address for x in v if x.family == 17]
            wireless_cards[k] = ''.join(if_mac)

    return wireless_cards


# 获取所有无线网卡（数据库）
def get_wireless_db():
    db_cards = set()
    conn = Conn()
    cursor = conn.cursor()
    cmd = "select device from %s WHERE physical_uid = '%s'" % \
          (DBNames.PhysicalWifiCardTableName, ConfigParameter.Myuid)
    try:
        cursor.execute(cmd)
        result = cursor.fetchall()
        if result:
            for i in result:
                db_cards.add(int(i[0]))
    except Exception, e:
        print e
    finally:
        if cursor:
            cursor.close()
        conn.close()
        return db_cards
