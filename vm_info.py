# !/usr/bin/python
# -*- coding:utf-8 -*-

import sys
import time
import xml.etree.cElementTree as ET

import mysql.connector

sys.path.append('/usr/')
from BCP.Common.Env.LogExceptionHelp import *

try:
    import libvirt
except ImportError:
    libvirt = None


# 创建链接对象
def Libvirt_conn():
    try:
        conn = libvirt.open('qemu:///system')
    except Exception, ex:
        LogExceptionHelp.logException('create libvirt connect fail: %s' % ex)
        exit(1)
    else:
        if not conn:
            return 'Failed to open connection to qemu:///system'
        else:
            return conn


# 数据库连接对象
def Conn():
    try:
        dbconn = mysql.connector.connect(user=ConfigParameter.DBUser, password=ConfigParameter.DBPasswd,
                                         host=ConfigParameter.DBHost, database=ConfigParameter.DBName,
                                         port=ConfigParameter.DBPort)
        return dbconn
    except Exception, ex:
        return ex


# 数据库查询并返回值
def sql_query(querycommand, conn=Conn()):
    storge = []
    cursor = conn.cursor()
    try:
        cursor.execute(querycommand)
    except Exception, e:
        return e
    for data in cursor:
        storge.append(data)
    cursor.close()
    conn.close()
    return storge


# 获得所有domain_name
def get_all_domain(conn=Libvirt_conn()):
    all_domain = []
    domains = conn.listAllDomains(0)
    if len(domains) != 0:
        for domain in domains:
            all_domain.append(domain.name())
        return all_domain
    else:
        return None


# 获取虚拟机状态(Active/inActive)
def get_domain_state(domain_name, conn=Libvirt_conn()):
    try:
        domain_obj = conn.lookupByName(domain_name)
    except Exception, e:
        print str(e)
        return 'None'
    domain_state = domain_obj.isActive()
    if domain_state == 1:
        return 'running'
    else:
        return 'closed'


# 获取虚拟机磁盘使用状况
def get_domain_DiskUsage(domain_name, conn=Libvirt_conn()):
    domain_disk_size = 0
    try:
        domain_obj = conn.lookupByName(domain_name)
    except Exception, e:
        print str(e)
        return 'None'
    # 获取域的UID
    get_vmUUID_sql_cmd = "select uid from %s  where name = '%s' limit 1;" % (
        ConfigParameter.Mysql_vmInfo_tbName, domain_name)
    vmUUIDs = sql_query(get_vmUUID_sql_cmd)
    if not vmUUIDs:
        return 0
    else:
        for vmUUID in vmUUIDs:
            get_vmDisks_sql_cmd = "select note_file from %s where vm_uid = '%s';" % (
                ConfigParameter.Mysql_vmdisk_tbName, vmUUID[0])
            vmDisks = sql_query(get_vmDisks_sql_cmd)
            for vmDisk in vmDisks:
                domain_disk_size += int(domain_obj.blockInfo(vmDisk[0])[1]) / 1024 / 1024
    # unit MB
    return domain_disk_size


# 获得虚拟机的CPU使用率
def get_domain_cpuusage(domain_name, conn=Libvirt_conn()):
    Active_domain_cpuinfo_1 = {}
    Active_domain_cpuinfo_2 = {}
    try:
        domain_obj = conn.lookupByName(domain_name)
    except Exception, e:
        print str(e)
        return 'None'
    domain_state = domain_obj.isActive()
    if domain_state == 0:
        return 0
    else:
        def _getcputime(Dict):
            cpu_core_num = int(domain_obj.info()[3])
            cpu_time = int(domain_obj.info()[4])
            Dict['cpu_core'] = cpu_core_num
            Dict['cpu_time'] = cpu_time
    # 得到CPU时间的差值
    _getcputime(Active_domain_cpuinfo_1)
    time.sleep(1)
    _getcputime(Active_domain_cpuinfo_2)
    cpu_core_num = Active_domain_cpuinfo_1['cpu_core']
    cputime1 = Active_domain_cpuinfo_1['cpu_time']
    cputime2 = Active_domain_cpuinfo_2['cpu_time']
    cpu_time_diff = cputime2 - cputime1
    cpuusage = 100 * cpu_time_diff / float((1 * cpu_core_num * 1000000000))
    return cpuusage


# 获得虚拟机的内存使用率
def get_domain_memusage(domain_name, conn=Libvirt_conn()):
    try:
        domain_obj = conn.lookupByName(domain_name)
    except Exception, e:
        print str(e)
        return 'None'
    domain_state = domain_obj.isActive()
    if domain_state == 0:
        return 0
    else:
        memusage = int(domain_obj.memoryStats()['rss']) / 1024
    # unit MB
    return memusage


# 得到哪些无线网卡被虚拟机使用
def get_wireless_usage():
    conn = Libvirt_conn()
    domains = conn.listAllDomains(0)
    usage_card = set()
    for domain in domains:
        if domain.isActive():
            xml = domain.XMLDesc()
            root = ET.fromstring(xml)
            try:
                devices = root.findall('devices')[0]
                if devices.findall('hostdev'):
                    host_dev_node = devices.findall('hostdev')[0]
                    source = host_dev_node.findall('source')[0]
                    # 暂时不用
                    # vendor = source.find('vendor').attrib['id'][2:]
                    # product = source.find('product').attrib['id'][2:]
                    # bus = source.find('address').attrib['bus']
                    device = source.find('address').attrib['device']
                    usage_card.add(int(device))
            except Exception as e:
                LogExceptionHelp.logException("vm_info.py in line 136 msg: {}".format(e))
    return usage_card
