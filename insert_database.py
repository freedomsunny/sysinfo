# !/usr/bin/python
# -*- coding:utf-8 -*-
from threading import Thread

import mysql.connector

from physcal_info import *
from target_info import *
from vm_info import *

sys.path.append('/usr/')
from BCP.Common.Env.ConfigParameter import ConfigParameter
from BCP.Common.Env.DBNames import DBNames


# 返回libvirt连接对象
def Libvirt_conn():
    try:
        conn = libvirt.open('qemu:///system')
    except Exception as ex:
        LogExceptionHelp.logException("get libivrt connection fail: %s" % ex)
        exit(1)
    else:
        if not conn:
            return 'Failed to open connection to qemu:///system'
        else:
            return conn


# 数据库连接对象
def Conn():
    try:
        dbconn = mysql.connector.connect(user=ConfigParameter.DBUser,
                                         password=ConfigParameter.DBPasswd,
                                         host=ConfigParameter.DBHost,
                                         database=ConfigParameter.DBName,
                                         port=ConfigParameter.DBPort)
        return dbconn
    except Exception, e:
        LogExceptionHelp.logException("generate db connection fail: %s" % e)
        return None


# 首次运行删除相关表数据
def delete_dada():
    del_conn = Conn()
    cursor = del_conn.cursor()
    del_phyinfo = 'delete from %s;' % ConfigParameter.Mysql_phyInfo_tbName
    del_phycard = 'delete from %s;' % ConfigParameter.Mysql_nicInfo_tbName
    try:
        cursor.execute(del_phyinfo)
        cursor.execute(del_phycard)
        del_conn.commit()
    except Exception, e:
        print str(e)
        LogExceptionHelp.logException(e)
    finally:
        if cursor:
            cursor.close()
        del_conn.close()


# 物理主机信息
def insert_phy_info():
    # 获取主机相关信息
    phy_UUID = str(get_phy_UUID())
    phy_name = str(get_phy_Name())
    phy_type = str(get_phy_Type())
    phy_cpu_num = str(get_cpu_num())
    phy_mem_size = str(get_mem_size())
    phy_mounted_disk_size = str(get_mounted_disk_size())
    phy_cpu_usage = str(get_cpu_usage())
    phy_mem_usage = str(get_current_ram_usage())
    phy_disk_usage = str(get_disk_usage_size())
    phy_state = str(get_host_state())
    phy_last_Updatetime = str(get_host_lastuptime())
    # 开始连接数据库并插入数据
    phy_conn = Conn()
    cursor = phy_conn.cursor()
    # 如果存在则插入数据，不存在则更新
    phy_sql_command = "INSERT INTO %s (uid,name,type,cpu,ram,disk_size,current_cpu," \
                      "current_ram,current_disk_size,physical_state,last_update_time) " \
                      "VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')" % \
                      (ConfigParameter.Mysql_phyInfo_tbName, phy_UUID, phy_name, phy_type,
                       phy_cpu_num, phy_mem_size, phy_mounted_disk_size, phy_cpu_usage,
                       phy_mem_usage, phy_disk_usage, phy_state, phy_last_Updatetime) + \
                      "ON DUPLICATE KEY UPDATE name='%s',type='%s',cpu='%s',ram='%s'," \
                      "disk_size='%s',current_cpu='%s',current_ram='%s',current_disk_size='%s'," \
                      "physical_state='%s',last_update_time='%s'" % \
                      (phy_name, phy_type, phy_cpu_num, phy_mem_size, phy_mounted_disk_size,
                       phy_cpu_usage, phy_mem_usage, phy_disk_usage, phy_state, phy_last_Updatetime)
    try:
        cursor.execute(phy_sql_command)
        phy_conn.commit()
    except Exception, e:
        print e
        LogExceptionHelp.logException("in insert_database.py line 62 "
                                      ": insert_phy_info error {}".format(e))
    finally:
        if cursor:
            cursor.close()
        phy_conn.close()


# 宿主机物理网卡信息
def insert_phy_card():
    UUID = str(get_phy_UUID())
    phy_cards_info = get_InterFace_flow()
    phy_cards = phy_cards_info.keys()
    for card in phy_cards:
        phy_card_type = str(phy_cards_info[card]['Role'])
        phy_card_ip = str(phy_cards_info[card]['Ipaddr'])
        phy_card_mac = str(phy_cards_info[card]['Macaddr'])
        phy_card_flow_inbond = phy_cards_info[card]['Inbond']
        phy_card_flow_outbond = phy_cards_info[card]['Outbond']
        phy_card_flow_total = phy_card_flow_inbond + phy_card_flow_outbond
        pyth_uuid = str(get_host_UUID())
        # 开始连接数据库
        card_conn = Conn()
        cursor = card_conn.cursor()
        # 定义sql语句
        check_command = "select name from %s where name = '%s' and physical_uid='%s' ;" % (
            ConfigParameter.Mysql_nicInfo_tbName, card, UUID)
        insert_cmd = "insert into %s (uid,name,card_type,ip,mac,current_flow,current_in_flow," \
                     "current_out_flow,physical_uid) values ('%s','%s','%s','%s','%s',%.2f,%.2f,%.2f,'%s')" % \
                     (ConfigParameter.Mysql_nicInfo_tbName, pyth_uuid, card, phy_card_type,
                      phy_card_ip, phy_card_mac, phy_card_flow_total, phy_card_flow_inbond,
                      phy_card_flow_outbond, UUID)
        update_cmd = "UPDATE %s set name='%s',card_type='%s',ip='%s',mac='%s',current_flow=%.2f," \
                     "current_in_flow=%.2f,current_out_flow=%.2f,physical_uid='%s' where name='%s'" \
                     " and physical_uid='%s'" % \
                     (ConfigParameter.Mysql_nicInfo_tbName, card, phy_card_type, phy_card_ip, phy_card_mac,
                      phy_card_flow_total, phy_card_flow_inbond, phy_card_flow_outbond, UUID, card, UUID)
        try:
            cursor.execute(check_command)
            result = cursor.fetchall()
        except Exception, e:
            return e
        # 检查记录是否存在，存在则更新，不存在则插入
        if result:
            try:
                cursor.execute(update_cmd)
                card_conn.commit()
            except Exception, e:
                print e
                LogExceptionHelp.logException(e)
            finally:
                cursor.close()
                card_conn.close()
        else:
            try:
                cursor.execute(insert_cmd)
                card_conn.commit()
            except Exception, e:
                print e
                LogExceptionHelp.logException(e)
            finally:
                cursor.close()
                card_conn.close()


# 挂载点信息
def insert_target_info():
    # 挂载点内所需的目录名称
    DIRs = [ConfigParameter.ParentDir, ConfigParameter.ISODir,
            ConfigParameter.SnapShotDr, ConfigParameter.IncrementDir]
    if len(ConfigParameter.Local_mount_path) != 0:
        for i in range(0, len(ConfigParameter.Local_mount_path)):
            local_path = ConfigParameter.Local_mount_path[i]
            # 检查配置的路径是否存在并修改权限
            for targe1_dir in DIRs[:2]:
                if local_path == ConfigParameter.Local_mount_path[0] and not os.path.isdir(
                                        local_path + os.sep + targe1_dir):
                    os.makedirs(local_path + os.sep + targe1_dir)
                    os.system("chown -R www.www %s" % local_path + os.sep + targe1_dir)
            for targe2_dir in DIRs[2:]:
                if local_path == ConfigParameter.Local_mount_path[1] and not os.path.isdir(
                                        local_path + os.sep + targe2_dir):
                    os.makedirs(local_path + os.sep + targe2_dir)
            tg_UUID = str(get_target_UUID()[i])
            tg_type = str(get_target_type()[i])
            tg_mount_name = str(get_target_mount_name()[i])
            tg_mount_path = str(get_target_mount_path()[i])
            tg_local_mountpath = str(get_local_mount_path()[i])
            tg_mount_disksize = str(get_mount_point_size(local_path))
            tg_mount_avlidesize = str(get_mount_point_ValidSize(local_path))
            tg_mount_pyhUUID = str(get_mount_point_UUID())
            tg_mount_last_updatime = str(get_mounted_last_updatetime())
            tg_mount_state = str(get_target_state(local_path))
            # 开始连接数据库并插入数据
            tg_conn = Conn()
            cursor = tg_conn.cursor()
            # 存在某条记录则更新，不存在则插入
            tg_sql_command = "INSERT INTO %s (uid,target_type,name,mount_path,local_path,size," \
                             "disk_size_available,physicalinfo_uid,last_update_time,state) " \
                             "VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')" % \
                             (ConfigParameter.Mysql_tgInfo_tbName, tg_UUID, tg_type, tg_mount_name,
                              tg_mount_path, tg_local_mountpath, tg_mount_disksize, tg_mount_avlidesize,
                              tg_mount_pyhUUID, tg_mount_last_updatime, tg_mount_state) + \
                             "ON DUPLICATE KEY UPDATE target_type='%s',name='%s',mount_path='%s'," \
                             "local_path='%s',size='%s',disk_size_available='%s',physicalinfo_uid='%s'," \
                             "last_update_time='%s',state='%s'" % \
                             (tg_type, tg_mount_name, tg_mount_path, tg_local_mountpath, tg_mount_disksize,
                              tg_mount_avlidesize, tg_mount_pyhUUID, tg_mount_last_updatime, tg_mount_state)
            try:
                cursor.execute(tg_sql_command)
                tg_conn.commit()
            except Exception, e:
                print e
                LogExceptionHelp.logException(e)
                continue
            finally:
                cursor.close()
                tg_conn.close()
    else:
        print("no mount path configured,check the configuration file")
        return False


# 单个虚拟机信息插入
def single_insert_vm_info(domain_name):
    # 开始连接数据库并插入数据
    vm_conn = Conn()
    cursor = vm_conn.cursor()
    vm_state = get_domain_state(domain_name)
    vm_cpuusage = get_domain_cpuusage(domain_name)
    vm_memusage = get_domain_memusage(domain_name)
    last_update_time = get_host_lastuptime()
    vm_sql_command = "update %s set vm_current_state='%s',vm_current_cpu='%.2f'," \
                     "vm_current_ram='%d',last_update_time='%s' where name = '%s'" % (
                         ConfigParameter.Mysql_vmInfo_tbName, vm_state, vm_cpuusage,
                         vm_memusage, last_update_time, domain_name)
    try:
        cursor.execute(vm_sql_command)
        vm_conn.commit()
    except Exception, e:
        print e
        LogExceptionHelp.logException(e)
    cursor.close()
    vm_conn.close()


# 多个虚拟机信息插入
def multi_insert_vm_info():
    conn = Libvirt_conn()
    # 获取所有虚拟机名称
    domains_obj = conn.listAllDomains()
    if len(domains_obj) != 0:
        for domain_obj in domains_obj:
            try:
                domain_name = domain_obj.name()
            except Exception, e:
                print e
                LogExceptionHelp.logException(e)
                continue
            task = Thread(target=lambda: single_insert_vm_info(domain_name))
            task.setDaemon(True)
            task.start()
    else:
        LogExceptionHelp.logMsg("Info: no domain can be update")
        return None


# 无线网卡更新方法
def update_Wireless():
    db_cards = get_wireless_db()
    phy_cards = get_wireless_phy()
    phy_card_keys = set(phy_cards.keys())
    conn = Conn()
    cursor = conn.cursor()
    # 得到数据库有，物理机没有的（删除数据库内容）
    if db_cards:
        result = db_cards.difference(phy_card_keys)
        if result:
            for card in result:
                cmd = "delete from %s WHERE %s='%s' and device='%s'" % \
                      (DBNames.PhysicalWifiCardTableName,
                       DBNames.PhysicalWifiCardTablePhysicalColumnName,
                       ConfigParameter.Myuid,
                       card)
                try:
                    cursor.execute(cmd)
                except Exception as e:
                    print e
                    LogExceptionHelp.logException(e)
                    continue
            conn.commit()
    # 得到物理机有，数据库没有的（添加到数据库）
    if phy_card_keys:
        result = phy_card_keys.difference(db_cards)
        if result:
            for card in result:
                cmd = "insert into %s (uid,physical_uid,bus,device,vendor,product,`name`)" \
                      " VALUES ('%s','%s','%s','%s','%s','%s','%s')" % \
                      (DBNames.PhysicalWifiCardTableName,
                       str(uuid.uuid1()),
                       ConfigParameter.Myuid,
                       phy_cards[card]['Bus'],
                       phy_cards[card]['Device'],
                       phy_cards[card]['Vendor'],
                       phy_cards[card]['Product'],
                       'wlan' + str(uuid.uuid1())[:5])
                try:
                    cursor.execute(cmd)
                except Exception as e:
                    print e
                    LogExceptionHelp.logException(e)
                    continue
            conn.commit()
    if cursor:
        cursor.close()
    conn.close()


# 校验数据库中网卡是否真实被虚拟机使用（实际使用情况和数据库不一至情况）
def check_db_Wireless_card():
    vm_usage_card = get_wireless_usage()
    all_card = set(get_wireless_phy().keys())
    conn = Conn()
    cursor = conn.cursor()
    try:
        # 数据库状态为已使用，实际未使用
        ret = all_card.difference(vm_usage_card)
        if ret:
            for card in ret:
                cmd = "update %s set state='%s' WHERE physical_uid='%s' and device='%s'" % \
                      (DBNames.PhysicalWifiCardTableName, 0, ConfigParameter.Myuid, card)
                cursor.execute(cmd)
                conn.commit()
                # 数据库状态未使用，实际已使用(直接更新)
        for card in vm_usage_card:
            cmd = "update %s set state='%s' WHERE physical_uid='%s' and device='%s'" % \
                  (DBNames.PhysicalWifiCardTableName, 1, ConfigParameter.Myuid, card)
            cursor.execute(cmd)
            conn.commit()
    except Exception as e:
        print e
        LogExceptionHelp.logException("insert_database.py in line 297 msg: {}".format(e))
    finally:
        if cursor:
            cursor.close()
        conn.close()


def main():
    if ConfigParameter.MyType == 'master' or ConfigParameter.MyType == 'slave':
        while True:
            insert_target_info()
            insert_phy_info()
            insert_phy_card()
            multi_insert_vm_info()
            if not ConfigParameter.nrtcase:
                update_Wireless()
            check_db_Wireless_card()
            time.sleep(ConfigParameter.Get_Data_interval)
    else:
        while True:
            insert_target_info()
            insert_phy_info()
            insert_phy_card()
            time.sleep(ConfigParameter.Get_Data_interval)


def work():
    # delete_dada()
    main()


if __name__ == '__main__':
    # delete_dada()
    main()
