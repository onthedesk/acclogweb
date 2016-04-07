#!/usr/bin/env python
# -*- coding: utf-8 -*-
import MySQLdb
import time
import warnings
from datetime import datetime
###过滤mysql警告
warnings.filterwarnings(action="ignore",category=MySQLdb.Warning)
#定义时间
date_name = datetime.now().strftime('%Y%m%d')
#分析日志函数
def import_log (dbname,logname,hostname):
    try:
        conn = MySQLdb.connect(host='127.0.0.1',user='root',passwd='root',port=3306)
        cur = conn.cursor()
        cur.execute('create database if not exists %s' %dbname )
        conn.select_db('%s'%dbname)
        # cur.execute('''DROP TABLE IF EXISTS log%s''' % date_name)
        cur.execute('''CREATE TABLE IF NOT EXISTS log%s (
        `id` INT (11) NOT NULL AUTO_INCREMENT COMMENT 'id',
        `host` VARCHAR (30) DEFAULT NULL COMMENT 'host',
        `remote_ip` VARCHAR (50) DEFAULT NULL COMMENT 'ip',
        `access_time` TIMESTAMP NOT NULL DEFAULT '0000-00-00 00:00:00' COMMENT 'request_time',
        `request` VARCHAR (2000) DEFAULT NULL COMMENT 'request',
        `status` varchar(5) DEFAULT NULL COMMENT 'status',
        `http_refere` VARCHAR (2000) DEFAULT NULL COMMENT 'http_refere',
        `ua` VARCHAR (400) DEFAULT NULL COMMENT 'ua',
        `request_time` FLOAT (7,5) COMMENT 'time',
         PRIMARY KEY (`id`)
         )''' % date_name)
        log = open("%s" % logname)
        for i in log.readlines():
            if len(i.split("\"")[7]) < 2000 and len(i.split("\"")[15]) < 2000:
                time_format = time.strftime("%Y-%m-%d %H:%M:%S", time.strptime(i.split("\"")[5], "%d/%b/%Y:%H:%M:%S +0800"))
               print "insert into log%s (host,remote_ip,access_time,request,status,http_refere,ua,request_time) values(\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\")" %(date_name,hostname,i.split("\"")[1],time_format,i.split("\"")[7],i.split("\"")[11],i.split("\"")[15],i.split("\"")[17],i.split("\"")[19])
                cur.execute("insert into log%s (host,remote_ip,access_time,request,status,http_refere,ua,request_time) values(\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\")" %(date_name,hostname,i.split("\"")[1],time_format,i.split("\"")[7],i.split("\"")[11],i.split("\"")[15],i.split("\"")[17],i.split("\"")[19]))
            else:
                continue
        log.close()
        conn.commit()
        cur.close()
        conn.close()
    except MySQLdb.Error,e:
        print "Mysql Error %d: %s" % (e.args[0], e.args[1])
#执行分析插入操作 
print "Begin**********"
print "log1"
import_log(dbname="app1",logname="log1",hostname='host1') 
import_log(dbname="app1",logname="log1",hostname='host2') 
import_log(dbname="app2",logname="log2",hostname='host1') 
import_log(dbname="app2",logname="log2",hostname='host2') 
print "End************"