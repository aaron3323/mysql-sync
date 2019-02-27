#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = 'jimmy'


import logging
import mysql.connector
import os

logging.basicConfig(level=logging.DEBUG)

host, port, user, password = '192.168.0.196', 3306, 'root', '123456789'

# 创建备份目录
curPath = os.path.dirname(os.path.realpath(__file__))
backDir = os.path.join(curPath, 'backup')
# os.mkdir(backDir)


def backup(host, port, user, password):
    try:
        conn = mysql.connector.connect(host=host, port=port, user=user, password=password)
        cursor = conn.cursor()
        sql = 'show databases'
        logging.debug(sql)
        cursor.execute(sql)
        databases = cursor.fetchall()
        return databases
    except BaseException:
        raise
    finally:
        cursor.close()
        conn.close()


databases = backup(host, port, user, password)
for db in databases:
    dumpcmd = "mysqldump -B -h %s -P %s -u%s -p%s %s --default-character-set=utf8 > %s/%s.sql" % (
    host, port, user, password, db[0], backDir, db[0])
    logging.debug(dumpcmd)
    os.system(dumpcmd)


