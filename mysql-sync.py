#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = 'jimmy'

import logging
import mysql.connector
import os
import time
import yaml

logging.basicConfig(level=logging.INFO)

def init():
    global source_database
    global source_host
    global source_port
    global source_user
    global source_pwd

    global target_database
    global target_host
    global target_port
    global target_user
    global target_pwd

    global upgrade_sql_file
    global upgrade_sql
    upgrade_sql = []

    # 获取当前脚本所在文件夹路径
    curPath = os.path.dirname(os.path.realpath(__file__))
    # sql脚本
    datetime = time.strftime('%Y%m%d%H%M%S')
    upgrade_sql_file = os.path.join(curPath, 'upgrade_'+datetime+'.sql')
    # 获取yaml文件路径
    yamlPath = os.path.join(curPath, "config.yaml")

    # open方法打开直接读出来
    f = open(yamlPath, 'r', encoding='utf-8')
    cfg = f.read()
    # print(type(cfg))  # 读出来是字符串
    # print(cfg)

    d = yaml.load(cfg)  # 用load方法转字典
    # print(d)
    # print(type(d))
    datadict = d.get("db")
    logging.debug(datadict)

    source_database = datadict.get("source").get("database")
    source_host = datadict.get("source").get("host")
    source_port = datadict.get("source").get("port")
    source_user = datadict.get("source").get("user")
    source_pwd = datadict.get("source").get("pwd")

    target_database = datadict.get("target").get("database")
    target_host = datadict.get("target").get("host")
    target_port = datadict.get("target").get("port")
    target_user = datadict.get("target").get("user")
    target_pwd = datadict.get("target").get("pwd")

# 获取所有表名
def get_tables(host, port, user, password, database):
    try:
        conn = mysql.connector.connect(host=host, port=port, user=user, password=password)
        cursor = conn.cursor()
        sql = 'show tables from ' + database
        logging.debug(sql)
        cursor.execute(sql)
        tables = cursor.fetchall()
        return tables
    except BaseException:
        raise
    finally:
        cursor.close()
        conn.close()


# 获取建表语句
def get_create_ddl(host, port, user, password, database, tablename):
    try:
        conn = mysql.connector.connect(host=host, port=port, user=user, password=password)
        cursor = conn.cursor()
        sql = 'show create table %s.%s' % (database, tablename)
        logging.debug(sql)
        cursor.execute(sql)
        tables = cursor.fetchone()
        return tables
    except BaseException:
        raise
    finally:
        cursor.close()
        conn.close()


# 查询column详情
def get_columns_by_tablename(host, port, user, password, database, tablename):
    try:
        conn = mysql.connector.connect(host=host, port=port, user=user, password=password)
        cursor = conn.cursor()
        sql = "select COLUMN_NAME,COLUMN_TYPE,IS_NULLABLE,COLUMN_DEFAULT,COLUMN_COMMENT,EXTRA from information_schema.columns where TABLE_SCHEMA='%s' and TABLE_NAME = '%s' order by ORDINAL_POSITION asc" % (database, tablename)
        logging.debug(sql)
        cursor.execute(sql)
        tables = cursor.fetchall()
        return tables
    except BaseException:
        raise
    finally:
        cursor.close()
        conn.close()



init()

# 获取源库、目标库所有表名
source_tables = get_tables(source_host, source_port, source_user, source_pwd, source_database)
target_tables = get_tables(target_host, target_port, target_user, target_pwd, target_database)

# logging.debug(source_tables)
# logging.debug(target_tables)

new_tables = set(source_tables).difference(set(target_tables))      # 获取新建的表
remove_tables = set(target_tables).difference(set(source_tables))   # 获取已删除的表

# logging.debug(new_tables)
# logging.debug(remove_tables)

# 生成建表语句
for new_table in new_tables:
    create_ddl = get_create_ddl(source_host, source_port, source_user, source_pwd, source_database, new_table[0])
    print(create_ddl[1] + ';')
    upgrade_sql.append(create_ddl[1] + ';')
    upgrade_sql.append("\n")

# 生成删除表语句
for remove_table in remove_tables:
    sql = 'drop table %s' % (remove_table[0])
    print(sql + ';')
    upgrade_sql.append(sql + ';')
    upgrade_sql.append("\n")


# 获取交集（获取源库和目标库都有的表，此时要做column对比）
# 只能识别add、drop、change
# 只判断3种情况
# 一：移除字段
# 二：字段属性变更（不包括重命名，暂时无法判断字段重命名）
# 三：新增字段
# 无法识别字段重命名、顺序变更
# 以source为准对比target，生成alert语句
intersection_tables = set(source_tables).intersection(set(target_tables))
for intersection_table in intersection_tables:
    table = intersection_table[0]
    logging.debug(table)

    # 获取表所有字段信息，source/target
    source_columns_table = get_columns_by_tablename(source_host, source_port, source_user, source_pwd, source_database, table)
    target_columns_table = get_columns_by_tablename(target_host, target_port, target_user, target_pwd, target_database, table)
    # print(source_columns_table)
    # print(target_columns_table)
    if source_columns_table == target_columns_table:
        logging.debug('%s表没有变更' % table)
        break

    # 转换成字典格式，便于操作
    source_columns_dict = {items[0]: items for items in source_columns_table}
    target_columns_dict = {items[0]: items for items in target_columns_table}
    # print(source_columns_dict)
    # print(target_columns_dict)

    # 情况一：移除字段
    # 遍历target表字段，判断条件'不包含在source里的'，得出要被删除的字段并生成sql
    remove_columns = {key: value for key, value in target_columns_dict.items() if key not in source_columns_dict.keys()}
    remove_sql = {'ALTER TABLE %s DROP %s' % (table, column_name) for column_name in remove_columns.keys()}
    for sql in remove_sql:
        print(sql + ';')
        upgrade_sql.append(sql + ';')
        upgrade_sql.append("\n")

    # 遍历source表字段，循环判断targer表里是否存在
    for source_key, source_value in source_columns_dict.items():
        if source_key in target_columns_dict.keys():
            # target表里包含当前字段，此时判断该字段属性
            target_value = target_columns_dict.get(source_key)
            if source_value != target_value:
                # 情况二：字段属性变更
                logging.debug('字段%s属性有变化' % source_key)
                logging.debug('     source: key=%s, value=%s' % (source_key, source_value))
                logging.debug('     target: key=%s, value=%s' % (source_key, target_value))
                source_column_name, source_column_type, source_is_nullable, source_column_default, source_column_comment, source_extra = source_value
                target_column_name, target_column_type, target_is_nullable, target_column_default, target_column_comment, target_extra = target_value
                sql = 'ALTER TABLE %s MODIFY COLUMN %s %s' % (table, source_column_name, source_column_type)
                if source_is_nullable != target_is_nullable:
                    sql = '%s %s' % (sql, 'null' if "YES".lower() == source_is_nullable.lower() else 'not null')
                if source_column_default != target_column_default:
                    sql = '%s %s' % (sql, 'default \'' + source_column_default + '\'')
                if source_column_comment != target_column_comment:
                    sql = '%s %s' % (sql, 'comment \'' + source_column_comment + '\'')

                print(sql + ';')
                upgrade_sql.append(sql + ';')
                upgrade_sql.append("\n")
        else:
            # 情况三：新增字段
            source_column_name, source_column_type, source_is_nullable, source_column_default, source_column_comment, source_extra = source_value
            sql = 'ALTER TABLE %s ADD COLUMN %s %s' % (table, source_column_name, source_column_type)
            sql = '%s %s' % (sql, 'null' if "YES".lower() == source_is_nullable.lower() else 'not null')
            if source_column_default:
                sql = '%s %s' % (sql, 'default \'' + source_column_default + '\'')
            if source_column_comment:
                sql = '%s %s' % (sql, 'comment \'' + source_column_comment + '\'')

            print(sql + ';')
            upgrade_sql.append(sql + ';')
            upgrade_sql.append("\n")


print('---------')
with open(upgrade_sql_file, 'w+') as f:
    for sql in upgrade_sql:
        f.write(sql)


# 备份当前数据库
# source脚本
