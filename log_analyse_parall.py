#!/bin/env python3
# coding:utf-8
"""
ljk 20161116(update 20170217)
This script should be put in crontab in every web server.Execute every n minutes.
Collect nginx access log, process it and insert the result into mysql.
"""
import os
import re
import subprocess
import time
import warnings
import pymysql
from sys import argv, exit
from socket import gethostname
from urllib.parse import unquote
from collections import OrderedDict
from zlib import crc32
from multiprocessing import Pool

##### 自定义部分 #####
# 定义日志格式，利用非贪婪匹配和分组匹配，需要严格参照日志定义中的分隔符和引号
log_pattern = r'^(?P<remote_addr>.*?) - \[(?P<time_local>.*?)\] "(?P<request>.*?)"' \
              r' (?P<status>.*?) (?P<body_bytes_sent>.*?) (?P<request_time>.*?)' \
              r' "(?P<http_referer>.*?)" "(?P<http_user_agent>.*?)" - (?P<http_x_forwarded_for>.*)$'
log_pattern_obj = re.compile(log_pattern)
# 日志目录
log_dir = '/zz_data/nginx_log/'
# 要处理的站点（可随需要向list中添加）
todo = ['www', 'user']
# MySQL相关设置
mysql_host = 'x.x.x.x'
mysql_user = 'xxxx'
mysql_passwd = 'xxxx'
mysql_port = 3307
mysql_database = 'log_analyse'
# 表结构
creat_table = "CREATE TABLE IF NOT EXISTS {} (\
                id bigint unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY,\
                server char(11) NOT NULL DEFAULT '',\
                uri varchar(255) NOT NULL DEFAULT '' COMMENT '$uri,已做uridecode',\
                uri_crc32 bigint unsigned NOT NULL DEFAULT '0' COMMENT '对上面uri字段计算crc32',\
                args varchar(255) NOT NULL DEFAULT '' COMMENT '$args,已做uridecode',\
                args_abs_crc32 bigint unsigned NOT NULL DEFAULT '0' COMMENT '对上面args字段进行抽象化然后计算crc32',\
                time_local timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',\
                response_code smallint NOT NULL DEFAULT '0',\
                bytes int NOT NULL DEFAULT '0',\
                request_time float(6,3) NOT NULL DEFAULT '0.000',\
                user_ip varchar(40) NOT NULL DEFAULT '',\
                cdn_ip varchar(15) NOT NULL DEFAULT '' COMMENT 'CDN最后节点的ip:空子串表示没经过CDN; - 表示没经过CDN和F5',\
                if_normal tinyint NOT NULL DEFAULT '0' \
                    COMMENT '0(正则根本无法匹配该行日志或日志中$request内容异常) 1(uri和args均正常) 2(uri不正常) 3(参数不正常:通过大小判断,200b) 4(uri和参数都不正常))',\
                KEY time_local (time_local),\
                KEY uri_crc32 (uri_crc32),\
                KEY args_abs_crc32 (args_abs_crc32)\
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
##### 自定义部分结束 #####

# 主机名
global server
server = gethostname()
# 今天零点
global today_start
today_start = time.strftime('%Y-%m-%d', time.localtime()) + ' 00:00:00'
# 将pymysql对于操作中的警告信息转为可捕捉的异常
warnings.filterwarnings('error', category=pymysql.err.Warning)


def my_connect():
    """链接数据库"""
    global connection, con_cur
    try:
        connection = pymysql.connect(host=mysql_host, user=mysql_user, password=mysql_passwd,
                                     charset='utf8mb4', port=mysql_port, autocommit=True, database=mysql_database)
    except pymysql.err.MySQLError as err:
        print('Error: ' + str(err))
        exit(20)
    con_cur = connection.cursor()


def create_table(t_name):
    """创建各站点对应的表"""
    my_connect()
    try:
        con_cur.execute(creat_table.format(t_name))
    except pymysql.err.Warning:
        pass


def process_line(line_str):
    """
    处理每一行记录
    line_str: 该行数据的字符串形式
    """
    processed = log_pattern_obj.search(line_str)
    if not processed:
        '''如果正则根本就无法匹配改行记录时'''
        print("Can't match the regex: {}".format(line_str))
        return server, '', 0, '', 0, '', '', '', '', '', '', 0
    else:
        # remote_addr (客户若不经过代理，则可认为用户的真实ip)
        remote_addr = processed.group('remote_addr')

        # time_local
        time_local = processed.group('time_local')
        # 转换时间为mysql date类型
        ori_time = time.strptime(time_local.split()[0], '%d/%b/%Y:%H:%M:%S')
        new_time = time.strftime('%Y-%m-%d %H:%M:%S', ori_time)

        # 处理uri和参数
        request = processed.group('request')
        request_further = re.split(r'[\s]+', request)
        if len(request_further) == 3:
            '''正常，$request的值应该以空格分为三部分 method full_uri schema。有的异常记录可能会少某个字段'''
            full_uri = request_further[1]
            uri_args = full_uri.split('?', 1)
            # 对日志中经过uri_encode过的字符进行还原
            uri = unquote(uri_args[0])
            if len(uri_args) == 1:
                args = ''
            else:
                args = unquote(uri_args[1])

                # 进一步处理args，将args中的参数部分做相应转换(抽象化)，规则：参数值不包含=的，转换为?；包含=的，若值由[a-zA-Z\-_]组成，则保留，其他情况值转为?
                args_abs = ''
                try:
                    arg_dict = OrderedDict((tmp.split('=') for tmp in args.split('&')))
                    for k, v in arg_dict.items():
                        if not re.match(r'[a-zA-Z\-_]+$', v):
                            '''value的值为全字母时，不进行转换'''
                            arg_dict[k] = '?'
                    for k, v in arg_dict.items():
                        if args_abs == '':
                            args_abs += '{}={}'.format(k, v)
                        else:
                            args_abs += '&{}={}'.format(k, v)
                except ValueError as err:
                    '''参数中没有= 或者 即没&也没= 会抛出ValueError'''
                    args_abs = '?'

            # 判断uri及args是否正常
            # if_normal: 1(正常) 2(uri不正常,通过大小暂定200b) 3(args不正常,同过大小暂定200b;or '?' in args) 4(uri和args都不正常)
            if len(uri) > 200:
                if_normal = 2
                if len(args) > 200 or '?' in args:
                    if_normal = 4
            else:
                if_normal = 1
                if len(args) > 200 or '?' in args:
                    if_normal = 3

            # 对库里的uri和args字段进行crc32校验
            uri_crc32 = crc32(uri.encode())
            args_abs_crc32 = 0 if args == '' else crc32(args_abs.encode())
        else:
            '''$request不能被正确的被空格分为三段时，正常是可以的'''
            print('$request abnormal: {}'.format(line_str))
            uri = request
            uri_crc32 = 0
            args = ''
            args_abs_crc32 = 0
            if_normal = 0

        # 状态码,字节数,响应时间
        response_code = processed.group('status')
        size = processed.group('body_bytes_sent')
        request_time = processed.group('request_time')

        # user_ip,cdn最后节点ip,以及是否经过F5
        http_x_forwarded_for = processed.group('http_x_forwarded_for')
        ips = http_x_forwarded_for.split()
        # user_ip：用户真实ip
        # cdn_ip: CDN最后节点的ip，''表示没经过CDN；'-'表示没经过CDN和F5
        if http_x_forwarded_for == '-':
            '''没经过CDN和F5'''
            user_ip = remote_addr
            cdn_ip = '-'
        elif ips[0] == remote_addr:
            '''没经过CDN，经过F5'''
            user_ip = remote_addr
            cdn_ip = ''
        else:
            '''经过CDN和F5'''
            user_ip = ips[0].rstrip(',')
            cdn_ip = ips[-1]

        return server, uri, uri_crc32, args, args_abs_crc32, new_time, response_code, size, request_time, user_ip, cdn_ip, if_normal


def insert_data(line_data, cursor, results, limit, t_name, l_name):
    """
    记录处理之后的数据,累积limit条执行一次插入
    line_data:每行处理之前的字符串数据;
    limit:每limit行执行一次数据插入;
    t_name:对应的表名;
    l_name:日志文件名
    """
    line_result = process_line(line_data)
    results.append(line_result)
    # print('len(result):{}'.format(len(result)))    #debug
    if len(results) == limit:
        insert_correct(cursor, results, t_name, l_name)
        results.clear()
        print('{} {} 处理至 {}'.format(time.strftime('%H:%M:%S', time.localtime()), l_name, line_result[5]))


def insert_correct(cursor, results, t_name, l_name):
    """在插入数据过程中处理异常"""
    insert_sql = 'insert into {} (server,uri,uri_crc32,args,args_abs_crc32,time_local,response_code,bytes,request_time,user_ip,cdn_ip,if_normal) ' \
                 'values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'.format(t_name)
    try:
        cursor.executemany(insert_sql, results)
    except pymysql.err.Warning as err:
        print('\n{}    Warning: {}'.format(l_name, err))
    except pymysql.err.MySQLError as err:
        print('\n{}    Error: {}'.format(l_name, err))
        print('插入数据时出错...\n')
        connection.close()
        exit(10)


def get_prev_num(t_name, l_name):
    """取得今天已入库的行数 t_name:表名 l_name:日志文件名"""
    try:
        con_cur.execute('select min(id) from {0} where time_local=('
                        'select min(time_local) from {0} where time_local>="{1}")'.format(t_name, today_start))
        min_id = con_cur.fetchone()[0]
        if min_id is not None:  # 假如有今天的数据
            con_cur.execute('select max(id) from {}'.format(t_name))
            max_id = con_cur.fetchone()[0]
            con_cur.execute(
                'select count(*) from {} where id>={} and id<={} and server="{}"'.format(t_name, min_id, max_id, server))
            prev_num = con_cur.fetchone()[0]
        else:
            prev_num = 0
        return prev_num
    except pymysql.err.MySQLError as err:
        print('Error: {}'.format(err))
        print('Error:未取得已入库的行数,本次跳过{}\n'.format(l_name))
        return


def del_old_data(t_name, l_name):
    """删除3天前的数据"""
    # 3天前的日期时间
    three_days_ago = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time() - 3600 * 24 * 3))
    try:
        con_cur.execute('select max(id) from {0} where time_local=('
                        'select max(time_local) from {0} where time_local!="0000-00-00 00:00:00" and time_local<="{1}")'.format(
            t_name, three_days_ago))
        max_id = con_cur.fetchone()[0]
        if max_id is not None:
            con_cur.execute('delete from {} where id<={}'.format(t_name, max_id))
    except pymysql.err.MySQLError as err:
        print('\n{}    Error: {}'.format(l_name, err))
        print('未能删除表3天前的数据...\n')


def main_loop(log_name):
    """主逻辑 log_name:日志文件名"""
    table_name = log_name.split('.access')[0].replace('.', '_')  # 将域名例如m.api转换成m_api,因为表名中不能包含'.'
    results = []
    # 创建表
    create_table(table_name)

    # 当前日志文件总行数
    num = int(subprocess.run('wc -l {}'.format(log_dir + log_name), shell=True, stdout=subprocess.PIPE,
                             universal_newlines=True).stdout.split()[0])
    # print('num: {}'.format(num))  # debug
    # 上一次处理到的行数
    prev_num = get_prev_num(table_name, log_name)
    if prev_num is not None:
        # 根据当前行数和上次处理之后记录的行数对比,来决定本次要处理的行数范围
        i = 0
        with open(log_name) as fp:
            for line in fp:
                i += 1
                if i <= prev_num:
                    continue
                elif prev_num < i <= num:
                    insert_data(line, con_cur, results, 1000, table_name, log_name)
                else:
                    break
        # 插入最后不足1000行的results
        if len(results) > 0:
            insert_correct(con_cur, results, table_name, log_name)

    del_old_data(table_name, log_name)

if __name__ == "__main__":
    # 检测如果当前已经有该脚本在运行,则退出
    if_run = subprocess.run('ps -ef|grep {}|grep -v grep|grep -v "/bin/sh"|wc -l'.format(argv[0]), shell=True,
                            stdout=subprocess.PIPE).stdout
    if if_run.decode().strip('\n') == '1':
        os.chdir(log_dir)
        logs_list = os.listdir(log_dir)
        logs_list = [i for i in logs_list if 'access' in i and os.path.isfile(i) and i.split('.access')[0] in todo]
        if len(logs_list) > 0:
            # 并行
            with Pool(len(logs_list)) as p:
                p.map(main_loop, logs_list)
