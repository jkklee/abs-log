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
from zlib import crc32
from multiprocessing import Pool
from functools import wraps


def timer(function):
    @wraps(function)
    def inner_func(*args, **kwargs):
        t0 = time.time()
        result_ = function(*args, **kwargs)
        t1 = time.time()
        print("Total time running %s: %s seconds" % (function.__name__, str(t1 - t0)))
        return result_

    return inner_func


# ---------- 自定义部分 ----------#
# 定义日志格式，利用非贪婪匹配和分组匹配，需要严格参照日志定义中的分隔符和引号
log_pattern = r'^(?P<remote_addr>.*?) - \[(?P<time_local>.*?)\] "(?P<request>.*?)"' \
              r' (?P<status>.*?) (?P<body_bytes_sent>.*?) (?P<request_time>.*?)' \
              r' "(?P<http_referer>.*?)" "(?P<http_user_agent>.*?)" - (?P<http_x_forwarded_for>.*)$'
# request的正则，其实是由 "request_method request_uri server_protocol"三部分组成
request_uri_pattern = r'^(?P<request_method>(GET|POST|HEAD|DELETE)?) (?P<request_uri>.*?) (?P<server_protocol>HTTP.*)$'
# 日志目录
log_dir = '/zz_data/nginx_log/'
# 要处理的站点（可随需要想list中添加）
todo = ['www', 'user']
# MySQL相关设置
mysql_host = 'x.x.x.x'
mysql_user = 'xxxx'
mysql_passwd = 'xxxx'
mysql_port = 3307
mysql_database = 'log_analyse'
# 表结构
table_format = "CREATE TABLE IF NOT EXISTS {} (\
                id bigint unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY,\
                server char(11) NOT NULL DEFAULT '',\
                uri_abs varchar(200) NOT NULL DEFAULT '' COMMENT '对$uri做uridecode,然后做抽象化处理',\
                uri_abs_crc32 bigint unsigned NOT NULL DEFAULT '0' COMMENT '对上面uri_abs字段计算crc32',\
                args_abs varchar(200) NOT NULL DEFAULT '' COMMENT '对$args做uridecode,然后做抽象化处理',\
                args_abs_crc32 bigint unsigned NOT NULL DEFAULT '0' COMMENT '对上面args字段计算crc32',\
                time_local timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',\
                response_code smallint NOT NULL DEFAULT '0',\
                bytes_sent int NOT NULL DEFAULT '0' COMMENT '发送给客户端的响应大小',\
                request_time float(6,3) NOT NULL DEFAULT '0.000',\
                user_ip varchar(40) NOT NULL DEFAULT '',\
                cdn_ip varchar(15) NOT NULL DEFAULT '' COMMENT 'CDN最后节点的ip:空字串表示没经过CDN; - 表示没经过CDN和F5',\
                request_method varchar(7) NOT NULL DEFAULT '',\
                uri varchar(255) NOT NULL DEFAULT '' COMMENT '$uri,已做uridecode',\
                args varchar(255) NOT NULL DEFAULT '' COMMENT '$args,已做uridecode',\
                referer varchar(255) NOT NULL DEFAULT '' COMMENT '',\
                KEY time_local (time_local),\
                KEY uri_abs_crc32 (uri_abs_crc32),\
                KEY args_abs_crc32 (args_abs_crc32)\
              ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 row_format=compressed"
# 每处理limit条日志,执行一次数据库插入操作(多行插入,减少和数据库间的交互)
limit = 1000
# ---------- 自定义部分结束 ----------#


log_pattern_obj = re.compile(log_pattern)
request_uri_pattern_obj = re.compile(request_uri_pattern)
# 主机名
global server
server = gethostname()
# 今天零点
global today_start
today_start = time.strftime('%Y-%m-%d', time.localtime()) + ' 00:00:00'
# 将pymysql对于操作中的警告信息转为可捕捉的异常
warnings.filterwarnings('error', category=pymysql.err.Warning)


@timer
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


@timer
def create_table(t_name):
    """创建各站点对应的表"""
    my_connect()
    try:
        con_cur.execute(table_format.format(t_name))
    except pymysql.err.Warning:
        pass
    except pymysql.err.MySQLError as err:
        print('\n{}    Error: {}'.format(t_name, err))
        connection.close()
        exit(10)


def process_line(line_str):
    """
    处理每一行记录
    line_str: 该行数据的字符串形式
    """
    processed = log_pattern_obj.search(line_str)
    if not processed:
        '''如果正则根本就无法匹配该行记录时'''
        print("Can't process this line: {}".format(line_str))
        return server, '', 0, '', 0, '', '', '', '', '', '', '', '', ''
    else:
        # remote_addr (客户若不经过代理，则可认为用户的真实ip)
        remote_addr = processed.group('remote_addr')

        # time_local
        time_local = ngx_time_local_to_mysql_timestamp(processed.group('time_local').split()[0])

        # 处理uri和args
        request = processed.group('request')
        request_further = request_uri_pattern_obj.search(request)
        if request_further:
            request_method = request_further.group('request_method')
            request_uri = request_further.group('request_uri')
            uri_args = request_uri.split('?', 1)
            # 对uri和args进行urldecode
            uri = unquote(uri_args[0])
            args = '' if len(uri_args) == 1 else unquote(uri_args[1])
            # 对uri和args进行抽象化
            uri_abs = text_abstract(uri, 'uri')
            args_abs = text_abstract(args, 'args')
            # 对库里的uri_abs和args_abs字段进行crc32校验
            uri_abs_crc32 = crc32(uri_abs.encode())
            args_abs_crc32 = 0 if args_abs == '' else crc32(args_abs.encode())
        else:
            print('$request abnormal: {}'.format(line_str))
            request_method = ''
            uri = request
            uri_abs = ''
            uri_abs_crc32 = 0
            args = ''
            args_abs = ''
            args_abs_crc32 = 0

        # 状态码,字节数,响应时间
        response_code = processed.group('status')
        bytes_sent = processed.group('body_bytes_sent')
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

        return (server, uri_abs, uri_abs_crc32, args_abs, args_abs_crc32, time_local, response_code, bytes_sent,
                request_time, user_ip, cdn_ip, request_method, uri, args)


def ngx_time_local_to_mysql_timestamp(ngx_time_local):
    month_dict={'Jan':'01','Feb':'02','Mar':'03','Apr':'04','May':'05','Jun':'06',
                'Jul':'07','Aug':'08','Sep':'09','Oct':'10','Nov':'11','Dec':'12'}
    # tmp中元素顺序: '%d/%b/%Y:%H:%M:%S'
    tmp=re.split('/|:',ngx_time_local)
    # 返回格式: '%Y-%m-%d %H:%M:%S'
    return '{}-{}-{} {}:{}:{}'.format(tmp[2], month_dict[tmp[1]], tmp[0], tmp[3], tmp[4], tmp[5])


def text_abstract(text, what):
    """
    对uri和args进行抽象化,利于分类
    抽象规则:
        uri中所有的数字抽象为'?'
        args中所有参数值抽象为'?'
    text: 待处理的内容
    what: uri 或 args
    """
    if what == 'uri':
        return re.sub('[0-9]+','?',text)
    elif what == 'args':
        return re.sub('=[^&=]+','=?',text)


def insert_correct(cursor, results, t_name, l_name):
    """多行插入,并且处理在插入过程中的异常"""
    insert_sql = 'insert into {} (server,uri_abs,uri_abs_crc32,args_abs,args_abs_crc32,time_local,response_code,' \
                 'bytes_sent,request_time,user_ip,cdn_ip,request_method,uri,args) ' \
                 'values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'.format(t_name)
    try:
        cursor.executemany(insert_sql, results)
    except pymysql.err.Warning as err:
        print('\n{}    Warning: {}'.format(l_name, err))
    except pymysql.err.MySQLError as err:
        print('\n{}    Error: {}'.format(l_name, err))
        print('插入数据时出错...\n')
        connection.close()
        exit(10)


@timer
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
                'select count(*) from {} where id>={} and id<={} and server="{}"'.format(t_name, min_id, max_id,
                                                                                         server))
            prev_num = con_cur.fetchone()[0]
        else:
            prev_num = 0
        return prev_num
    except pymysql.err.MySQLError as err:
        print('Error: {}'.format(err))
        print('Error:未取得已入库的行数,本次跳过{}\n'.format(l_name))
        return


@timer
def del_old_data(t_name, l_name, n=3):
    """删除n天前的数据,n默认为3"""
    have_del = con_cur.execute('select info from information_schema.processlist where info like "delete from {}%"'.format(t_name))
    if have_del < 1:
        '''避免多个server都发起删除请求'''
        # n天前的日期间
        n_days_ago = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time() - 3600 * 24 * n))
        try:
            con_cur.execute('select max(id) from {0} where time_local=(select max(time_local) from {0}\
                             where time_local!="0000-00-00 00:00:00" and time_local<="{1}")'.format(t_name, n_days_ago))
            max_id = con_cur.fetchone()[0]
            if max_id is not None:
                con_cur.execute('delete from {} where id<={}'.format(t_name, max_id))
        except pymysql.err.MySQLError as err:
            print('\n{}    Error: {}'.format(l_name, err))
            print('未能删除{}天前的数据...\n'.format(n))


@timer
def main_loop(log_name):
    """log_name:日志文件名"""
    table_name = log_name.split('.access')[0].replace('.', '_')  # 将域名例如m.api转换成m_api,因为表名中不能包含'.'
    # 创建表
    create_table(table_name)

    # 当前日志文件总行数
    num = int(subprocess.run('wc -l {}'.format(log_dir + log_name), shell=True, stdout=subprocess.PIPE,
                             universal_newlines=True).stdout.split()[0])
    # 上一次处理到的行数
    prev_num = get_prev_num(table_name, log_name)
    if prev_num is not None:
        # 根据当前行数和上次处理之后记录的行数对比,来决定本次要处理的行数范围
        i = 0
        # 用于存放每行日志处理结果的列表,该列表长度达到limit后执行一次插入数据库操作
        results = []
        with open(log_name) as fp:
            for line in fp:
                i += 1
                if i <= prev_num:
                    continue
                elif prev_num < i <= num:
                    '''对本次应该处理的行进行处理并入库'''
                    line_result = process_line(line)
                    results.append(line_result)
                    if len(results) == limit:
                        insert_correct(con_cur, results, table_name, log_name)
                        results.clear()
                        print('{} {} 处理至 {}'.format(time.strftime('%H:%M:%S', time.localtime()), log_name, line_result[5]))
                else:
                    break
        # 插入最后不足limit行的results
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
