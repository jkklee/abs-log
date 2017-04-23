#!/bin/env python3 
# coding:utf-8
"""
ljk 20161116
This script should be put in crontab in every web server.Execute every 10 minutes.
Collect nginx access log, process it and insert the result into mysql in 1.21.
"""
import os
from sys import argv
import subprocess
import time
from socket import gethostname
from urllib.parse import unquote
from sys import exit
import warnings
import pymysql
from multiprocessing import Pool

log_dir = '/zz_data/nginx_log/'
todo = ['www', 'user']
# exclude_ip = ['192.168.1.200', '192.168.1.202']
# 主机名
global server
server = gethostname()
# 今天零点
global today_start
today_start = time.strftime('%Y-%m-%d', time.localtime()) + ' 00:00:00'
# 将pymysql对于操作中的警告信息转为可捕捉的异常
warnings.filterwarnings('error', category=pymysql.err.Warning)


def my_connect():
    """链接数据库函数"""
    global connection, con_cur
    try:
        connection = pymysql.connect(host='172.16.200.24', user='xxxx', password='xxxx', charset='utf8', port=3307, autocommit=True, database='test')
    except pymysql.err.MySQLError as err:
        print('Error: ' + str(err))
        exit(20)
    con_cur = connection.cursor()


def create_table(t_name):
    """创建表函数"""
    my_connect()
    try:
        con_cur.execute(
            "CREATE TABLE IF NOT EXISTS {} (id bigint NOT NULL AUTO_INCREMENT PRIMARY KEY,server char(11) NOT NULL DEFAULT '',url varchar(255) NOT NULL DEFAULT '' COMMENT '去掉参数的url,已做urldecode',url_digest char(32) NOT NULL DEFAULT '' COMMENT '对原始的不含参数的url计算MD5',time_local timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',response_code smallint NOT NULL DEFAULT '0',bytes int NOT NULL DEFAULT '0',request_time float(6,3) NOT NULL DEFAULT '0.000',user_ip varchar(40) NOT NULL DEFAULT '',cdn_ip varchar(15) NOT NULL DEFAULT '' COMMENT 'CDN最后节点的ip:空子串表示没经过CDN; - 表示没经过CDN和F5',if_normal tinyint NOT NULL DEFAULT '1' COMMENT '2(url不正常) 3(参数不正常:通过大小判断,200bytes) 4(url和参数都不正常)',KEY time_local (time_local),KEY url_digest (url_digest(8))) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4".format(t_name))
    except pymysql.err.Warning:
        pass


def process_line(line_str):
    """
    处理每一行记录
    line_str: 该行数据的字符串形式
    """
    step1 = line_str.split('"')
    '''
    # 过滤反向代理的探测请求,返回None
    for ip in exclude_ip:
        if ip in step1[0]:
            return server, '', '', '', '', '', '', ip, '-', ''
    '''
    # 处理remote_ip和时间
    step2 = step1[0].split()
    remote_ip = step2[0]
    time_local = step2[2].lstrip('[')
    # 转换时间为mysql date类型
    ori_time = time.strptime(time_local, '%d/%b/%Y:%H:%M:%S')
    new_time = time.strftime('%Y-%m-%d %H:%M:%S', ori_time)
    # 处理url和参数
    if '?' not in step1[1]:
        step3 = step1[1].split()
        args = ''
        if len(step3) > 1:
            ori_url = url = step3[1]  # 这里若url里包含空格的话,就会被截断
        else:
            ori_url = step3[0]
            url = ori_url
        if '%' in ori_url:
            url = unquote(ori_url)
    else:
        n = step1[1].index('?')
        step3 = step1[1][0:n]
        ori_url = step3.split()[-1]
        url = ori_url
        try:
            m = step1[1].index('HTTP', n)
            args = step1[1][n:m]
        except ValueError:
            args = step1[1][n:]
        if '%' in ori_url:
            url = unquote(ori_url)
    # 判断url及args是否正常
    # if_normal: 1(正常) 2(url不正常) 3(args过大,暂定200bytes) 4(url和args都不正常)
    if ' ' in ori_url or '"' in ori_url or "'" in ori_url:
        if_normal = 2
        if len(args) > 200:
            if_normal = 4
    else:
        if_normal = 1
        if len(args) > 200:
            if_normal = 3
    # 计算原始url MD5(处理原始url里的引号,以便取得MD5)
    if '"' in ori_url and "'" not in ori_url:
        md5 = subprocess.run("echo '{}'|md5sum".format(ori_url), shell=True, stdout=subprocess.PIPE,
                             universal_newlines=True).stdout.split()[0]
    elif "'" in ori_url and '"' not in ori_url:
        md5 = subprocess.run('echo "{}"|md5sum'.format(ori_url), shell=True, stdout=subprocess.PIPE,
                             universal_newlines=True).stdout.split()[0]
    elif '"' in ori_url and "'" in ori_url:
        md5 = ''
    else:
        md5 = subprocess.run('echo "{}"|md5sum'.format(ori_url), shell=True, stdout=subprocess.PIPE,
                             universal_newlines=True).stdout.split()[0]
    # 状态码,字节数,响应时间
    triple = step1[2].split()
    response_code = triple[0]
    size = triple[1]
    request_time = triple[2]
    # user_ip,cdn最后节点ip,以及是否经过F5
    ips = step1[-1].split()[1:]
    if ips[-1] == '-':
        user_ip = remote_ip
        cdn_ip = '-'
    elif ips[-1] == ips[0]:
        user_ip = remote_ip
        cdn_ip = ''
    else:
        user_ip = ips[0].rstrip(',')
        cdn_ip = ips[-1]

    return server, url, md5, new_time, response_code, size, request_time, user_ip, cdn_ip, if_normal


def insert_data(line_data, cursor, results, limit, line_num, t_name, l_name):
    """
    记录处理之后的数据,累积limit条执行一次插入
    line_data:每行处理之前的字符串数据; 
    limit:每limit行执行一次数据插入; 
    t_name:对应的表名;
    line_num:行号,仅在输出错误信息时用
    l_name:日志文件名
    """
    try:
        line_result = process_line(line_data)
    except Exception as err:
        print('\n{}    Error: {}'.format(l_name, err))
        print('{}: {}\n'.format(line_num, line_data))
        line_result = (server, '第{}行'.format(line_num), '', '', '', '', '', '', '', '')

    results.append(line_result)
    # print('len(result):{}'.format(len(result)))    #debug
    if len(results) == limit:
        insert_correct(cursor, results, line_num, t_name, l_name)
        results.clear()
        print('{} {} 处理至 {}'.format(time.strftime('%H:%M:%S', time.localtime()), l_name, line_result[3]))


def insert_correct(cursor, results, line_num, t_name, l_name):
    """在插入数据过程中处理异常"""
    insert_sql = 'insert into {} (server,url,url_digest,time_local,response_code,bytes,request_time,user_ip,cdn_ip,if_normal) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'.format(t_name)
    try:
        cursor.executemany(insert_sql, results)
    except pymysql.err.Warning as err:
        print('\n{}    Warning: {}'.format(l_name, err))
        print('Line {}: 此前的{}行中有异常数据\n'.format(line_num, len(results)))
    except pymysql.err.MySQLError as err:
        print('\n{}    Error: {}'.format(l_name, err))
        print('插入数据时出错...\n')
        connection.close()
        exit(10)


def get_prev_num(t_name, l_name):
    """取得今天已入库的行数 t_name:表名 l_name:日志文件名"""
    try:
        con_cur.execute('select min(id) from {0} where time_local=(select min(time_local) from {0} where time_local>="{1}")'.format(t_name, today_start))
        min_id = con_cur.fetchone()[0]
        if min_id is not None:  # 假如有今天的数据
            con_cur.execute('select max(id) from {}'.format(t_name))
            max_id = con_cur.fetchone()[0]
            con_cur.execute('select count(*) from {} where id>={} and id<={} and server="{}"'.format(t_name, min_id, max_id, server))
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
    three_days_ago = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()-3600*24*3))
    try:
        con_cur.execute('select max(id) from {0} where time_local=(select max(time_local) from {0} where time_local!="0000-00-00 00:00:00" and time_local<="{1}")'.format(t_name, three_days_ago))
        max_id = con_cur.fetchone()[0]
        if max_id is not None:
            con_cur.execute('delete from {} where id<={}'.format(t_name, max_id))
    except pymysql.err.MySQLError as err:
        print('\n{}    Error: {}'.format(l_name, err))
        print('未能删除表3天前的数据...\n')


def main_loop(log_name):
    """主逻辑 log_name:日志文件名"""
    table_name = log_name.split('.access')[0].replace('.', '_')  # 将域名例如v.api转换成v2_api,因为表名中不能包含'.'
    results = []
    # 创建表
    create_table(table_name)

    # 当前日志文件总行数
    num = int(subprocess.run('wc -l {}'.format(log_dir + log_name), shell=True, stdout=subprocess.PIPE, universal_newlines=True).stdout.split()[0])
    print('num: {}'.format(num))  #debug
    # 上一次处理到的行数
    prev_num = get_prev_num(table_name, log_name)
    if prev_num is not None:
        # 根据当前行数和上次处理之后记录的行数对比,来决定本次要处理的行数范围
        with open(log_name) as fp:
            i = 0
            for line in fp:
                i += 1
                if i <= prev_num:
                    continue
                elif prev_num < i <= num:
                    # print(i)    #debug
                    insert_data(line, con_cur, results, 1000, i, table_name, log_name)
                else:
                    break
        #插入不足1000行的results
        if len(results) > 0:
            insert_correct(con_cur, results, i, table_name, log_name)

    del_old_data(table_name, log_name)


if __name__ == "__main__":
    # 检测如果当前已经有该脚本在运行,则退出
    if_run=subprocess.run('ps -ef|grep {}|grep -v grep|grep -v "/bin/sh"|wc -l'.format(argv[0]),shell=True,stdout=subprocess.PIPE).stdout
    if if_run.decode().strip('\n') == '1':
        os.chdir(log_dir)
        logs_list=os.listdir(log_dir)
        logs_list=[i for i in logs_list if 'access' in i  and os.path.isfile(i) and i.split('.access')[0] in todo]
        # 并行
        with Pool(len(logs_list)) as p:
            p.map(main_loop,logs_list)
