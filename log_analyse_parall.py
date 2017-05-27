#!/bin/env python3
# coding:utf-8
"""
ljk 20161116(update 20170510)
This script should be put in crontab in every web server.Execute every n minutes.
Collect nginx access log, process it and insert the result into mysql.
"""
import os
import re
import subprocess
import time
import pymongo
from sys import argv, exit
from socket import gethostname
from urllib.parse import unquote
from multiprocessing import Pool
from functools import wraps
from random import choice

# ---------- 自定义部分 ----------#
# -----日志格式
# 利用非贪婪匹配和分组匹配，需要严格参照日志定义中的分隔符和引号
log_pattern = r'^(?P<remote_addr>.*?) - \[(?P<time_local>.*?) \+0800\] "(?P<request>.*?)"' \
              r' (?P<status>.*?) (?P<body_bytes_sent>.*?) (?P<request_time>.*?)' \
              r' "(?P<http_referer>.*?)" "(?P<http_user_agent>.*?)" - (?P<http_x_forwarded_for>.*)$'
# request的正则，其实是由 "request_method request_uri server_protocol"三部分组成
request_uri_pattern = r'^(?P<request_method>(GET|POST|HEAD|DELETE|PUT|OPTIONS)?) ' \
                      r'(?P<request_uri>.*?) ' \
                      r'(?P<server_protocol>.*)$'

# -----日志相关
log_dir = '/data/nginx_log/'
# 要处理的站点（可随需要向list中添加）
todo = ['www', 'm', 'user']
exclude_ip = []

# -----mongo连接
mongo_host = '172.16.2.24'
mongo_port = 27017
# mongodb存储结构为每个站点对应一个库, 每天对应一个集合, 日志文件每分钟的数据分析合并后放到一个文档里(分析粒度达到分钟级)

# -----结果入库及存储
max_uri_num = 300
max_arg_num = 50

# ---------- 自定义部分结束 ----------#


def timer(func):
    @wraps(func)
    def inner_func(*args, **kwargs):
        t0 = time.time()
        result_ = func(*args, **kwargs)
        t1 = time.time()
        print("Time running %s: %s seconds" % (func.__name__, str(t1 - t0)))
        return result_

    return inner_func


def my_connect(db_name):
    """获得MongoClient对象,进而获得Database(MongoClient）对象
    db_name:mongodb的库名(不同站点对应不同的库名)"""
    global mongo_client, mongo_db
    mongo_client = pymongo.MongoClient(mongo_host, mongo_port)
    mongo_db = mongo_client[db_name]

def process_line(line_str):
    """
    处理每一行记录
    line_str: 该行日志的原始形式
    """
    processed = log_pattern_obj.search(line_str)
    if not processed:
        '''如果正则根本就无法匹配该行记录时'''
        print("Can't process this line: {}".format(line_str))
        return
    else:
        # remote_addr (客户若不经过代理，则可认为用户的真实ip)
        remote_addr = processed.group('remote_addr')
        time_local = processed.group('time_local')

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
        else:
            print('$request abnormal: {}'.format(line_str))
            return

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

        return {'uri_abs': uri_abs, 'args_abs': args_abs, 'time_local': time_local, 'response_code': response_code,
                'bytes_sent': int(bytes_sent), 'request_time': float(request_time), 'user_ip': user_ip,
                'cdn_ip': cdn_ip, 'request_method': request_method, 'request_uri': request_uri}


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
        step1 = re.sub(r'/[0-9]+\.', r'/?.', text)
        step2 = re.sub(r'/[0-9]+$', r'/?', step1)
        while re.search(r'/[0-9]+/', step2):
            step2 = re.sub(r'/[0-9]+/', r'/?/', step2)
        return step2
    elif what == 'args':
        return re.sub('=[^&=]+', '=?', text)


def insert_mongo(mongo_db_obj, results, t_name, l_name, num, date, s_name):
    """插入mongodb, 在主进程中根据函数返回值来决定是否退出对日志文件的循环，进而退出主进程
    results: mongodb文档
    t_name: 集合名称
    l_name: 日志名称
    num: 当前已入库的行数
    date: 今天日期,格式 20170515
    s_name: 主机名"""
    try:
        mongo_db_obj[t_name].insert(results)  # 插入数据
        # 同时插入每台server已处理的行数
        mongo_db_obj['last_num'].update({}, {'$set': {'last_num': num, 'date': date, 'server': s_name}}, upsert=True)
        return True
    except Exception as err:
        print('{}: 插入数据时出错...'.format(l_name))
        print('Error: {}\n'.format(err))
        mongo_client.close()


def get_prev_num(l_name):
    """取得今天已入库的行数 
    l_name:日志文件名"""
    try:
        tmp = mongo_db['last_num'].find({'date': today, 'server': server}, {'last_num': 1, '_id': 0})
        if tmp.count() == 1:
            return tmp.next()['last_num']
        elif tmp.count() == 0:
            return 0
        else:
            print('Error:"{}"未取得已入库的行数,本次跳过\n'.format(l_name))
    except Exception as err:
        print('Error: {}'.format(err))
        print('Error:"{}"未取得已入库的行数,本次跳过\n'.format(l_name))


@timer
def del_old_data(t_name, l_name, n=2):
    """删除n天前的数据,n默认为2"""
    try:
        pass
    except pymysql.err.MySQLError as err:
        print('{}    Error: {}'.format(l_name, err))
        print('未能删除{}天前的数据...\n'.format(n))


@timer
def main_loop(log_name):
    """log_name:日志文件名"""
    invalid = 0  # 无效的请求数
    tmp_res = {'minute_total_bytes': 0}  # 存储处理过程中用于保存一分钟内的各项原始数据
    # 下面3个变量用于生成mongodb的_id
    # 当前处理的一分钟(亦即mongodb文档_id键的一部分,初始为''),格式: 0101(1点1分).(对日志数据以分钟为粒度进行处理分析)
    this_h_m = ''
    random_char = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'
    month_dict = {'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04', 'May': '05', 'Jun': '06',
                  'Jul': '07', 'Aug': '08', 'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12'}

    # mongodb 中的库名
    mongo_db_name = log_name.split('.access')[0].replace('.', '')
    my_connect(mongo_db_name)

    # 开始处理逻辑
    # 当前日志文件总行数
    num = int(subprocess.run('wc -l {}'.format(log_dir + log_name), shell=True, stdout=subprocess.PIPE,
                             universal_newlines=True).stdout.split()[0])
    # 上一次处理到的行数
    prev_num_outer = get_prev_num(log_name)
    if prev_num_outer is not None:
        # 根据当前行数和上次处理之后记录的行数对比,来决定本次要处理的行数范围
        i = 0
        with open(log_name) as fp:
            for line in fp:
                i += 1
                if i <= prev_num_outer:
                    continue
                elif i > num:
                    break
                # 接下来对本次应该处理的并且正常的行进行处理
                line_res = process_line(line)
                if not line_res:
                    invalid += 1
                    continue
                date_time = line_res['time_local'].split(':')
                hour = date_time[1]
                minute = date_time[2]
                if this_h_m != '' and this_h_m != hour + minute:
                    '''分钟粒度交替时: 从临时字典中汇总上一分钟的结果并将其入库'''
                    # 存储一分钟区间内的最终汇总结果
                    prev_num_inner = get_prev_num(log_name)
                    this_minute_res = {
                        '_id': y_m_d + this_h_m + '-' + choice(random_char) + choice(random_char) + '-' + server, 
                        'total_hits': i - 1 - prev_num_inner,
                        'invalid_hits': invalid,
                        'total_bytes': tmp_res.pop('minute_total_bytes'),
                        'requests': []}

                    # 对tmp_res里的原始数据进行整合,插入到this_minute_res['request']中,生成最终存储到mongodb的文档(字典)
                    # 最终一个uri_abs在this_minute_res中对应的格式如下
                    for uri_k, uri_v in sorted(tmp_res.items(), key=lambda item: item[1]['hits'], reverse=True)[
                                        :max_uri_num]:
                        '''取点击量前max_uri_num的uri_abs'''
                        single_uri_dict = {'uri_abs': uri_k,
                                           'hits': uri_v['hits'],
                                           'max_time': max(uri_v['time']),
                                           'min_time': min(uri_v['time']),
                                           'avg_time': format(sum(uri_v['time']) / len(uri_v['time']), '.2f'),
                                           'max_bytes': max(uri_v['bytes']),
                                           'min_bytes': min(uri_v['bytes']),
                                           'bytes': sum(uri_v['bytes']),
                                           'avg_bytes': format(sum(uri_v['bytes']) / len(uri_v['bytes']), '.2f'),
                                           'args': []}
                        for arg_k, arg_v in sorted(uri_v['args'].items(), key=lambda item: item[1]['hits'],
                                                   reverse=True)[:max_arg_num]:
                            '''取点击量前max_arg_num的args_abs'''
                            single_arg_dict = {'args_abs': arg_k,
                                               'hits': arg_v['hits'],
                                               'max_time': max(arg_v['time']),
                                               'min_time': min(arg_v['time']),
                                               'avg_time': format(sum(arg_v['time']) / len(arg_v['time']), '.2f'),
                                               'max_bytes': max(arg_v['bytes']),
                                               'min_bytes': min(arg_v['bytes']),
                                               'bytes': sum(arg_v['bytes']),
                                               'avg_bytes': format(sum(arg_v['bytes']) / len(arg_v['bytes']), '.2f'),
                                               'method': arg_v['method'],
                                               'example': arg_v['example']}
                            single_uri_dict['args'].append(single_arg_dict)
                        this_minute_res['requests'].append(single_uri_dict)

                    # 执行插入操作(每分钟的最终结果)
                    if not insert_mongo(mongo_db, this_minute_res, y_m_d, log_name, i - 1, y_m_d, server):
                        break
                    # 清空临时字典tmp_res和invalid
                    tmp_res = {'minute_total_bytes': 0}
                    invalid = 0
                    print('{} 处理至 {}'.format(log_name, line_res['time_local']))

                # 将每行的分析结果"追加"进tmp_res字典
                uri_abs = line_res['uri_abs']
                args_abs = line_res['args_abs']
                if uri_abs in tmp_res:
                    '''将uri数据汇总至临时字典'''
                    tmp_res[uri_abs]['time'].append(line_res['request_time'])
                    tmp_res[uri_abs]['bytes'].append(line_res['bytes_sent'])
                    tmp_res[uri_abs]['hits'] += 1
                else:
                    tmp_res[uri_abs] = {'time': [line_res['request_time']],
                                        'bytes': [line_res['bytes_sent']],
                                        'hits': 1,
                                        'args': {}}
                tmp_res['minute_total_bytes'] += line_res['bytes_sent']

                if args_abs in tmp_res[uri_abs]['args']:
                    '''将args数据汇总到临时字典'''
                    tmp_res[uri_abs]['args'][args_abs]['time'].append(line_res['request_time'])
                    tmp_res[uri_abs]['args'][args_abs]['bytes'].append(line_res['bytes_sent'])
                    tmp_res[uri_abs]['args'][args_abs]['hits'] += 1
                else:
                    tmp_res[uri_abs]['args'][args_abs] = {'time': [line_res['request_time']],
                                                          'bytes': [line_res['bytes_sent']],
                                                          'hits': 1,
                                                          'method': line_res['request_method'],
                                                          'example': line_res['request_uri']}

                date = date_time[0]
                # 以下3行用于生成mongodb中文档的_id
                d_m_y = date.split('/')
                y_m_d = d_m_y[2][2:] + month_dict[d_m_y[1]] + d_m_y[0]  # 作为mongodb库里的集合的名称(每天一个集合)
                this_h_m = hour + minute
                if y_m_d != today:
                    print('日志不是今天的,将退出')
                    break
                    # del_old_data(table_name, log_name)

if __name__ == "__main__":
    # global server, today
    server = gethostname()  # 主机名
    today = time.strftime('%y%m%d', time.localtime())  # 今天日期
    log_pattern_obj = re.compile(log_pattern)
    request_uri_pattern_obj = re.compile(request_uri_pattern)

    # 检测如果当前已经有该脚本在运行,则退出
    if_run = subprocess.run('ps -ef|grep {}|grep -v grep|grep -v "/bin/sh"|wc -l'.format(argv[0]), shell=True,
                            stdout=subprocess.PIPE).stdout
    if if_run.decode().strip('\n') == '1':
        os.chdir(log_dir)
        logs_list = [i for i in os.listdir(log_dir) if
                     'access' in i and os.path.isfile(i) and i.split('.access')[0] in todo]
        if len(logs_list) > 0:
            # 并行
            try:
                with Pool(len(logs_list)) as p:
                    p.map(main_loop, logs_list)
            except KeyboardInterrupt:
                exit(10)
