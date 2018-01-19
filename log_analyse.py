#!/bin/env python3
# coding:utf-8
"""
ljk 20161116(update 20170510)
This script should be put in crontab in every web server.Execute every n minutes.
Collect nginx access log, process it and insert the result into mysql.
"""
from analyse_config import *
from common_func import text_abstract, get_median, get_quartile, special_insert, re
from socket import gethostname
from urllib.parse import unquote
from multiprocessing import Pool
from random import choice
from os import path, listdir, chdir
from time import strftime, localtime
from subprocess import run, PIPE
from sys import exit
import fcntl
import logging
import pymongo

logging.basicConfig(format='%(asctime)s %(levelname)8s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)
logger.setLevel('DEBUG')


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
        logger.warning("Can't process this line: {}".format(line_str))
        return
    else:
        # remote_addr (客户若不经过代理,则可认为用户的真实ip)
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
            logger.warning('$request abnormal: {}'.format(line_str))
            return

        # 状态码, 字节数, 响应时间
        response_code = processed.group('status')
        bytes_sent = processed.group('body_bytes_sent')
        request_time = processed.group('request_time')

        # user_ip, cdn最后节点ip, 以及是否经过F5
        http_x_forwarded_for = processed.group('http_x_forwarded_for')
        ips = http_x_forwarded_for.split()
        # user_ip：用户真实ip
        # cdn_ip: CDN最后节点的ip, ''表示没经过CDN; '-'表示没经过CDN和F5
        if http_x_forwarded_for == '-':
            '''没经过CDN和F5'''
            user_ip = remote_addr
            cdn_ip = '-'
        elif ips[0] == remote_addr:
            '''没经过CDN,经过F5'''
            user_ip = remote_addr
            cdn_ip = ''
        else:
            '''经过CDN和F5'''
            user_ip = ips[0].rstrip(',')
            cdn_ip = ips[-1]

        return {'uri_abs': uri_abs, 'args_abs': args_abs, 'time_local': time_local, 'response_code': response_code,
                'bytes_sent': int(bytes_sent), 'request_time': float(request_time), 'user_ip': user_ip,
                'cdn_ip': cdn_ip, 'request_method': request_method, 'request_uri': request_uri}


def final_uri_dicts(stage_res, log_name, this_h_m):
    """对stage_res里的原始数据进行整合生成每个uri_abs对应的字典,插入到this_minute_doc['request']中, 生成最终存储到mongodb的文档(字典)
    一个uri_abs在this_minute_doc中对应的格式如下"""
    uris = []
    if len(stage_res) > MAX_URI_NUM:
        logger.warning("{}: truncate uri_abs reverse sorted by 'hits' from {} to {} at {} due to the "
                       "MAX_URI_NUM setting".format(log_name, len(stage_res), MAX_URI_NUM, this_h_m))
    for uri_k, uri_v in sorted(stage_res.items(), key=lambda item: item[1]['hits'], reverse=True)[:MAX_URI_NUM]:
        '''取点击量前MAX_URI_NUM的uri_abs'''
        uri_quartile_time = get_quartile(uri_v['time'])
        uri_quartile_bytes = get_quartile(uri_v['bytes'])
        single_uri_dict = {'uri_abs': uri_k,
                           'hits': uri_v['hits'],
                           'min_time': uri_quartile_time[0],
                           'q1_time': round(uri_quartile_time[1], 3),
                           'q2_time': round(uri_quartile_time[2], 3),
                           'q3_time': round(uri_quartile_time[3], 3),
                           'max_time': uri_quartile_time[-1],
                           'time': int(sum(uri_v['time'])),
                           'min_bytes': uri_quartile_bytes[0],
                           'q1_bytes': int(uri_quartile_bytes[1]),
                           'q2_bytes': int(uri_quartile_bytes[2]),
                           'q3_bytes': int(uri_quartile_bytes[3]),
                           'max_bytes': uri_quartile_bytes[-1],
                           'bytes': sum(uri_v['bytes']),
                           'args': [],
                           'max_time_request': stage_res[uri_k]['max_time_request'],
                           'max_bytes_request': stage_res[uri_k]['max_bytes_request'],
                           'response_code': stage_res[uri_k]['response_code']}
        if len(uri_v['args']) > MAX_ARG_NUM:
            logger.warning("{}:{} truncate arg_abs reverse sorted by 'hits' from {} to {} at {} due to the "
                           "MAX_ARG_NUM setting".format(log_name, uri_k, len(stage_res), MAX_ARG_NUM, this_h_m))
        for arg_k, arg_v in sorted(uri_v['args'].items(), key=lambda item: item[1]['hits'], reverse=True)[:MAX_ARG_NUM]:
            '''取点击量前MAX_ARG_NUM的args_abs'''
            arg_quartile_time = get_quartile(arg_v['time'])
            arg_quartile_bytes = get_quartile(arg_v['bytes'])
            single_arg_dict = {'args_abs': arg_k,
                               'hits': arg_v['hits'],
                               'min_time': arg_quartile_time[0],
                               'q1_time': round(arg_quartile_time[1], 3),
                               'q2_time': round(arg_quartile_time[2], 3),
                               'q3_time': round(arg_quartile_time[3], 3),
                               'max_time': arg_quartile_time[-1],
                               'time': int(sum(arg_v['time'])),
                               'min_bytes': arg_quartile_bytes[0],
                               'q1_bytes': int(arg_quartile_bytes[1]),
                               'q2_bytes': int(arg_quartile_bytes[2]),
                               'q3_bytes': int(arg_quartile_bytes[3]),
                               'max_bytes': arg_quartile_bytes[-1],
                               'bytes': sum(arg_v['bytes']),
                               'method': arg_v['method'],
                               'response_code': stage_res[uri_k]['args'][arg_k]['response_code']}
            single_uri_dict['args'].append(single_arg_dict)
        uris.append(single_uri_dict)
    return uris


def append_line_to_stage(line_res, stage_res, line_str):
    """将每行的分析结果(line_res)追加进(stage_res)字典
    line_str: 日志行原始数据"""
    uri_abs = line_res['uri_abs']
    args_abs = line_res['args_abs']
    # 将该行数据汇总至临时字典
    if uri_abs in stage_res:
        # 记录最大时间和最大字节的请求
        if line_res['request_time'] > stage_res[uri_abs]['time'][-1]:
            stage_res[uri_abs]['max_time_request'] = line_str
            if line_res['bytes_sent'] > stage_res[uri_abs]['bytes'][-1]:
                stage_res[uri_abs]['max_bytes_request'] = "same as max_time_request"
        else:
            if line_res['bytes_sent'] > stage_res[uri_abs]['bytes'][-1]:
                stage_res[uri_abs]['max_bytes_request'] = line_str

        special_insert(stage_res[uri_abs]['time'], line_res['request_time'])
        special_insert(stage_res[uri_abs]['bytes'], line_res['bytes_sent'])
        stage_res[uri_abs]['hits'] += 1
        # http响应码
        if line_res['response_code'] in stage_res[uri_abs]['response_code']:
            stage_res[uri_abs]['response_code'][line_res['response_code']] += 1
        else:
            stage_res[uri_abs]['response_code'][line_res['response_code']] = 1
    else:
        stage_res[uri_abs] = {'time': [line_res['request_time']],
                              'bytes': [line_res['bytes_sent']],
                              'hits': 1,
                              'args': {},
                              'max_time_request': line_str,
                              'max_bytes_request': line_str,
                              'response_code': {line_res['response_code']: 1}}
    stage_res['minute_total_bytes'] += line_res['bytes_sent']
    stage_res['minute_total_time'] += line_res['request_time']

    # 将args数据汇总到临时字典
    if args_abs in stage_res[uri_abs]['args']:
        stage_res[uri_abs]['args'][args_abs]['time'].append(line_res['request_time'])
        stage_res[uri_abs]['args'][args_abs]['bytes'].append(line_res['bytes_sent'])
        stage_res[uri_abs]['args'][args_abs]['hits'] += 1
        # http响应码
        if line_res['response_code'] in stage_res[uri_abs]['args'][args_abs]['response_code']:
            stage_res[uri_abs]['args'][args_abs]['response_code'][line_res['response_code']] += 1
        else:
            stage_res[uri_abs]['args'][args_abs]['response_code'][line_res['response_code']] = 1
    else:
        stage_res[uri_abs]['args'][args_abs] = {'time': [line_res['request_time']],
                                                'bytes': [line_res['bytes_sent']],
                                                'hits': 1,
                                                'method': line_res['request_method'],
                                                'response_code': {line_res['response_code']: 1}}


def insert_mongo(mongo_db_obj, results, t_name, l_name, num, date, s_name):
    """插入mongodb, 在主进程中根据函数返回值来决定是否退出对日志文件的循环, 进而退出主进程
    results: mongodb文档
    t_name: 集合名称
    l_name: 日志名称
    num: 当前已入库的行数
    date: 今天日期,格式 170515
    s_name: 主机名"""
    try:
        mongo_db_obj[t_name].insert(results)  # 插入数据
        # 同时插入每台server已处理的行数
        if mongo_db_obj['last_num'].find({'server': server}).count() == 0:
            mongo_db_obj['last_num'].insert({'last_num': num, 'date': date, 'server': s_name})
        else:
            mongo_db_obj['last_num'].update({'server': server}, {'$set': {'last_num': num, 'date': date}})
        return True
    except Exception as err:
        logger.error('{}: insert data error: {}'.format(l_name, err))
    finally:
        mongo_client.close()


def get_prev_num(l_name):
    """取得本server今天已入库的行数
    l_name:日志文件名"""
    try:
        tmp = mongo_db['last_num'].find({'date': today, 'server': server}, {'last_num': 1, '_id': 0})
        if tmp.count() == 1:
            return tmp.next()['last_num']
        elif tmp.count() == 0:
            return 0
        else:
            logger.error("{}: more than one 'last_num' record of {} at {}, skip".format(l_name, server, today))
    except Exception as err:
        logger.error("{}: get 'last_num' of {} at {} error, skip: {}".format(l_name, server, today, err))


def del_old_data(l_name):
    """删除N天前的数据, 默认为LIMIT"""
    col_name = mongo_db.collection_names()
    del_col = sorted(col_name, reverse=True)[LIMIT:] if len(col_name) > LIMIT else []
    try:
        for col in del_col:
            mongo_db.drop_collection(col)
    except Exception as err:
        logger.error("{}: delete collections before {} days error: {}".format(l_name, LIMIT, err))


def main(log_name):
    """log_name:日志文件名"""
    invalid = 0  # 无效的请求数
    stage_res = {'minute_total_bytes': 0, 'minute_total_time': 0}  # 存储处理过程中用于保存一分钟内的各项原始数据
    # 下面3个变量用于生成mongodb的_id
    # 当前处理的一分钟(亦即mongodb文档_id键的一部分,初始为''),格式: 0101(1时1分).(对日志数据以分钟为粒度进行处理分析)
    this_h_m = ''
    random_char = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'
    month_dict = {'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04', 'May': '05', 'Jun': '06',
                  'Jul': '07', 'Aug': '08', 'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12'}
    # mongodb 中的库名
    mongo_db_name = log_name.split('.access')[0].replace('.', '')  # 将域名中的.去掉
    my_connect(mongo_db_name)
    # 开始处理逻辑
    # 当前日志文件总行数
    cur_num = int(run('wc -l {}'.format(log_dir + log_name), shell=True, stdout=PIPE, universal_newlines=True).stdout.split()[0])
    # 上一次处理到的行数
    prev_num_outer = get_prev_num(log_name)
    if prev_num_outer is None:
        return
    # 根据当前行数和mongodb中记录的last_num对比, 决定本次要处理的行数范围
    n = 0
    with open(log_name) as fp:
        for line_str in fp:
            n += 1
            if n <= prev_num_outer:
                continue
            elif n > cur_num:
                break
            # 开始处理
            line_res = process_line(line_str)
            if not line_res:
                invalid += 1
                continue
            date_time = line_res['time_local'].split(':')
            date = date_time[0]
            hour = date_time[1]
            minute = date_time[2]

            # 分钟粒度交替时: 从临时字典中汇总上一分钟的结果并将其入库
            if this_h_m != '' and this_h_m != hour + minute:
                prev_num_inner = get_prev_num(log_name)
                this_minute_doc = {
                    '_id': y_m_d + this_h_m + '-' + choice(random_char) + choice(random_char) + '-' + server,
                    'total_hits': n - 1 - prev_num_inner,
                    'invalid_hits': invalid,
                    'total_bytes': stage_res.pop('minute_total_bytes'),
                    'total_time': round(stage_res.pop('minute_total_time'), 3),
                    'requests': []}
                this_minute_doc['requests'].extend(final_uri_dicts(stage_res, log_name, this_h_m))
                # 执行插入操作(每分钟的最终结果)
                if not insert_mongo(mongo_db, this_minute_doc, y_m_d, log_name, n-1, y_m_d, server):
                    break
                # 清空临时字典stage_res和invalid
                stage_res = {'minute_total_bytes': 0, 'minute_total_time': 0}
                invalid = 0
                logger.info('{} processed to {}'.format(log_name, line_res['time_local']))

            append_line_to_stage(line_res, stage_res, line_str)
            # 以下3行用于生成mongodb中文档的_id
            d_m_y = date.split('/')
            y_m_d = d_m_y[2][2:] + month_dict[d_m_y[1]] + d_m_y[0]  # 作为mongodb库里的集合的名称(每天一个集合)
            this_h_m = hour + minute
            if y_m_d != today:
                logger.error("{}: not today's log, exit".format(log_name))
                break
    del_old_data(log_name)


if __name__ == "__main__":
    server = gethostname()  # 主机名
    today = strftime('%y%m%d', localtime())  # 今天日期
    log_pattern_obj = re.compile(log_pattern)
    request_uri_pattern_obj = re.compile(request_uri_pattern)

    with open('/tmp/test_singleton', 'wb') as f:
        try:
            fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            exit(11)
        # 以上5行为实现单例模式
        chdir(log_dir)
        logs_list = [i for i in listdir(log_dir) if
                     'access' in i and path.isfile(i) and i.split('.access')[0] in todo]
        if len(logs_list) > 0:
            try:
                with Pool(len(logs_list)) as p:
                    p.map(main, logs_list)
            except KeyboardInterrupt:
                exit(10)
