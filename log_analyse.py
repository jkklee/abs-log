#!/bin/env python3
# -*- coding:utf-8 -*-
# Collect nginx access log, process it and insert the result into mongodb.
# This script should be put in crontab in every web server.Execute every n minutes.
"""
ljk 20161116(update 20170510)
This script should be put in crontab in every web server.Execute every n minutes.
Collect nginx access log, process it and insert the result into mysql.
"""
from config import *
from common.common import *
from socket import gethostname
from multiprocessing import Pool
from random import choice
from os import path, listdir, chdir
from subprocess import run, PIPE
from sys import exit
import fcntl
import logging

logging.basicConfig(format='%(asctime)s %(levelname)8s: %(message)s', datefmt='%y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)
logger.setLevel('INFO')


def my_connect(db_name):
    """获得MongoClient对象,进而获得Database(MongoClient）对象
    db_name:mongodb的库名(不同站点对应不同的库名)"""
    global mongo_db
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
    # remote_addr字段在有反向代理的情况下多数时候是无意义的(仅代表反向代理的ip),
    # 除非：以nginx为例,配置了set_real_ip_from和real_ip_header指令
    remote_addr = processed.group('remote_addr')
    time_local = processed.group('time_local')

    # 处理uri和args
    request = processed.group('request')
    request_further = request_uri_pattern_obj.search(request)
    if not request_further:
        logger.warning('$request abnormal: {}'.format(line_str))
        return
    request_method = request_further.group('request_method')
    request_uri = request_further.group('request_uri')
    # 对uri和args进行抽象化
    uri_abs, args_abs = text_abstract(request_uri, site_name)

    # 状态码, 字节数, 响应时间
    response_code = processed.group('status')
    bytes_sent = processed.group('body_bytes_sent')
    try:
        request_time = processed.group('request_time')
    except IndexError:
        '''正则中无(?P <request_time>.* ?)段'''
        request_time = None
    try:
        http_x_forwarded_for = processed.group('http_x_forwarded_for')
        # 通过remote_addr和user_ip可分析出请求来源的三种情况
        # 1.直达server(last_cdn_ip='-',user_ip='-',)
        # 2.未经cdn,直达reverse_proxy(remote_addr == user_ip)
        # 3.经cdn(remote_addr != user_ip)
        ips = http_x_forwarded_for.split()
        user_ip = ips[0].rstrip(',')
        last_cdn_ip = ips[-1]
    except IndexError:
        '''正则中无(?P<http_x_forwarded_for>.*)段'''
        user_ip = None
        last_cdn_ip = None

    return {'uri_abs': uri_abs, 'args_abs': args_abs, 'time_local': time_local, 'response_code': response_code,
            'bytes_sent': int(bytes_sent), 'request_time': float(request_time), 'remote_addr': remote_addr,
            'user_ip': user_ip, 'last_cdn_ip': last_cdn_ip, 'request_method': request_method, 'request_uri': request_uri}


def get_log_date(fp):
    """从日志文件中读取第一行, 获取日志产生的日期时间"""
    line_res = process_line(fp.readline().rstrip('\n'))
    date = line_res['time_local'].split(':')[0]
    d_m_y = date.split('/')
    y_m_d = d_m_y[2][2:] + month_dict[d_m_y[1]] + d_m_y[0]
    fp.seek(0)
    return date, y_m_d


def final_uri_dicts(main_stage, log_name, this_h_m):
    """对main_stage里的原始数据进行整合生成每个uri_abs对应的字典,插入到minute_main_doc['request']中, 生成最终存储到mongodb的文档
    一个uri_abs在minute_main_doc中对应的格式如下"""
    uris = []
    if len(main_stage) > URI_STORE_MAX_NUM:
        logger.warning("{} truncate uri_abs sorted by 'hits' from {} to {} at {}".format(log_name, len(main_stage), URI_STORE_MAX_NUM, this_h_m))
    for uri_k, uri_v in sorted(main_stage.items(), key=lambda item: item[1]['hits'], reverse=True)[:URI_STORE_MAX_NUM]:
        '''取点击量前URI_STORE_MAX_NUM的uri_abs'''
        if uri_v['hits'] < URI_STORE_MIN_HITS:
            break
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
                           'ips': []}
        for arg_k, arg_v in uri_v['args'].items():
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
                               'error_code': main_stage[uri_k]['args'][arg_k]['error_code']}
            single_uri_dict['args'].append(single_arg_dict)

        def add_ip_statistics(ip_type):
            """将种类型ip的统计信息加入到single_uri_dict字典
            ip_type: user_ip_via_cdn, last_cdn_ip, user_ip_via_proxy, remote_addr"""
            nonlocal single_uri_dict
            if len(uri_v[ip_type]) > IP_STORE_MAX_NUM:
                logger.warning("{} {} truncate user_ip_via_cdn sorted by 'hits' from {} to {} at {}".format(
                    log_name, uri_k, len(main_stage[uri_k][ip_type]), IP_STORE_MAX_NUM, this_h_m))
            for ip_k, ip_v in sorted(uri_v[ip_type].items(), key=lambda item: item[1]['hits'], reverse=True)[:IP_STORE_MAX_NUM]:
                '''取ip类型为ip_type的统计中点击量前IP_STORE_MAX_NUM的user_ip_via_cdn'''
                if ip_v['hits'] < IP_STORE_MIN_HITS:
                    break
                single_ip_dict = {'ip': ip_k, 'hits': ip_v['hits'], 'time': round(ip_v['time'], 3),
                                  'bytes': ip_v['bytes'], 'type': ip_type}
                single_uri_dict['ips'].append(single_ip_dict)
        add_ip_statistics('user_ip_via_cdn')
        add_ip_statistics('last_cdn_ip')
        add_ip_statistics('user_ip_via_proxy')
        add_ip_statistics('remote_addr')

        uris.append(single_uri_dict)
    return uris


def append_line_to_main_stage(line_res, main_stage):
    """将每行的分析结果(line_res)追加进(main_stage)字典"""
    uri_abs = line_res['uri_abs']
    args_abs = line_res['args_abs']
    user_ip = line_res['user_ip']
    last_cdn_ip = line_res['last_cdn_ip']
    remote_addr = line_res['remote_addr']

    # 将该行数据汇总至临时字典
    if uri_abs in main_stage:
        special_insert_list(main_stage[uri_abs]['time'], line_res['request_time'])
        special_insert_list(main_stage[uri_abs]['bytes'], line_res['bytes_sent'])
        main_stage[uri_abs]['hits'] += 1
    else:
        main_stage[uri_abs] = {'time': [line_res['request_time']],
                               'bytes': [line_res['bytes_sent']],
                               'hits': 1,
                               'args': {},
                               'user_ip_via_cdn': {},
                               'last_cdn_ip': {},
                               'user_ip_via_proxy': {},
                               'remote_addr': {}}
    # 将args数据汇总到临时字典
    if args_abs in main_stage[uri_abs]['args']:
        main_stage[uri_abs]['args'][args_abs]['time'].append(line_res['request_time'])
        main_stage[uri_abs]['args'][args_abs]['bytes'].append(line_res['bytes_sent'])
        main_stage[uri_abs]['args'][args_abs]['hits'] += 1
        # http错误码
        if int(line_res['response_code']) >= 400:
            special_update_dict(main_stage[uri_abs]['args'][args_abs]['error_code'], line_res['response_code'], 1)
    else:
        main_stage[uri_abs]['args'][args_abs] = {'time': [line_res['request_time']],
                                                 'bytes': [line_res['bytes_sent']],
                                                 'hits': 1,
                                                 'method': line_res['request_method'],
                                                 'error_code': {line_res['response_code']: 1} if int(line_res['response_code']) >= 400 else {}}
    # 将ip信息汇总到临时字典
    if user_ip != '-' and user_ip != last_cdn_ip:
        '''come from cdn'''
        main_stage['source']['from_cdn']['hits'] += 1
        main_stage['source']['from_cdn']['bytes'] += line_res['bytes_sent']
        main_stage['source']['from_cdn']['time'] += line_res['request_time']
        special_update_dict(main_stage[uri_abs]['user_ip_via_cdn'], user_ip, sub_type={}, sub_keys=['hits', 'time', 'bytes'],
                            sub_values=[1, line_res['request_time'], line_res['bytes_sent']])
        special_update_dict(main_stage[uri_abs]['last_cdn_ip'], last_cdn_ip, sub_type={}, sub_keys=['hits', 'time', 'bytes'],
                            sub_values=[1, line_res['request_time'], line_res['bytes_sent']])
    elif user_ip != '-' and user_ip == last_cdn_ip:
        '''come from reverse_proxy'''
        main_stage['source']['from_reverse_proxy']['hits'] += 1
        main_stage['source']['from_reverse_proxy']['bytes'] += line_res['bytes_sent']
        main_stage['source']['from_reverse_proxy']['time'] += line_res['request_time']
        special_update_dict(main_stage[uri_abs]['user_ip_via_proxy'], user_ip, sub_type={}, sub_keys=['hits', 'time', 'bytes'],
                            sub_values=[1, line_res['request_time'], line_res['bytes_sent']])
    elif user_ip == '-' and user_ip == last_cdn_ip:
        '''come from user directly'''
        main_stage['source']['from_client_directly']['hits'] += 1
        main_stage['source']['from_client_directly']['bytes'] += line_res['bytes_sent']
        main_stage['source']['from_client_directly']['time'] += line_res['request_time']
        special_update_dict(main_stage[uri_abs]['remote_addr'], remote_addr, sub_type={}, sub_keys=['hits', 'time', 'bytes'],
                            sub_values=[1, line_res['request_time'], line_res['bytes_sent']])
        

def insert_mongo(mongo_db_obj, bulk_doc, log_name, num, ymdhm):
    """插入mongodb, 在主进程中根据函数返回值来决定是否退出对日志文件的循环, 进而退出主进程
    bulk_doc: 由每分钟文档组成的批量插入的数组
    num: 当前已入库的行数
    ymdhm: 日志中的日期,格式 1705150101(17年05月15日01时01分)
    """
    try:
        mongo_db_obj['main'].insert_many(bulk_doc)  # 插入数据
        mongo_db_obj['last_num'].update({'$and': [{'server': server}, {'date_time': {'$regex': '^'+ymdhm[:6]}}]}, {'$set': {'last_num': num, 'date_time': ymdhm}}, upsert=True)
    except Exception as err:
        logger.error('{} insert data error: {}'.format(log_name, err))
        raise
    finally:
        mongo_client.close()


def get_prev_info(log_name, date):
    """取得本server在date这一天已入库的行数和日志中最后的时间
    date: 日志中的日期,格式 170515"""
    tmp = mongo_db['last_num'].aggregate([{'$project': {'server': 1, 'last_num': 1, 'date_time': 1, 'date': {'$substrBytes': ["$date_time", 0, 6]}}},
                                          {'$match': {'date': date, 'server': server}}])
    try:
        res = tmp.next()
        return res['last_num'], res['date_time']
    except StopIteration:
        return 0, ''
    except Exception as err:
        logger.error("{} get 'last_num' of {} at {} error, skip: {}".format(log_name, server, date, err))
        raise


def del_old_data(log_name, date, h_m):
    """删除N天前的数据, 默认为LIMIT
    date: 日期, 格式180315
    h_m: 当前hour and minute(到23:59时执行清理操作)"""
    if h_m != '2359':
        return
    min_date = get_delta_date(date, LIMIT)
    try:
        mongo_db['main'].remove({'_id': {'$lt': min_date}})
        mongo_db['last_num'].remove({'date_time': {'$lt': min_date}})
    except Exception as err:
        logger.error("{} delete documents before {} days error: {}".format(log_name, LIMIT, err))


def main(log_name):
    """log_name:日志文件名"""
    global site_name
    site_name = log_name.split('.access')[0].replace('.', '')  # 也即mongodb 中的库名(将域名中的.去掉)
    invalid = 0  # 无效的请求数
    # main_stage存储处理过程中用于保存一分钟内的各项原始数据
    main_stage = {'source': {'from_cdn': {'hits': 0, 'bytes': 0, 'time': 0},
                             'from_reverse_proxy': {'hits': 0, 'bytes': 0, 'time': 0},
                             'from_client_directly': {'hits': 0, 'bytes': 0, 'time': 0}}}
    bulk_documents = []  # 作为每分钟文档的容器, 当累积100个文档时, 进行一次批量插入
    # 当前处理的一分钟(亦即mongodb文档_id键的一部分,初始为''),格式: 0101(1时1分).(对日志数据以分钟为粒度进行处理分析)
    this_h_m = ''
    my_connect(site_name)

    # 开始处理逻辑
    fp = open(log_name)
    log_date_ori, log_date = get_log_date(fp)
    # 当前日志文件总行数
    cur_num = int(run('wc -l {}'.format(log_dir + log_name), shell=True, stdout=PIPE, universal_newlines=True).stdout.split()[0])
    try:
        # 上一次处理到的行数和时间
        last_num, last_date_time = get_prev_info(log_name, log_date)
    except:
        return
    if cur_num <= last_num:
        return
    # 根据当前行数和mongodb中记录的last_num对比, 决定本次要处理的行数范围
    n = processed_num = 0
    for line_str in fp:
        n += 1
        processed_num += 1
        if n <= prev_num:
            processed_num -= 1
            continue
        elif n > cur_num:
            break
        # 开始处理
        line_res = process_line(line_str)
        if not line_res:
            invalid += 1
            continue
        time_local_list = line_res['time_local'].split(':')
        date, hour, minute = time_local_list[0:3]

        # 分钟粒度交替时: 从临时字典中汇总上一分钟的结果并将其入库
        if this_h_m != '' and this_h_m != hour + minute:
            date_time = y_m_d + this_h_m
            minute_main_doc = {
                '_id': date_time + '-' + choice(random_char) + choice(random_char) + '-' + server,
                'total_hits': processed_num,
                'invalid_hits': invalid,
                'total_bytes': main_stage['source']['from_cdn']['bytes'] + main_stage['source']['from_reverse_proxy']['bytes'] + main_stage['source']['from_client_directly']['bytes'],
                'total_time': round(main_stage['source']['from_cdn']['time'] + main_stage['source']['from_reverse_proxy']['time'] + main_stage['source']['from_client_directly']['time'], 3),
                'requests': [],
                'source': main_stage.pop('source')}  # 此处必须用pop，以保证下一行中引用的main_stage只包含以uri_abs为key的结构
            minute_main_doc['requests'].extend(final_uri_dicts(main_stage, log_name, this_h_m))
            bulk_documents.append(minute_main_doc)
            if len(bulk_documents) == 100:  # bulk_documents中累积100个文档之后再执行一次批量插入
                try:
                    insert_mongo(mongo_db, bulk_documents, log_name, n, date_time)
                    bulk_documents = []
                except:
                    return  # 这里用exit无法退出主程序
            # 清空临时字典main_stage, invalid, processed_num
            processed_num = 0
            main_stage = {'source': {'from_cdn': {'hits': 0, 'bytes': 0, 'time': 0},
                                     'from_reverse_proxy': {'hits': 0, 'bytes': 0, 'time': 0},
                                     'from_client_directly': {'hits': 0, 'bytes': 0, 'time': 0}}}
            invalid = 0
            logger.info('{} processed to {}'.format(log_name, line_res['time_local']))

        # 不到分钟粒度交替时:
        # y_m_d和this_h_m行用于生成mongodb中文档的_id
        if date == log_date_ori:
            y_m_d = log_date
        else:
            # 对应一个日志文件中包含跨天日志内容的情况
            d_m_y = date.split('/')
            y_m_d = d_m_y[2][2:] + month_dict[d_m_y[1]] + d_m_y[0]
        this_h_m = hour + minute
        # 调用append_line_to_main_stage函数处理每一行
        append_line_to_main_stage(line_res, main_stage)

    # 对最后一部分未能满足分钟交替条件的日志进行处理
    if processed_num > 0:
        date_time = y_m_d + this_h_m
        minute_main_doc = {
            '_id': date_time + '-' + choice(random_char) + choice(random_char) + '-' + server,
            'total_hits': processed_num,
            'invalid_hits': invalid,
            'total_bytes': main_stage['source']['from_cdn']['bytes'] + main_stage['source']['from_reverse_proxy']['bytes'] + main_stage['source']['from_client_directly']['bytes'],
            'total_time': round(main_stage['source']['from_cdn']['time'] + main_stage['source']['from_reverse_proxy']['time'] + main_stage['source']['from_client_directly']['time'], 3),
            'requests': [],
            'source': main_stage.pop('source')}
        minute_main_doc['requests'].extend(final_uri_dicts(main_stage, log_name, this_h_m))
        bulk_documents.append(minute_main_doc)
        try:
            insert_mongo(mongo_db, bulk_documents, log_name, n, date_time)
        except:
            return
    if bulk_documents:
        try:
            insert_mongo(mongo_db, bulk_documents, log_name, n, date_time)
        except:
            return

    del_old_data(log_name, y_m_d, this_h_m)


if __name__ == "__main__":
    come_from_cdn = come_from_proxy = come_from_user = 0
    server = gethostname()  # 主机名
    log_pattern_obj = re.compile(log_pattern)
    request_uri_pattern_obj = re.compile(request_uri_pattern)

    with open('/tmp/test_singleton', 'wb') as f:
        try:
            fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            exit(11)
        # 以上5行为实现单例执行
        chdir(log_dir)
        # 日志文件名格式必须为xxx.access.log, 以便取得app(站点)名称xxx
        logs_list = [i for i in listdir(log_dir) if '.access' in i and path.isfile(i) and i.split('.access')[0] in todo]
        if len(logs_list) > 0:
            try:
                with Pool(len(logs_list)) as p:
                    p.map(main, logs_list)
            except KeyboardInterrupt:
                exit(10)
