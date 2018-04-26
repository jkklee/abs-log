#!/bin/env python3
# -*- coding:utf-8 -*-
# Collect nginx access log, process it and insert the result into mongodb.
# This script should put into crontab in every web server. Execute every N minutes.

from config import *
from common.common import *
from socket import gethostname
from multiprocessing import Pool
from random import choice
from os import path, listdir, chdir
from subprocess import run, PIPE
from sys import exit, argv as sys_argv
import fcntl
import logging

if len(sys_argv) == 1:
    argv_log_list = None
elif sys_argv[1] == '-f':
    # 对应指定日志文件的情况(非默认用法)
    argv_log_list = sys_argv[2:]
else:
    print("Usage:\n  log_analyse.py [-f <log_path>...]\n  Note: the format of log file name should be 'xxx.access[.log]'")
    exit(12)

logging.basicConfig(format='%(asctime)s %(levelname)7s: %(message)s', datefmt='%y%m%d %H:%M:%S')
logger = logging.getLogger(__name__)
logger.setLevel(error_level)


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
    processed = log_pattern_obj.match(line_str)
    if not processed:
        # 如果正则无法匹配该行时
        logger.warning("Can't parse line: {}".format(line_str))
        return
    # remote_addr字段在有反向代理的情况下多数时候是无意义的(仅代表反向代理的ip),
    # 除非：以nginx为例,配置了set_real_ip_from和real_ip_header指令
    remote_addr = processed.group('remote_addr')
    time_local = processed.group('time_local')

    # 状态码, 字节数, 响应时间
    response_code = processed.group('status')
    bytes_sent = processed.group('body_bytes_sent')
    try:
        request_time = processed.group('request_time')
    except IndexError:
        # 正则中无(?P <request_time>.* ?)段
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
        # 正则中无(?P<http_x_forwarded_for>.*)段
        user_ip = None
        last_cdn_ip = None

    # 处理uri和args
    request = processed.group('request')
    request_further = request_uri_pattern_obj.search(request)
    if not request_further:
        logger.warning('$request abnormal: {}'.format(line_str))
        return {'uri_abs': 'parse_error', 'args_abs': 'parse_error', 'time_local': time_local, 'response_code': response_code,
                'bytes_sent': int(bytes_sent), 'request_time': float(request_time), 'remote_addr': remote_addr,
                'user_ip': user_ip, 'last_cdn_ip': last_cdn_ip, 'request_method': 'parse_error'}
    request_method = request_further.group('request_method')
    request_uri = request_further.group('request_uri')
    # 对uri和args进行抽象
    uri_abs, args_abs = text_abstract(request_uri, site_name)

    return {'uri_abs': uri_abs, 'args_abs': args_abs, 'time_local': time_local, 'response_code': response_code,
            'bytes_sent': int(bytes_sent), 'request_time': float(request_time), 'remote_addr': remote_addr,
            'user_ip': user_ip, 'last_cdn_ip': last_cdn_ip, 'request_method': request_method}


def get_log_date(fp, log_name):
    """从日志文件获取日志产生的日期时间, 最多读 5 行"""
    n = 1
    while n <= 5:
        n += 1
        line = fp.readline()
        if line:
            line_res = process_line(fp.readline().rstrip('\n'))
        else:
            raise Exception("{} empty file".format(log_name))
        if line_res:
            try:
                date = line_res['time_local'].split(':')[0]
                d_m_y = date.split('/')
                y_m_d = d_m_y[2][2:] + month_dict[d_m_y[1]] + d_m_y[0]
                fp.seek(0)
                return date, y_m_d
            except Exception as err:
                raise Exception('get_log_date() {} {}'.format(log_name, err))
    raise Exception("{} can't parse the first five lines, will exit. check log content and 'log_pattern'".format(log_name))


def get_prev_info(date):
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
        raise Exception("{} get 'last_num' of {} at {} error, will exit: {}".format(site_name, server, date, err))


def insert_mongo(mongo_db_obj, bulk_doc, num, ymdhm):
    """插入mongodb
    bulk_doc: 由每分钟文档组成的批量插入的数组
    num: 当前已入库的行数
    ymdhm: 日志中的日期,格式 1705150101(17年05月15日01时01分)
    """
    try:
        mongo_db_obj['main'].insert_many(bulk_doc)  # 插入数据
        mongo_db_obj['last_num'].update({'$and': [{'server': server}, {'date_time': {'$regex': '^'+ymdhm[:6]}}]}, {'$set': {'last_num': num, 'date_time': ymdhm}}, upsert=True)
    except Exception as err:
        logger.error('{} insert data error: {}'.format(site_name, err))
        raise
    finally:
        mongo_client.close()


def del_old_data(date, h_m):
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
        logger.error("{} delete documents before {} days error: {}".format(site_name, LIMIT, err))


def final_uri_dicts(main_stage, log_name, this_h_m):
    """对main_stage里的原始数据进行整合生成每个uri_abs对应的字典, 插入到minute_main_doc['request']中, 生成最终存储到mongodb的文档"""
    uris = []
    if len(main_stage) > URI_STORE_MAX_NUM:
        logger.debug("{} truncate uri_abs sorted by 'hits' from {} to {} at {}".format(log_name, len(main_stage), URI_STORE_MAX_NUM, this_h_m))
    for uri_k, uri_v in sorted(main_stage.items(), key=lambda item: item[1]['hits'], reverse=True)[:URI_STORE_MAX_NUM]:
        # 取点击量前URI_STORE_MAX_NUM的uri_abs
        if uri_v['hits'] < URI_STORE_MIN_HITS:
            break
        # 先由main_stage[uri_abs][args_abs]字典中计算出main_stage[uri_abs]字典
        for args_abs_value in main_stage[uri_k]['args'].values():
            main_stage[uri_k]['time'].extend(args_abs_value['time'])
            main_stage[uri_k]['bytes'].extend(args_abs_value['bytes'])
        uri_quartile_time = get_quartile(uri_v['time'])
        uri_quartile_bytes = get_quartile(uri_v['bytes'])
        # minute_main_doc['request']列表中一个uri_abs对的应字典格式如下
        single_uri_dict = {'uri_abs': uri_k,
                           'hits': uri_v['hits'],
                           'q2_time': round(uri_quartile_time[2], 3),
                           'q3_time': round(uri_quartile_time[3], 3),
                           'max_time': uri_quartile_time[-1],
                           'time': int(sum(uri_v['time'])),
                           'q2_bytes': int(uri_quartile_bytes[2]),
                           'q3_bytes': int(uri_quartile_bytes[3]),
                           'max_bytes': uri_quartile_bytes[-1],
                           'bytes': sum(uri_v['bytes']),
                           'args': [],
                           'ips': [],
                           'errors': []}
        for arg_k, arg_v in uri_v['args'].items():
            # 生成single_uri_dict['args']列表中的一个args_abs对应的字典single_arg_dict
            arg_quartile_time = get_quartile(arg_v['time'])
            arg_quartile_bytes = get_quartile(arg_v['bytes'])
            single_arg_dict = {'args_abs': arg_k,
                               'hits': arg_v['hits'],
                               'q2_time': round(arg_quartile_time[2], 3),
                               'q3_time': round(arg_quartile_time[3], 3),
                               'max_time': arg_quartile_time[-1],
                               'time': int(sum(arg_v['time'])),
                               'q2_bytes': int(arg_quartile_bytes[2]),
                               'q3_bytes': int(arg_quartile_bytes[3]),
                               'max_bytes': arg_quartile_bytes[-1],
                               'bytes': sum(arg_v['bytes']),
                               'method': arg_v['method']}
            single_uri_dict['args'].append(single_arg_dict)
        for error_k, error_v in uri_v['errors'].items():
            # 生成single_uri_dict['errors']列表中的一个error对应的字典single_error_dict
            error_quartile_time = get_quartile(error_v['time'])
            error_quartile_bytes = get_quartile(error_v['bytes'])
            single_error_dict = {'error_code': error_k,
                                 'hits': error_v['hits'],
                                 'q2_time': round(error_quartile_time[2], 3),
                                 'q3_time': round(error_quartile_time[3], 3),
                                 'max_time': error_quartile_time[-1],
                                 'time': int(sum(error_v['time'])),
                                 'q2_bytes': int(error_quartile_bytes[2]),
                                 'q3_bytes': int(error_quartile_bytes[3]),
                                 'max_bytes': error_quartile_bytes[-1],
                                 'bytes': sum(error_v['bytes']),
                                 'method': error_v['method']
                                 }
            single_uri_dict['errors'].append(single_error_dict)

        def add_ip_statistics(ip_type):
            """将种类型ip的统计信息加入到single_uri_dict字典
            ip_type: user_ip_via_cdn, last_cdn_ip, user_ip_via_proxy, remote_addr"""
            nonlocal single_uri_dict
            if len(uri_v[ip_type]) > IP_STORE_MAX_NUM:
                logger.debug("{} {} truncate user_ip_via_cdn sorted by 'hits' from {} to {} at {}".format(
                    log_name, uri_k, len(main_stage[uri_k][ip_type]), IP_STORE_MAX_NUM, this_h_m))
            for ip_k, ip_v in sorted(uri_v[ip_type].items(), key=lambda item: item[1]['hits'], reverse=True)[:IP_STORE_MAX_NUM]:
                # 取ip类型为ip_type的统计中点击量前IP_STORE_MAX_NUM的user_ip_via_cdn
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
    request_time = line_res['request_time']
    bytes_sent = line_res['bytes_sent']
    uri_abs = line_res['uri_abs']
    args_abs = line_res['args_abs']
    user_ip = line_res['user_ip']
    last_cdn_ip = line_res['last_cdn_ip']
    remote_addr = line_res['remote_addr']
    response_code = line_res['response_code']
    # 将uri_abs数据汇总至临时字典(考虑性能,放到final_uri_dicts()中由args_abs数据计算而来)
    if uri_abs in main_stage:
        main_stage[uri_abs]['hits'] += 1
    else:
        main_stage[uri_abs] = {'time': [], 'bytes': [], 'hits': 1, 'args': {}, 'errors': {}, 'user_ip_via_cdn': {},
                               'last_cdn_ip': {}, 'user_ip_via_proxy': {}, 'remote_addr': {}}
    # 将args_abs数据汇总到临时字典
    if args_abs in main_stage[uri_abs]['args']:
        main_stage[uri_abs]['args'][args_abs]['time'].append(request_time)
        main_stage[uri_abs]['args'][args_abs]['bytes'].append(bytes_sent)
        main_stage[uri_abs]['args'][args_abs]['hits'] += 1
    else:
        main_stage[uri_abs]['args'][args_abs] = {'time': [request_time], 'bytes': [bytes_sent],
                                                 'hits': 1, 'method': line_res['request_method']}
    # 将error信息汇总到临时字典
    if response_code >= '400':
        if response_code in main_stage[uri_abs]['errors']:
            main_stage[uri_abs][response_code]['time'].append(request_time)
            main_stage[uri_abs][response_code]['bytes'].append(bytes_sent)
            main_stage[uri_abs][response_code]['hits'] += 1
        else:
            main_stage[uri_abs][response_code] = {'time': [request_time], 'bytes': [bytes_sent],
                                                  'hits': 1, 'method': line_res['request_method']}
    # 将ip信息汇总到临时字典
    if user_ip != '-' and user_ip != last_cdn_ip:
        # come from cdn
        main_stage['source']['from_cdn']['hits'] += 1
        main_stage['source']['from_cdn']['bytes'] += bytes_sent
        main_stage['source']['from_cdn']['time'] += request_time
        special_update_dict(main_stage[uri_abs]['user_ip_via_cdn'], user_ip, sub_type={},
                            sub_keys=['hits', 'time', 'bytes'], sub_values=[1, request_time, bytes_sent])
        special_update_dict(main_stage[uri_abs]['last_cdn_ip'], last_cdn_ip, sub_type={},
                            sub_keys=['hits', 'time', 'bytes'], sub_values=[1, request_time, bytes_sent])
    elif user_ip != '-' and user_ip == last_cdn_ip:
        # come from reverse_proxy
        main_stage['source']['from_reverse_proxy']['hits'] += 1
        main_stage['source']['from_reverse_proxy']['bytes'] += bytes_sent
        main_stage['source']['from_reverse_proxy']['time'] += request_time
        special_update_dict(main_stage[uri_abs]['user_ip_via_proxy'], user_ip, sub_type={},
                            sub_keys=['hits', 'time', 'bytes'], sub_values=[1, request_time, bytes_sent])
    elif user_ip == '-' and user_ip == last_cdn_ip:
        # come from user directly
        main_stage['source']['from_client_directly']['hits'] += 1
        main_stage['source']['from_client_directly']['bytes'] += bytes_sent
        main_stage['source']['from_client_directly']['time'] += request_time
        special_update_dict(main_stage[uri_abs]['remote_addr'], remote_addr, sub_type={},
                            sub_keys=['hits', 'time', 'bytes'], sub_values=[1, request_time, bytes_sent])


def main(log_name):
    """log_name: 日志文件名"""
    if argv_log_list:
        argv_log_dir = path.dirname(log_name) if path.dirname(log_name) else '.'
        log_name = path.basename(log_name)
        chdir(argv_log_dir)
    else:
        chdir(log_dir)
        if not path.isfile(log_name) or log_name.split('.access')[0] not in todo:
            return
    if '.access' not in log_name:
        logger.error("{}, the format of log file name should be 'xxx.access[.log]'".format(log_name))
        return

    global site_name
    site_name = log_name.split('.access')[0].replace('.', '')  # 即mongodb中的库名(将域名中的.去掉)

    invalid = 0  # 无效请求数
    # main_stage: 处理过程中, 用于保存一分钟内的各项原始数据
    main_stage = {'source': {'from_cdn': {'hits': 0, 'bytes': 0, 'time': 0},
                             'from_reverse_proxy': {'hits': 0, 'bytes': 0, 'time': 0},
                             'from_client_directly': {'hits': 0, 'bytes': 0, 'time': 0}}}
    bulk_documents = []  # 作为每分钟文档的容器, 累积100个文档时, 进行一次批量插入
    this_h_m = ''  # 当前处理的一分钟, 格式: 0101(1时1分)
    my_connect(site_name)

    def generate_bulk_docs(y_m_d):
        """生成每分钟的文档, 存放到bulk_documents中"""
        minute_main_doc = {
            '_id': y_m_d + this_h_m + '-' + choice(random_char) + choice(random_char) + choice(random_char) + '-' + server,
            'total_hits': processed_num,
            'invalid_hits': invalid,
            'total_bytes': main_stage['source']['from_cdn']['bytes'] + main_stage['source']['from_reverse_proxy']['bytes'] + main_stage['source']['from_client_directly']['bytes'],
            'total_time': round(main_stage['source']['from_cdn']['time'] + main_stage['source']['from_reverse_proxy']['time'] + main_stage['source']['from_client_directly']['time'], 3),
            'requests': [],
            'source': main_stage.pop('source')}
        minute_main_doc['requests'].extend(final_uri_dicts(main_stage, log_name, this_h_m))
        bulk_documents.append(minute_main_doc)

    def reset_every_minute():
        nonlocal processed_num, main_stage, invalid
        processed_num = 0
        invalid = 0
        main_stage = {'source': {'from_cdn': {'hits': 0, 'bytes': 0, 'time': 0},
                                 'from_reverse_proxy': {'hits': 0, 'bytes': 0, 'time': 0},
                                 'from_client_directly': {'hits': 0, 'bytes': 0, 'time': 0}}}
    # 开始处理日志文件
    try:
        fp = open(log_name)
        # 当前日志文件总行数
        cur_num = int(run('wc -l {}'.format(log_name), shell=True, stdout=PIPE, universal_newlines=True).stdout.split()[0])
        log_date_ori, log_date = get_log_date(fp, log_name)
        last_num, last_date_time = get_prev_info(log_date)  # 上一次处理到的行数和时间
    except Exception as err:
        logger.error(err)
        return

    n = processed_num = 0
    for line_str in fp:
        n += 1
        if not argv_log_list:
            # 默认方式(通过配置文件)运行时: 根据cur_num和mongodb中记录的last_num对比, 决定本次要处理的行数范围
            if n <= last_num:
                continue
            elif n > cur_num:
                break
        # 解析行
        line_res = process_line(line_str)
        if not line_res:
            invalid += 1
            continue
        date, hour, minute = line_res['time_local'].split(':')[:3]
        if date == log_date_ori:
            y_m_d = log_date
        else:
            # 对应一个日志文件中包含跨天日志内容的情况
            d_m_y = date.split('/')
            log_date_prev = log_date
            log_date_ori = date
            log_date = d_m_y[2][2:] + month_dict[d_m_y[1]] + d_m_y[0]
            last_num, last_date_time = get_prev_info(log_date)
            generate_bulk_docs(log_date_prev)
            # if bulk_documents:
            try:
                insert_mongo(mongo_db, bulk_documents, n - 1, log_date_prev + this_h_m)
                bulk_documents = []
            except Exception:
                return
            reset_every_minute()
            n = 1
        if argv_log_list:
            # 在命令行指定日志文件时: 根据日志中的ymdhm和mongodb中记录的last_date_time对比, 决定本次要处理的行数范围
            if last_date_time and y_m_d + hour + minute <= last_date_time:
                continue

        # 分钟粒度交替时: 从临时字典中汇总上一分钟的结果并将其入库
        if this_h_m != hour + minute and this_h_m != '':
            generate_bulk_docs(y_m_d)
            if len(bulk_documents) == 100:  # 累积100个文档后执行一次批量插入
                try:
                    insert_mongo(mongo_db, bulk_documents, n, y_m_d + this_h_m)
                    bulk_documents = []
                except Exception:
                    return  # 这里用exit无法退出主程序
            # 清空临时字典main_stage, invalid, processed_num
            reset_every_minute()
            logger.info('{} processed to {}'.format(log_name, line_res['time_local']))

        # 不到分钟粒度交替时:
        processed_num += 1
        this_h_m = hour + minute
        append_line_to_main_stage(line_res, main_stage)  # 对每一行的解析结果进行处理

    # 最后可能会存在一部分已解析但未达到分钟交替的行, 需要额外逻辑进行入库
    if processed_num > 0:
        generate_bulk_docs(y_m_d)
    if bulk_documents and this_h_m:
        try:
            insert_mongo(mongo_db, bulk_documents, n, y_m_d + this_h_m)
        except Exception:
            return
    if this_h_m:
        del_old_data(y_m_d, this_h_m)


if __name__ == "__main__":
    server = gethostname()  # 主机名
    log_pattern_obj = re.compile(log_pattern)
    request_uri_pattern_obj = re.compile(request_uri_pattern)
    with open('/tmp/test_singleton', 'wb') as f:
        try:
            fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)  # 实现单例执行
        except BlockingIOError:
            exit(11)
        if argv_log_list:
            logs_list = argv_log_list
        else:
            logs_list = listdir(log_dir)
        if len(logs_list) > 0:
            try:
                with Pool(len(logs_list)) as p:
                    p.map(main, logs_list)
            except KeyboardInterrupt:
                exit(10)
