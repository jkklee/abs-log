#!/bin/env python3
# -*- coding:utf-8 -*-
# Collect nginx access log, process it and insert the result into mongodb.
# This script should put into crontab in every web server. Execute every N minutes.
import logging
import fcntl
import json
from socket import gethostname
from multiprocessing import Pool
from random import choice
from os import stat, path, getcwd
from sys import exit, argv as sys_argv
from subprocess import run, PIPE

from config import *
from common.common import (random_char, text_abstract, special_update_dict, get_quartile, convert_time,
                           log_pattern_obj, request_uri_pattern_obj, mongo_client, get_delta_date, todo_log)


logging.basicConfig(format='%(asctime)s %(levelname)7s: %(message)s', datefmt='%Y%m%d %H:%M:%S')
logger = logging.getLogger(__name__)
logger.setLevel(ERROR_LEVEL)


class MyMongo(object):
    def __init__(self, db_name):
        """获得Database(MongoClient）对象
        db_name: mongodb的库名(不同站点对应不同的库名)"""
        self.db_name = db_name
        self.mongodb = mongo_client[db_name]

    def insert_mongo(self, bulk_doc, offset, inode, timestamp):
        """插入mongodb
        bulk_doc: 由每分钟文档组成的批量插入的数组
        offset: 当前已入库的offset
        inode: 当前处理的文件的inode
        """
        # timestamp = time.strftime('%Y%m%d%H%M%S', time.localtime(time.time()))
        ##print('db_name:', self.db_name, 'cur_offset:', offset, 'cur_inode:', inode)
        try:
            self.mongodb['main'].insert_many(bulk_doc)  # 插入数据
            self.mongodb['registry'].update({'server': server},
                                            {'$set': {'offset': offset, 'inode': inode, 'timestamp': timestamp}}, upsert=True)
        except Exception as err:
            logger.error('{} insert data error: {}'.format(self.db_name, repr(err)))
            raise
        finally:
            mongo_client.close()

    def get_prev_info(self):
        """取得本server本日志这一天已入库的offset"""
        ##print('db_name:', self.db_name, 'server:', server)
        tmp = self.mongodb['registry'].find({'server': server}, {'server': 1, 'inode': 1, 'offset': 1})
        try:
            res = tmp.next()
            return res['offset'], res['inode']
        except StopIteration:
            return 0, 0
        except Exception as err:
            logger.error("get offset of {} at {} error, will exit: {}".format(self.db_name, server, repr(err)))
            raise
        finally:
            mongo_client.close()

    def del_old_data(self, date, h_m):
        """删除N天前的数据, 默认为LIMIT
        date: 日期, 格式20180315
        h_m: 当前hour and minute(到23:59时执行清理操作)"""
        if h_m != '2359':
            return
        min_date = get_delta_date(date, LIMIT)
        try:
            self.mongodb['main'].remove({'_id': {'$lt': min_date}})
            self.mongodb['registry'].remove({'timestamp': {'$lt': min_date}})
        except Exception as err:
            logger.error("{} delete documents before {} days ago error: {}".format(self.db_name, LIMIT, repr(err)))


class LogBase(object):
    def __init__(self, log_name):
        """根据文件名或者inode获取文件信息"""
        self.log_name = log_name
        fstat = stat(log_name)
        self.cur_size = fstat.st_size
        self.cur_timestamp = fstat.st_mtime
        self.cur_inode = fstat.st_ino


class LogPlainText(LogBase):
    def parse_line(self, line_str):
        """
        处理每一行记录
        line_str: 该行日志的原始形式
        """
        parsed = log_pattern_obj.match(line_str)
        if not parsed:
            # 如果正则无法匹配该行时
            logger.warning("Can't parse line: {}".format(line_str))
            return
        parsed_dict = parsed.groupdict()
        # remote_addr字段在有反向代理的情况下多数时候是无意义的(仅代表反向代理的ip),
        # 除非：以nginx为例,配置了set_real_ip_from和real_ip_header指令(该指令也是下面分析请求流向是要考虑的)
        remote_addr = parsed_dict['remote_addr']
        if 'time_local' in parsed_dict:
            time_local = convert_time(parsed_dict['time_local'], 'time_local')
        else:
            time_local = convert_time(parsed_dict['time_local'], 'time_iso8601')

        # 状态码, 字节数, 响应时间
        response_code = parsed_dict['status']
        bytes_sent = parsed_dict['body_bytes_sent']
        request_time = parsed_dict['request_time']

        if 'http_x_forwarded_for' in parsed_dict:
            http_x_forwarded_for = parsed_dict['http_x_forwarded_for']
            # 通过remote_addr和user_ip以及last_cdn_ip可分析出请求来源的四种情况(在cdn和反代都正常传递http_x_forwarded_for的前提下)
            # 1.user-->cdn-->reverse_proxy-->web_server (remote_addr != user_ip)
            # 2.user-->cdn-->web_server (remote_addr == last_cdn_ip and last_cdn_ip != user_ip)
            # 2这种概率很小, 并且如果nginx有set_real_ip_from指令的话会和1无法区分,故而忽略此种情况
            # 3.user-->reverse_proxy-->web_server (last_cdn_ip == user_ip and last_cdn_ip != '-')
            # 4.user-->web_server(last_cdn_ip == user_ip == '-')
            ips = http_x_forwarded_for.split()
            user_ip = ips[0].rstrip(',')
            last_cdn_ip = ips[-1]
        else:
            # 视为 user-->web_server
            user_ip = last_cdn_ip = None

        # 处理uri和args
        if 'request' in parsed_dict:
            request = parsed_dict['request']
            request_further = request_uri_pattern_obj.search(request)
            if not request_further:
                logger.warning("Can't parse $request: {}".format(line_str))
                uri_abs = args_abs = request_method = None
            else:
                request_method = request_further.group('request_method')
                # 对uri和args进行抽象
                uri_abs, args_abs = text_abstract(request_uri_=request_further.group('request_uri'), log_name_=self.log_name)
        elif 'request_uri' in parsed_dict:
            request_method = parsed_dict['request_method']
            uri_abs, args_abs = text_abstract(request_uri_=parsed_dict['request_uri'], log_name_=self.log_name)
        else:
            request_method = parsed_dict['request_method']
            uri_abs, args_abs = text_abstract(uri_=parsed_dict['uri'], args_=parsed_dict['args'], log_name_=self.log_name)

        return {'uri_abs': uri_abs, 'args_abs': args_abs, 'time_local': time_local, 'response_code': int(response_code),
                'bytes_sent': int(bytes_sent), 'request_time': float(request_time), 'remote_addr': remote_addr,
                'user_ip': user_ip, 'last_cdn_ip': last_cdn_ip, 'request_method': request_method}


class LogJson(LogBase):
    def __init__(self, log_name):
        LogBase.__init__(self, log_name)
        # 反转字典, 解决json格式中key name可能和nginx内置的变量名不同的问题
        self.reverse_dict = dict([(v.lstrip('$'), k) for k, v in json.loads(LOG_FORMAT).items()])

    def parse_line(self, line_str):
        parsed_dict = json.loads(line_str)

        remote_addr = parsed_dict[self.reverse_dict['remote_addr']]
        # 处理时间
        if 'time_local' in self.reverse_dict:
            time_local = convert_time(parsed_dict[self.reverse_dict['time_local']], 'time_local')
        else:
            time_local = convert_time(parsed_dict[self.reverse_dict['time_iso8601']], 'time_iso8601')

        # 状态码, 字节数, 响应时间
        response_code = parsed_dict[self.reverse_dict['status']]
        bytes_sent = parsed_dict[self.reverse_dict['body_bytes_sent']]
        request_time = parsed_dict[self.reverse_dict['request_time']]

        if self.reverse_dict['http_x_forwarded_for'] in parsed_dict:
            http_x_forwarded_for = parsed_dict[self.reverse_dict['http_x_forwarded_for']]
            ips = http_x_forwarded_for.split()
            user_ip = ips[0].rstrip(',')
            last_cdn_ip = ips[-1]
        else:
            # 视为 user-->web_server
            user_ip = last_cdn_ip = None

        # 处理uri和args
        if 'request' in self.reverse_dict:
            request = parsed_dict[self.reverse_dict['request']]
            request_further = request_uri_pattern_obj.search(request)
            if not request_further:
                logger.warning("Can't parse $request: {}".format(line_str))
                uri_abs = args_abs = request_method = None
            else:
                request_method = request_further.group('request_method')
                # 对uri和args进行抽象
                uri_abs, args_abs = text_abstract(request_uri_=request_further.group('request_uri'), log_name_=self.log_name)
        elif 'request_uri' in self.reverse_dict:
            request_method = parsed_dict[self.reverse_dict['request_method']]
            uri_abs, args_abs = text_abstract(request_uri_=parsed_dict[self.reverse_dict['request_uri']], log_name_=self.log_name)
        else:
            request_method = parsed_dict[self.reverse_dict['request_method']]
            if 'uri' in self.reverse_dict:
                uri = parsed_dict[self.reverse_dict['uri']]
            else:
                uri = parsed_dict[self.reverse_dict['document_uri']]
            if 'args' in self.reverse_dict:
                args = parsed_dict[self.reverse_dict['args']]
            else:
                args = parsed_dict[self.reverse_dict['query_string']]
            uri_abs, args_abs = text_abstract(uri_=uri, args_=args, log_name_=self.log_name)

        return {'uri_abs': uri_abs, 'args_abs': args_abs, 'time_local': time_local, 'response_code': int(response_code),
                'bytes_sent': int(bytes_sent), 'request_time': float(request_time),  'remote_addr': remote_addr,
                'user_ip': user_ip, 'last_cdn_ip': last_cdn_ip, 'request_method': request_method}


class Processor(object):
    def __init__(self, log_name):
        """log_name: 日志文件名"""
        self.log_name = log_name
        self.base_name = path.basename(log_name)
        self.db_name = self.base_name.replace('.', '_')  # mongodb中的库名(将域名中的.替换为_)
        self.mymongo = MyMongo(self.db_name)
        self.processed_num = 0  # 总请求数
        self.invalid_hits = 0  # 无效请求数
        self.error_hits = 0  # 返回码>400的请求数
        # self.main_stage: 处理过程中, 用于保存一分钟内的各项原始数据
        self.main_stage = {'source': {'from_cdn': {'hits': 0, 'bytes': 0, 'time': 0},
                                      'from_reverse_proxy': {'hits': 0, 'bytes': 0, 'time': 0},
                                      'from_client_directly': {'hits': 0, 'bytes': 0, 'time': 0}}}
        self.bulk_documents = []  # 作为每分钟文档的容器, 累积BATCH_INSERT个文档时, 进行一次批量插入
        self.this_h_m = ''  # 当前处理的一分钟, 格式: 0101(1时1分)
        self.single_uri_dict = None
        self.uri_k = None
        self.uri_v = None

    def _final_uri_dicts(self):
        """对self.main_stage里的原始数据进行整合生成每个uri_abs对应的字典, 插入到minute_main_doc['request']中, 生成最终存储到mongodb的文档"""
        uris = []
        if len(self.main_stage) > URI_STORE_MAX_NUM:
            logger.debug("{} truncate uri_abs sorted by 'hits' from {} to {} at {}".format(
                self.base_name, len(self.main_stage), URI_STORE_MAX_NUM, self.this_h_m))
        for self.uri_k, self.uri_v in sorted(
                self.main_stage.items(), key=lambda item: item[1]['hits'], reverse=True)[:URI_STORE_MAX_NUM]:
            # 取点击量前URI_STORE_MAX_NUM的uri_abs
            # if self.uri_v['hits'] < URI_STORE_MIN_HITS:
            #     break
            # 先由self.main_stage[uri_abs][args_abs]字典中计算出self.main_stage[uri_abs]字典
            for args_abs_value in self.main_stage[self.uri_k]['args'].values():
                self.main_stage[self.uri_k]['time'].extend(args_abs_value['time'])
                self.main_stage[self.uri_k]['bytes'].extend(args_abs_value['bytes'])
            uri_quartile_time = get_quartile(self.uri_v['time'])
            uri_quartile_bytes = get_quartile(self.uri_v['bytes'])
            # minute_main_doc['request']列表中一个uri_abs对的应字典格式如下
            self.single_uri_dict = {'uri_abs': self.uri_k,
                                    'hits': self.uri_v['hits'],
                                    'q2_time': round(uri_quartile_time[2], 3),
                                    'q3_time': round(uri_quartile_time[3], 3),
                                    'max_time': uri_quartile_time[-1],
                                    'time': round(sum(self.uri_v['time']), 3),
                                    'q2_bytes': int(uri_quartile_bytes[2]),
                                    'q3_bytes': int(uri_quartile_bytes[3]),
                                    'max_bytes': uri_quartile_bytes[-1],
                                    'bytes': sum(self.uri_v['bytes']),
                                    'args': [],
                                    'ips': [],
                                    'errors': []}
            for arg_k, arg_v in self.uri_v['args'].items():
                # 生成self.single_uri_dict['args']列表中的一个args_abs对应的字典single_arg_dict
                arg_quartile_time = get_quartile(arg_v['time'])
                arg_quartile_bytes = get_quartile(arg_v['bytes'])
                single_arg_dict = {'args_abs': arg_k,
                                   'hits': arg_v['hits'],
                                   'q2_time': round(arg_quartile_time[2], 3),
                                   'q3_time': round(arg_quartile_time[3], 3),
                                   'max_time': arg_quartile_time[-1],
                                   'time': round(sum(arg_v['time']), 3),
                                   'q2_bytes': int(arg_quartile_bytes[2]),
                                   'q3_bytes': int(arg_quartile_bytes[3]),
                                   'max_bytes': arg_quartile_bytes[-1],
                                   'bytes': sum(arg_v['bytes']),
                                   'method': arg_v['method']}
                self.single_uri_dict['args'].append(single_arg_dict)
            for error_k, error_v in self.uri_v['errors'].items():
                # 生成self.single_uri_dict['errors']列表中的一个error对应的字典single_error_dict
                error_quartile_time = get_quartile(error_v['time'])
                error_quartile_bytes = get_quartile(error_v['bytes'])
                single_error_dict = {'error_code': error_k,
                                     'hits': error_v['hits'],
                                     'q2_time': round(error_quartile_time[2], 3),
                                     'q3_time': round(error_quartile_time[3], 3),
                                     'max_time': error_quartile_time[-1],
                                     'time': round(sum(error_v['time']), 3),
                                     'q2_bytes': int(error_quartile_bytes[2]),
                                     'q3_bytes': int(error_quartile_bytes[3]),
                                     'max_bytes': error_quartile_bytes[-1],
                                     'bytes': sum(error_v['bytes']),
                                     'method': error_v['method']}
                self.single_uri_dict['errors'].append(single_error_dict)

            self._add_ip_statistics('user_ip_via_cdn')
            self._add_ip_statistics('last_cdn_ip')
            self._add_ip_statistics('user_ip_via_proxy')
            self._add_ip_statistics('remote_addr')

            uris.append(self.single_uri_dict)
        return uris

    def _add_ip_statistics(self, ip_type):
        """将各种类型ip的统计信息加入到self.single_uri_dict字典
        ip_type: user_ip_via_cdn, last_cdn_ip, user_ip_via_proxy, remote_addr"""
        if len(self.uri_v[ip_type]) > IP_STORE_MAX_NUM:
            logger.debug("{} {} truncate {} sorted by 'hits' from {} to {} at {}".format(
                self.base_name, self.uri_k, ip_type, len(self.main_stage[self.uri_k][ip_type]), IP_STORE_MAX_NUM, self.this_h_m))
        for ip_k, ip_v in sorted(self.uri_v[ip_type].items(), key=lambda item: item[1]['hits'], reverse=True)[:IP_STORE_MAX_NUM]:
            # 取ip类型为ip_type的统计中点击量前IP_STORE_MAX_NUM的user_ip_via_cdn
            # if ip_v['hits'] < IP_STORE_MIN_HITS:
            #     break
            single_ip_dict = {'ip': ip_k, 'hits': ip_v['hits'], 'time': round(ip_v['time'], 3),
                              'bytes': ip_v['bytes'], 'type': ip_type}
            self.single_uri_dict['ips'].append(single_ip_dict)

    def _append_line_to_main_stage(self, line_res):
        """将每行的分析结果(line_res)追加进(self.main_stage)字典"""
        request_time = line_res['request_time']
        bytes_sent = line_res['bytes_sent']
        uri_abs = line_res['uri_abs']
        args_abs = line_res['args_abs']
        user_ip = line_res['user_ip']
        last_cdn_ip = line_res['last_cdn_ip']
        remote_addr = line_res['remote_addr']
        response_code = line_res['response_code']
        # 将uri_abs数据汇总至临时字典(考虑性能,放到final_uri_dicts()中由args_abs数据计算而来)
        if uri_abs in self.main_stage:
            self.main_stage[uri_abs]['hits'] += 1
        else:
            self.main_stage[uri_abs] = {'time': [], 'bytes': [], 'hits': 1, 'args': {}, 'errors': {}, 'user_ip_via_cdn': {},
                                        'last_cdn_ip': {}, 'user_ip_via_proxy': {}, 'remote_addr': {}}
        # 将args_abs数据汇总到临时字典
        if args_abs in self.main_stage[uri_abs]['args']:
            self.main_stage[uri_abs]['args'][args_abs]['time'].append(request_time)
            self.main_stage[uri_abs]['args'][args_abs]['bytes'].append(bytes_sent)
            self.main_stage[uri_abs]['args'][args_abs]['hits'] += 1
        else:
            self.main_stage[uri_abs]['args'][args_abs] = {'time': [request_time], 'bytes': [bytes_sent],
                                                          'hits': 1, 'method': line_res['request_method']}
        # 将error信息汇总到临时字典
        if response_code >= 400:
            if response_code in self.main_stage[uri_abs]['errors']:
                self.main_stage[uri_abs]['errors'][response_code]['time'].append(request_time)
                self.main_stage[uri_abs]['errors'][response_code]['bytes'].append(bytes_sent)
                self.main_stage[uri_abs]['errors'][response_code]['hits'] += 1
            else:
                self.main_stage[uri_abs]['errors'][response_code] = {'time': [request_time], 'bytes': [bytes_sent],
                                                                     'hits': 1, 'method': line_res['request_method']}
        # 将ip信息汇总到临时字典
        if user_ip != '-' and user_ip != last_cdn_ip:
            # come from cdn
            self.main_stage['source']['from_cdn']['hits'] += 1
            self.main_stage['source']['from_cdn']['bytes'] += bytes_sent
            self.main_stage['source']['from_cdn']['time'] += request_time
            special_update_dict(self.main_stage[uri_abs]['user_ip_via_cdn'], user_ip, sub_type={},
                                sub_keys=['hits', 'time', 'bytes'], sub_values=[1, request_time, bytes_sent])
            special_update_dict(self.main_stage[uri_abs]['last_cdn_ip'], last_cdn_ip, sub_type={},
                                sub_keys=['hits', 'time', 'bytes'], sub_values=[1, request_time, bytes_sent])
        elif user_ip and user_ip != '-' and user_ip == last_cdn_ip:
            # come from reverse_proxy
            self.main_stage['source']['from_reverse_proxy']['hits'] += 1
            self.main_stage['source']['from_reverse_proxy']['bytes'] += bytes_sent
            self.main_stage['source']['from_reverse_proxy']['time'] += request_time
            special_update_dict(self.main_stage[uri_abs]['user_ip_via_proxy'], user_ip, sub_type={},
                                sub_keys=['hits', 'time', 'bytes'], sub_values=[1, request_time, bytes_sent])
        elif user_ip == last_cdn_ip == '-' or not user_ip:
            # user->web_server
            self.main_stage['source']['from_client_directly']['hits'] += 1
            self.main_stage['source']['from_client_directly']['bytes'] += bytes_sent
            self.main_stage['source']['from_client_directly']['time'] += request_time
            special_update_dict(self.main_stage[uri_abs]['remote_addr'], remote_addr, sub_type={},
                                sub_keys=['hits', 'time', 'bytes'], sub_values=[1, request_time, bytes_sent])

    def _generate_bulk_docs(self, date):
        """生成每分钟的文档, 存放到self.bulk_documents中"""
        minute_main_doc = {
            '_id': date + self.this_h_m + '-' + choice(random_char) + choice(random_char) + choice(random_char) + '-' + server,
            'total_hits': self.processed_num,
            'invalid_hits': self.invalid_hits,
            'error_hits': self.error_hits,
            'total_bytes': self.main_stage['source']['from_cdn']['bytes'] + self.main_stage['source']['from_reverse_proxy']['bytes'] + self.main_stage['source']['from_client_directly']['bytes'],
            'total_time': round(self.main_stage['source']['from_cdn']['time'] + self.main_stage['source']['from_reverse_proxy']['time'] + self.main_stage['source']['from_client_directly']['time'], 3),
            'requests': [],
            'source': self.main_stage.pop('source')}
        minute_main_doc['requests'].extend(self._final_uri_dicts())
        self.bulk_documents.append(minute_main_doc)

    def _reset_every_minute(self):
        self.processed_num = self.invalid_hits = self.error_hits = 0
        self.main_stage = {'source': {'from_cdn': {'hits': 0, 'bytes': 0, 'time': 0},
                                      'from_reverse_proxy': {'hits': 0, 'bytes': 0, 'time': 0},
                                      'from_client_directly': {'hits': 0, 'bytes': 0, 'time': 0}}}

    def go_process(self):
        """开始处理日志文件"""
        if LOG_TYPE == 'plaintext':
            logobj = LogPlainText(self.log_name)
        elif LOG_TYPE == 'json':
            logobj = LogJson(self.log_name)
        else:
            logger.error("wrong LOG_TYPE in config.py, must one of 'plaintext' or 'json'")
            return

        try:
            # 对于一个日志文件名, 上一次处理到的offset和inode
            last_offset, last_inode = self.mymongo.get_prev_info()
            ##print('log_name:',self.log_name, 'last_offset:', last_offset, 'last_inode:', last_inode)
        except Exception:
            return

        if last_inode and last_inode != logobj.cur_inode:
            # 发生了日志切割, 要检查并处理被切割操作移走的上一个文件(可能会遗留部分未处理的内容)
            last_log_name = run('find / -inum {}'.format(last_inode), shell=True, stdout=PIPE, universal_newlines=True).stdout.rstrip('\n')
            another_processor = Processor(last_log_name)
            another_processor.mymongo = MyMongo(self.db_name)  # 这里将切割后文件对应的库重定向到当前库(一个日志唯一的库)
            another_processor.go_process()
            another_processor.mymongo.mongodb['registry'].remove({'inode': last_inode})
            last_offset = 0
            ##print('last_inode', 'end')

        # 打开文件,找到相应的offset进行处理
        fobj = open(self.log_name)
        fobj.seek(last_offset)
        parsed_offset = last_offset
        for line_str in fobj:
            if last_offset >= logobj.cur_size:
                break
            parsed_offset += len(line_str)

            line_res = logobj.parse_line(line_str)
            if not line_res or not line_res['uri_abs']:
                self.invalid_hits += 1
                continue
            if line_res['response_code'] >= 400:
                self.error_hits += 1
            date, hour, minute = line_res['time_local']

            # 分钟粒度交替时: 从临时字典中汇总上一分钟的结果并将其入库
            if self.this_h_m != hour + minute and self.this_h_m:
                self._generate_bulk_docs(date)
                if len(self.bulk_documents) == BATCH_INSERT:  # 累积BATCH_INSERT个文档后执行一次批量插入
                    try:
                        self.mymongo.insert_mongo(self.bulk_documents, parsed_offset, logobj.cur_inode, date + self.this_h_m)
                        self.bulk_documents = []
                    except Exception:
                        return  # 这里用exit无法退出主程序
                # 清空临时字典self.main_stage, invalid, processed_num
                self._reset_every_minute()
                logger.info('{} processed to {}'.format(self.base_name, ''.join(line_res['time_local'])))

            # 不到分钟粒度交替时:
            self.processed_num += 1
            self.this_h_m = hour + minute
            self._append_line_to_main_stage(line_res)  # 对每一行的解析结果进行处理

        # 最后可能会存在一部分已解析但未达到分钟交替的行, 需要额外逻辑进行入库
        if self.processed_num > 0:
            self._generate_bulk_docs(date)
        if self.bulk_documents and self.this_h_m:
            try:
                self.mymongo.insert_mongo(self.bulk_documents, parsed_offset, logobj.cur_inode, date + self.this_h_m)
            except Exception:
                return
        if self.this_h_m:
            self.mymongo.del_old_data(date, self.this_h_m)


def main(log_name):
    processor = Processor(log_name)
    processor.go_process()


if __name__ == "__main__":
    if len(sys_argv) == 1:
        argv_log_list = None
    elif sys_argv[1] == '-f':
        # 对应指定日志文件的情况(非默认用法)
        argv_log_list = sys_argv[2:]
    else:
        print("Usage:\n  log_analyse.py [-f <log_path>...]")
        exit(12)

    server = gethostname()  # 主机名

    with open('/tmp/test_singleton', 'wb') as f:
        try:
            fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)  # 实现单例执行
        except BlockingIOError:
            exit(11)
        if argv_log_list:
            logs_list = [path.join(getcwd(), x) if not x.startswith('/') else x for x in argv_log_list]
        else:
            logs_list = todo_log()
        if len(logs_list) > 0:
            try:
                with Pool(len(logs_list)) as p:
                    p.map(main, logs_list)
            except KeyboardInterrupt:
                exit(10)
