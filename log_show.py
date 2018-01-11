#!/bin/env python3
# coding:utf-8
"""
Usage:
  log_show <site_name> [options]
  log_show <site_name> [options] -r <request_uri>
  log_show <site_name> [options] -u <uri> [(--distribution|--detail)]
  
Options:
  -h --help                       Show this screen.
  -f --from <start_time>          Start time.Format: %y%m%d[%H[%M]], %H and %M is optional
  -t --to <end_time>              End time.Same as --from
  -l --limit <num>                Number of lines in output, 0 means no limit. [default: 10]
  -s --server <server>            Web server hostname
  -u --uri <uri>                  URI in request(should in quotation marks). Default implies --detail
  -r --request_uri <request_uri>  Show distribution(about hits,bytes,time) of a special full_request_uri(should in quotation marks)
                                  in each period(which --group_by specific). Default implies --distribution
  --detail                        Display details of args analyse of the uri that -u specific
  --distribution                  Show distribution(about hits,bytes,time) of uri  in every period(which --group_by specific)
  -g --group_by <group_by>        Group by every minute, every ten minutes, every hour or every day,
                                  valid values: "minute", "ten_min", "hour", "day". [default: hour]
"""

import pymongo
import time
import re
from sys import exit, argv
from docopt import docopt
from functools import wraps
from copy import deepcopy

arguments = docopt(__doc__)
print(arguments)
# 判断--group_by合理性
if arguments['--group_by'] not in ('minute', 'ten_min', 'hour', 'day'):
    print("  Warning: --group_by must be one of 'minute', 'ten_min', 'hour', 'day'")
    exit(10)

today = time.strftime('%y%m%d', time.localtime())  # 今天日期,取两位年份
mongo_client = pymongo.MongoClient('192.168.1.2')
mongo_db = mongo_client[arguments['<site_name>']]
# mongodb中每天一个集合, 选定要查询的集合
mongo_col = mongo_db[arguments['--from'][:6]] if arguments['--from'] else mongo_db[today]


def timer(func):
    """测量函数执行时间的装饰器"""
    @wraps(func)
    def inner_func(*args, **kwargs):
        t0 = time.time()
        result_ = func(*args, **kwargs)
        t1 = time.time()
        print("Time running %s: %s seconds" % (func.__name__, str(t1 - t0)))
        return result_
    return inner_func


@timer
def get_collection_list():
    """mongodb中集合是按天分割的,取得指定时间段所包含的mongodb集合"""
    collection_list = []
    # 取得参数中--from和--to的日期
    from_date = int(arguments['--from'][:6])
    to_date = int(arguments['--to'][:6])
    if to_date == from_date:
        collection_list.append(str(from_date))
    for i in range(to_date - from_date):
        collection_list.append(str(from_date+i))
    return collection_list


def get_human_size(n):
    """返回更可读的size单位"""
    units = {0: 'B', 1: 'KB', 2: 'MB', 3: 'GB'}
    i = 0
    while n//1024 > 0 and i < 3:
        n = n/1024
        i += 1
    return format(n, '.2f') + ' ' + units[i]


def base_condition(server, start, end, uri_abs, args_abs):
    """额外的 server或者起始时间条件. 返回一个mongodb中aggregate操作的$match条件
    用$and操作符，方便对$match条件进行增减
    what: hits or bytes
    server: 显示来自该server的日志
    start: 开始时间
    end: 结束时间
    uri_abs: 经过抽象的uri
    args_abs: 经过抽象的args"""
    if start and end:
        if int(end[:6]) - int(start[:6]) > 1:  # 判断--from --to时间跨度不能超过单独一天(当前limit)
            print('  Warning: can only do analyse within a single day for now')
            exit(10)
        match = {'$match': {'$and': [{'_id': {'$gte': start}}, {'_id': {'$lt': end}}]}}
    elif start and not end:
        match = {'$match': {'$and': [{'_id': {'$gte': start}}]}}
    else:     # 默认取今天的数据做汇总; 只有end时, 忽略end
        match = {'$match': {'$and': [{'_id': {'$gte': today}}]}}
    if server:
        match['$match']['$and'].append({'_id': {'$regex': server + '$'}})
    if uri_abs:
        match['$match']['$and'].append({'requests.uri_abs': uri_abs})
    if args_abs:
        match['$match']['$and'].append({'requests.args.args_abs': args_abs})
    return match


# @timer
def base_summary(what, limit):
    """输出指定时间内集合中所有uri_abs按hits/bytes/times排序
    what: 'hits' or 'bytes' or 'time'
    limit: 限制显示多少行
    """
    pipeline0 = [{'$group': {'_id': 'null', 'total': {'$sum': '$total_' + what}}}]
    pipeline1 = [{'$project': {'requests.uri_abs': 1, 'requests.' + what: 1}}, {'$unwind': "$requests"},
                 {'$group': {'_id': '$requests.uri_abs', what: {'$sum': '$requests.' + what}}}]
    additional_condition = base_condition(arguments['--server'], arguments['--from'], arguments['--to'], None, None)
    pipeline0.insert(0, additional_condition)
    pipeline1.insert(0, additional_condition)
    # print(pipeline0)  # debug
    # print(pipeline1)  # debug
    try:
        # 符合条件的总hits/bytes/time
        collection_total = mongo_col.aggregate(pipeline0).next()['total']
    except StopIteration:
        print('  Warning: there is no record in the condition you specified')
        exit(11)
    # pymongo.command_cursor.CommandCursor 对象无法保留结果中的顺序，故而mongodb pipeline中就不需要调用$sort排序
    total_uri = mongo_col.aggregate(pipeline1)
    # 这一步来对pymongo.command_cursor.CommandCursor中的每一行进行排序，并存进list对象
    total_uri = sorted(total_uri, key=lambda x: x[what], reverse=True)
    if what == 'hits':
        global total_hits  # 为了给bytes和time维度求每个uri均值使用
        total_hits = deepcopy(total_uri)
    if int(limit):
        total_uri = total_uri[:int(limit)]
    if what == 'hits':
        print('{0}\nTotal {1}: {2}\n{0}'.format('=' * 20, what, collection_total))
        print('{}    {}    {}'.format('hits'.rjust(10), 'percent'.rjust(10), 'uri_abs'))
    elif what == 'bytes':
        print('{0}\nTotal {1}: {2}\n{0}'.format('='*20, what, get_human_size(collection_total)))
        print('{}    {}    {}    {}'.format('bytes'.rjust(10), 'percent'.rjust(10), 'avg_bytes'.rjust(10), 'uri_abs'))
    elif what == 'time':
        print('{0}\nTotal cum. {1}: {2}s\n{0}'.format('=' * 20, what, format(collection_total, '.0f')))
        print('{}    {}    {}    {}'.format('cum. time'.rjust(10), 'percent'.rjust(10), 'avg_time'.rjust(10), 'uri_abs'))
    for one_doc in total_uri:
        uri = one_doc['_id']
        value = one_doc[what]
        if what != 'hits':
            '''bytes和time要计算平均值'''
            for pair in total_hits:
                if pair['_id'] == uri:
                    hits = pair['hits']
        if what == 'hits':
            print('{}   {}%    {}'.format(str(value).rjust(10), format(value / collection_total * 100, '.2f').rjust(10), uri))
        elif what == 'bytes':
            print('{}   {}%    {}    {}'.format(
                get_human_size(value).rjust(10), format(value / collection_total * 100, '.2f').rjust(10),
                get_human_size(value / hits).rjust(10), uri))
        elif what == 'time':
            print('{}s   {}%   {}s    {}'.format(format(value, '.0f').rjust(9),
                                                 format(value / collection_total * 100, '.2f').rjust(10),
                                                 format(value / hits, '.3f').rjust(10), uri))


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


def specific_base_pipeline(what, uri_type, text):
    """为specific_uri_summary函数提供通用的pipeline
    what: 'hits' or 'bytes' or 'time'
    uri_type: uri or request_uri
    text: text content
    """
    # 定义一个mongodb aggregate操作pipeline的模板
    pipeline = [{'$unwind': '$requests'}, {'$group': {'_id': 'null', what: {'$sum': ''}}}]
    # 根据uri或request_uri决定mongodb aggregate操作的$match条件
    if uri_type == 'uri' or uri_type is None:
        uri_abs = text_abstract(text, 'uri') if text is not None else None
        args_abs = None
        additional_condition = base_condition(
            arguments['--server'], arguments['--from'], arguments['--to'], uri_abs, None)
        sum_ = '$requests.' + what
        # pipeline.insert(0, {'$project': {'requests.args': 0}})
        pipeline.insert(1, additional_condition)
    elif uri_type == 'request_uri':
        try:
            uri_abs = text_abstract(text.split('?', 1)[0], 'uri')
            args_abs = text_abstract(text.split('?', 1)[1], 'args')
        except IndexError:
            '''-u 后跟随一个不含参数的uri会触发该异常'''
            print('  Warning: full original request URI must contain a "?"')
            exit(12)
        additional_condition = base_condition(
            arguments['--server'], arguments['--from'], arguments['--to'], uri_abs, args_abs)
        sum_ = '$requests.args.' + what
        pipeline.insert(0, {'$project': {'requests.uri_abs': 1, 'requests.args': 1}})
        pipeline.insert(2, {'$unwind': '$requests.args'})
        pipeline.insert(3, additional_condition)
    # 修改aggregate操作$sum字段
    pipeline[-1]['$group'][what]['$sum'] = sum_
    return {'pipeline': pipeline, 'uri_abs': uri_abs, 'args_abs': args_abs}


# @timer
def specific_uri_summary(uri_type, how, text, group_by, limit):
    """指定的uri/request_uri在给定时间段内的分布
    uri_type: uri or request_uri
    how: distribution or detail
    text: text content
    group_by: 取样粒度, 分钟/十分钟/小时/天"""
    # 根据指定的汇总粒度, 决定aggregate操作中$group条件的_id列
    if group_by == 'ten_min':
        group_id = {'$substrBytes': ['$_id', 0, 9]}
    elif group_by == 'hour':
        group_id = {'$substrBytes': ['$_id', 0, 8]}
    elif group_by == 'day':
        group_id = {'$substrBytes': ['$_id', 0, 6]}
    else:  # group_by == 'min':  默认为分
        group_id = {'$substrBytes': ['$_id', 0, 10]}

    pipeline_hits = specific_base_pipeline('hits', uri_type, text)['pipeline']
    pipeline_bytes = specific_base_pipeline('bytes', uri_type, text)['pipeline']
    pipeline_time = specific_base_pipeline('time', uri_type, text)['pipeline']

    try:
        # 指定uri在指定条件下的总hits/bytes/time
        total_hits = mongo_col.aggregate(pipeline_hits).next()['hits']
        total_bytes = mongo_col.aggregate(pipeline_bytes).next()['bytes']
        total_time = mongo_col.aggregate(pipeline_time).next()['time']
    except StopIteration:
        print('  Warning: there is no record in the condition you specified')
        exit(13)

    uri_args_dict = specific_base_pipeline('hits', uri_type, text)  # 为了获取uri_abs和args_abs
    if uri_type is None:
        print('=' * 20)  # 表头
    if uri_type == 'uri':
        print('{}\nuri_abs: {}'.format('=' * 20, uri_args_dict['uri_abs']))  # 表头
    elif uri_type == 'request_uri':
        print('{}\nrequest_uri_abs: {}'.format('=' * 20, uri_args_dict['uri_abs'] + ' + ' + uri_args_dict['args_abs']))  # 表头
    print('Total hits: {}    Total bytes: {}    Avg_time: {}\n{}'
          ''.format(total_hits, get_human_size(total_bytes), format(total_time/total_hits, '.3f'), '=' * 20))
    if not how or how == 'distribution':
        print('{}  {}  {}  {}  {}  {}'.format((group_by if group_by else 'min').rjust(10),
                                              'hits'.rjust(10), 'hits_percent'.rjust(10),
                                              'bytes'.rjust(10), 'bytes_percent'.rjust(10),
                                              'avg_time'.rjust(10)))
        # 修改aggregate操作 $group字段
        pipeline_hits[-1]['$group']['_id'] = group_id
        pipeline_bytes[-1]['$group']['_id'] = group_id
        pipeline_time[-1]['$group']['_id'] = group_id
        # print(pipeline_hits)  #debug
        # print(pipeline_bytes)  #debug
        # print(pipeline_time)  #debug
        result_hits = sorted(mongo_col.aggregate(pipeline_hits), key=lambda x: x['_id'])  # 按_id列排序,即按时间从小到大输出
        result_bytes = mongo_col.aggregate(pipeline_bytes)
        result_time = mongo_col.aggregate(pipeline_time)
        if int(limit):
            result_hits = result_hits[:int(limit)]

        result_bytes_dict = {}
        result_time_dict = {}
        for one_doc in result_bytes:
            result_bytes_dict[one_doc['_id']] = one_doc['bytes']
        for one_doc in result_time:
            result_time_dict[one_doc['_id']] = one_doc['time']

        for one_doc in result_hits:
            date = one_doc['_id']
            hits = one_doc['hits']
            # print('date: {}    result_time_dict[date]: {}'.format(date, result_time_dict[date]))  # debug
            print('{}  {}  {}%  {}  {}%  {}s'.format(date.rjust(10), str(hits).rjust(10),
                                                     format(hits / total_hits * 100, '.2f').rjust(11),
                                                     get_human_size(result_bytes_dict[date]).rjust(10),
                                                     format(result_bytes_dict[date] / total_bytes * 100, '.2f').rjust(12),
                                                     format(result_time_dict[date] / hits, '.3f').rjust(9)))
    else:
        '''显示指定uri的args点击情况,即--detail'''
        print('{}  {}  {}  {}  {}  args_abs'.format('hits'.rjust(10), 'hits_percent'.rjust(10),
                                                    'bytes'.rjust(10), 'bytes_percent'.rjust(10),
                                                    'avg_time'.rjust(10)))
        pipeline_hits.insert(1, {'$unwind': '$requests.args'})
        pipeline_hits[-1]['$group']['_id'] = '$requests.args.args_abs'
        pipeline_hits[-1]['$group']['hits']['$sum'] = '$requests.args.hits'
        pipeline_bytes.insert(1, {'$unwind': '$requests.args'})
        pipeline_bytes[-1]['$group']['_id'] = '$requests.args.args_abs'
        pipeline_bytes[-1]['$group']['bytes']['$sum'] = '$requests.args.bytes'
        pipeline_time.insert(1, {'$unwind': '$requests.args'})
        pipeline_time[-1]['$group']['_id'] = '$requests.args.args_abs'
        pipeline_time[-1]['$group']['time']['$sum'] = '$requests.args.time'

        # print('pipeline_hits: {}'.format(pipeline_hits))  # debug
        # print('pipeline_bytes: {}'.format(pipeline_bytes))  # debug
        # print('pipeline_time: {}'.format(pipeline_time))  # debug

        result_hits = sorted(mongo_col.aggregate(pipeline_hits), key=lambda x: x['hits'], reverse=True)  # 按args的点击数排序
        result_bytes = mongo_col.aggregate(pipeline_bytes)
        result_time = mongo_col.aggregate(pipeline_time)

        result_bytes_dict = {}
        result_time_dict = {}
        for one_doc in result_bytes:
            result_bytes_dict[one_doc['_id']] = one_doc['bytes']
        for one_doc in result_time:
            result_time_dict[one_doc['_id']] = one_doc['time']

        if int(limit):
            result_hits = result_hits[:int(limit)]
        for one_doc in result_hits:
            args = one_doc['_id']
            hits = one_doc['hits']
            print('{}  {}%  {}  {}%  {}s  {}'.format(str(hits).rjust(10),
                                                     format(hits / total_hits * 100, '.2f').rjust(11),
                                                     get_human_size(result_bytes_dict[args]).rjust(10),
                                                     format(result_bytes_dict[args] / total_bytes * 100, '.2f').rjust(12),
                                                     format(result_time_dict[args] / hits, '.3f').rjust(9),
                                                     args if args != '' else '""'))


# 根据参数执行动作
if arguments['--uri'] and not arguments['--detail']:
    specific_uri_summary('uri', 'distribution', arguments['--uri'], arguments['--group_by'], arguments['--limit'])
elif arguments['--uri'] and arguments['--detail']:
    specific_uri_summary('uri', 'detail', arguments['--uri'], arguments['--group_by'], arguments['--limit'])
elif arguments['--request_uri']:
    specific_uri_summary('request_uri', None, arguments['--request_uri'], arguments['--group_by'], arguments['--limit'])
elif '-g' in argv or '--group_by' in argv:
    specific_uri_summary(None, 'distribution', None, arguments['--group_by'], arguments['--limit'])
else:
    base_summary('hits', arguments['--limit'])
    base_summary('bytes', arguments['--limit'])
    base_summary('time', arguments['--limit'])
