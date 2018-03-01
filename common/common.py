# -*- coding:utf-8 -*-
from functools import wraps
from analyse_config import mongo_host, mongo_port
import time
import re
import pymongo

mongo_client = pymongo.MongoClient(mongo_host, mongo_port, connect=False)
today = time.strftime('%y%m%d', time.localtime())  # 今天日期,取两位年份


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


# -----log_analyse使用----- #
# 文档中_id字段中需要的随机字符串
random_char = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'
# 快速转换月份格式
month_dict = {'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04', 'May': '05', 'Jun': '06',
              'Jul': '07', 'Aug': '08', 'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12'}


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


def get_median(sorted_data):
    """获取列表的中位数"""
    half = len(sorted_data) // 2
    return (sorted_data[half] + sorted_data[~half]) / 2


def get_quartile(data):
    """获取列表的4分位数(参考盒须图思想,用于体现响应时间和响应大小的分布.)
    以及min和max值(放到这里主要考虑对排序后数据的尽可能利用)"""
    data = sorted(data)
    size = len(data)
    if size == 1:
        return data[0], data[0], data[0], data[0], data[0]
    half = size // 2
    q1 = get_median(data[:half])
    q2 = get_median(data)
    q3 = get_median(data[half + 1:]) if size % 2 == 1 else get_median(data[half:])
    return data[0], q1, q2, q3, data[-1]


def special_insert_list(arr, v):
    """list插入过程加入对最大值(index: -1)的维护"""
    if len(arr) == 1:
        if v >= arr[0]:
            arr.append(v)
        else:
            arr.insert(0, v)
    else:
        if v >= arr[-1]:
            arr.append(v)
        else:
            arr.insert(-1, v)


def special_update_dict(dict_obj, key, standby_value=None, sub_type=None, sub_keys=None, sub_values=None):
    """若key对应的值的类型为非字典类型(数字,字符,列表,元组), 对应第1,2,3个参数
    若key对应值类型为字典,对应第1,2,4,5,6参数"""
    if sub_type is None:
        if key in dict_obj:
            dict_obj[key] += standby_value
        else:
            dict_obj[key] = standby_value
    elif isinstance(sub_type, dict):
        if key in dict_obj:
            for k, v in zip(sub_keys, sub_values):
                dict_obj[key][k] += v
        else:
            dict_obj[key] = sub_type
            for k, v in zip(sub_keys, sub_values):
                dict_obj[key][k] = v


# -----log_show使用----- #
def get_human_size(n):
    """返回更可读的size单位"""
    units = {0: 'B', 1: 'KB', 2: 'MB', 3: 'GB'}
    i = 0
    while n//1024 > 0 and i < 3:
        n = n/1024
        i += 1
    return format(n, '.2f') + ' ' + units[i]


def base_condition(server, start, end, uri_abs=None, args_abs=None, ip=None):
    """额外的 server或 起始时间 条件. 返回一个mongodb中aggregate操作的$match条件
    用$and操作符，方便对$match条件进行增减
    server: 显示来自该server的日志
    start: 开始时间
    end: 结束时间
    uri_abs: 经过抽象的uri
    args_abs: 经过抽象的args"""
    # print('uri_abs:', uri_abs, 'args_abs:', args_abs)  # debug
    if start and end:
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
    if ip:
        match['$match']['$and'].append({'requests.ips.ip': ip})
    return match


def total_info(arguments, mongo_col, uri_abs=None, args_abs=None, ip=None):
    """输出指定时间内集合中所有uri_abs按hits/bytes/times排序
    arguments: 用户从log_show界面输入的参数
    mongo_col: 本次操作对应的集合名称
    """
    pipeline = [{'$group': {'_id': 'null', 'total_hits': {'$sum': '$total_hits'}, 'total_bytes': {'$sum': '$total_bytes'},
                            'total_time': {'$sum': '$total_time'}, 'invalid_hits': {'$sum': '$invalid_hits'}}}]

    if uri_abs and args_abs:
        pipeline.insert(0, {'$unwind': '$requests'})
        pipeline.insert(1, {'$unwind': '$requests.args'})
        pipeline.insert(2, base_condition(arguments['--server'], arguments['--from'], arguments['--to'], uri_abs=uri_abs, args_abs=args_abs))
        pipeline[-1]['$group']['total_hits']['$sum'] = '$requests.args.hits'
        pipeline[-1]['$group']['total_bytes']['$sum'] = '$requests.args.bytes'
        pipeline[-1]['$group']['total_time']['$sum'] = '$requests.args.time'

    elif uri_abs:
        pipeline.insert(0, {'$unwind': '$requests'})
        pipeline.insert(1, base_condition(arguments['--server'], arguments['--from'], arguments['--to'], uri_abs=uri_abs))
        pipeline[-1]['$group']['total_hits']['$sum'] = '$requests.hits'
        pipeline[-1]['$group']['total_bytes']['$sum'] = '$requests.bytes'
        pipeline[-1]['$group']['total_time']['$sum'] = '$requests.time'
    elif ip:
        pipeline.insert(0, {'$unwind': '$requests'})
        pipeline.insert(1, {'$unwind': '$requests.ips'})
        pipeline.insert(2, base_condition(arguments['--server'], arguments['--from'], arguments['--to'], ip=ip))
        pipeline[-1]['$group']['total_hits']['$sum'] = '$requests.ips.hits'
        pipeline[-1]['$group']['total_bytes']['$sum'] = '$requests.ips.bytes'
        pipeline[-1]['$group']['total_time']['$sum'] = '$requests.ips.time'
    else:
        pipeline.insert(0, base_condition(arguments['--server'], arguments['--from'], arguments['--to']))
    try:
        # 符合条件的总hits/bytes/time/invalid_hits
        return mongo_col.aggregate(pipeline).next()
    except StopIteration:
        print('Warning: no record under the condition you specified')
        exit(11)


def group_by_func(arg):
    """根据指定的汇总粒度, 决定 aggregate 操作中 $group 条件的 _id 列"""
    if arg == 'minute':
        group_id = {'$substrBytes': ['$_id', 0, 10]}
    elif arg == 'ten_min':
        group_id = {'$substrBytes': ['$_id', 0, 9]}
    elif arg == 'day':
        group_id = {'$substrBytes': ['$_id', 0, 6]}
    else:  # 默认 arg == 'hour'
        group_id = {'$substrBytes': ['$_id', 0, 8]}
    return group_id


