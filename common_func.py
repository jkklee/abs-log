# -*- coding:utf-8 -*-
from functools import wraps
import time
import re


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


def special_insert(arr, v):
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


def get_human_size(n):
    """返回更可读的size单位"""
    units = {0: 'B', 1: 'KB', 2: 'MB', 3: 'GB'}
    i = 0
    while n//1024 > 0 and i < 3:
        n = n/1024
        i += 1
    return format(n, '.2f') + ' ' + units[i]


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


