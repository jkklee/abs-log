# -*- coding:utf-8 -*-
from sys import exit
from common.common import *


def ip_base_summary(what, ip_type, limit, mongo_col, arguments):
    """根据ip的出现频率进行分析,以请求的三种来源为三个维度进行分析:(来自cdn; 来自反向代理; 直接来自客户端)
    what: 'hits' or 'bytes' or 'time'
    ip_type: user_ip_via_cdn or last_cdn_ip or user_ip_via_proxy or remote_addr
    limit: 限制显示多少行
    mongo_col: 本次操作对应的集合名称
    arguments: 用户从log_show界面输入的参数
    """
    additional_condition = base_condition(arguments['--server'], arguments['--from'], arguments['--to'])
    try:
        total = total_info(arguments, mongo_col)
        total = mongo_col.aggregate([additional_condition, {'$group': {'_id': 'null',
                                                                       'total_hits': {'$sum': '$total_hits'},
                                                                       'total_bytes': {'$sum': '$total_bytes'},
                                                                       'total_time': {'$sum': '$total_time'}}}]).next()
        print(total)
    except StopIteration:
        print('  Warning: there is no record in the condition you specified')
        exit(11)
    pipeline1 = [{'$project': {'requests.uri_abs': 1, 'requests.ips': 1}}, {'$unwind': '$requests'}, {'$unwind': '$requests.ips'},
                 {'$match': {'requests.ips.type': ip_type}}, {'$group': {'_id': '$requests.uri_abs', '_'.join(ip_type, what): {'$sum': '$requests.ips.' + what}}}]