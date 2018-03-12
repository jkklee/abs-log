# -*- coding:utf-8 -*-
from common.common import *


def base_summary(ip_type, limit, mongo_col, match, total_dict):
    """
    根据ip的出现频率进行分析,以请求的三种来源为三个维度进行分析:(来自cdn; 来自反向代理; 直接来自客户端)
    ip_type: ip类型, 'user_ip_via_cdn', 'last_cdn_ip', 'user_ip_via_proxy', 'remote_addr'
    limit: 对每个独立ip的点击数(hits)进行倒序排列，取limit行
    mongo_col: 本次操作对应的集合名称
    match: common.match_condition()返回的过滤条件字典
    total_dict: 指定条件内total_hits, total_bytes, total_time, invalid_hits (dict)
    """
    pipeline = [match['basic_match'], {'$project': {'requests.ips': 1}}, {'$unwind': '$requests'}, {'$unwind': '$requests.ips'},
                {'$match': {'$and': [{'requests.ips.type': ip_type}]}},
                {'$group': {'_id': '$requests.ips.ip', 'hits': {'$sum': '$requests.ips.hits'},
                            'bytes': {'$sum': '$requests.ips.bytes'}, 'time': {'$sum': '$requests.ips.time'}}}, {'$sort': {'hits': -1}}]  # 待优化，慢在$group阶段
    pipeline_source_func = lambda source: [{'$project': {'source': 1}}, {'$group': {'_id': 'null',
                                           'hits': {'$sum': '$source.{}.hits'.format(source)},
                                           'bytes': {'$sum': '$source.{}.bytes'.format(source)},
                                           'time': {'$sum': '$source.{}.time'.format(source)}}}]
    # 限制条数时，$sort + $limit 可以减少mongodb内部的操作量，若不限制显示条数，此步的mongodb内部排序将无必要
    if limit:
        pipeline.append({'$limit': limit})
    # print('base_summary pipeline:\n', pipeline)  # debug
    mongo_result = list(mongo_col.aggregate(pipeline))
    # pymongo.command_cursor.CommandCursor 对象无法保留结果中的顺序，故而需要python再做一次排序，并存进list对象
    # mongo_result = sorted(mongo_result, key=lambda x: x['hits'], reverse=True)  # pymongo (3.4.0)可以保留顺序
    # print('---mongo_result---:\n', mongo_result)  # debug
    if not mongo_result:
        return

    # 打印表头
    if ip_type == 'last_cdn_ip':
        # user_ip_via_cdn和last_cdn_ip均属于From_cdn,调用该函数时要保证先调用last_cdn_ip才能保证From_cdn表头正确输出
        print('{}\n{}  {}  {}  {}  {}  {}'.format('=' * 20, 'From_cdn/Proxy'.ljust(20), 'hits'.rjust(10), 'hits(%)'.rjust(7), 'bytes'.rjust(10), 'bytes(%)'.rjust(7), 'time(%)'.rjust(7)))
        this_total = mongo_col.aggregate(pipeline_source_func('from_cdn')).next()
    elif ip_type == 'user_ip_via_proxy':
        print('{}\n{}  {}  {}  {}  {}  {}'.format('=' * 20, 'From_reverse_proxy'.ljust(20), 'hits'.rjust(10), 'hits(%)'.rjust(7), 'bytes'.rjust(10), 'bytes(%)'.rjust(7), 'time(%)'.rjust(7)))
        this_total = mongo_col.aggregate(pipeline_source_func('from_reverse_proxy')).next()
    elif ip_type == 'remote_addr':
        print('{}\n{}  {}  {}  {}  {}  {}'.format('=' * 20, 'From_client_directly'.ljust(20), 'hits'.rjust(10), 'hits(%)'.rjust(7), 'bytes'.rjust(10), 'bytes(%)'.rjust(7), 'time(%)'.rjust(7)))
        this_total = mongo_col.aggregate(pipeline_source_func('from_client_directly')).next()
    if ip_type != 'user_ip_via_cdn':
        print('{}  {}  {}  {}  {}  {}'.format('=' * 20, str(this_total['hits']).rjust(10),
                                          format(this_total['hits'] / total_dict['total_hits'] * 100, '.2f').rjust(7),
                                          get_human_size(this_total['bytes']).rjust(10),
                                          format(this_total['bytes'] / total_dict['total_bytes'] * 100, '.2f').rjust(7),
                                          format(this_total['time'] / total_dict['total_time'] * 100, '.2f').rjust(7)))
    print(ip_type.capitalize().rjust(20))
    # 打印结果
    for one_doc in mongo_result:
        print('{}  {}  {}  {}  {}  {}'.format(
            one_doc['_id'].rjust(20), str(one_doc['hits']).rjust(10),
            format(one_doc['hits'] / total_dict['total_hits'] * 100, '.2f').rjust(7),
            get_human_size(one_doc['bytes']).rjust(10),
            format(one_doc['bytes'] / total_dict['total_bytes'] * 100, '.2f').rjust(7),
            format(one_doc['time'] / total_dict['total_time'] * 100, '.2f').rjust(7)))


def distribution(mongo_col, arguments):
    """
    展示ip统计按照指定period做group聚合的结果
    mongo_col: 本次操作对应的集合名称
    arguments: docopt解析用户从log_show界面输入的参数而来的dict
    """
    groupby = arguments['--group_by'] if arguments['--group_by'] else 'hour'
    group_id = group_by_func(groupby)
    ip = arguments['<ip>']
    limit = int(arguments['--limit'])

    match = match_condition(arguments['--server'], arguments['--from'], arguments['--to'], ip=ip)
    pipeline = [match['basic_match'], {'$project': {'requests.ips': 1}}, {'$unwind': '$requests'}, {'$unwind': '$requests.ips'},
                {'$match': {'$and': [{'requests.ips.ip': ip}]}},
                {'$group': {'_id': group_id, 'hits': {'$sum': '$requests.ips.hits'},
                            'bytes': {'$sum': '$requests.ips.bytes'}, 'time': {'$sum': '$requests.ips.time'}}}, {'$sort': {'_id': 1}}]
    if limit:
        pipeline.append({'$limit': limit})
    # print('distribution pipeline:\n', pipeline)  # debug
    mongo_result = mongo_col.aggregate(pipeline)

    total_project = {'$project': {'requests.ips': 1}}
    total_dict = total_info(mongo_col, match, project=total_project, ip=ip)

    # 打印表头
    print('{0}\nIP: {1}'.format('=' * 20, ip))
    print('Total_hits: {}    Total_bytes: {}\n{}'.format(total_dict['total_hits'], get_human_size(total_dict['total_bytes']), '=' * 20))
    print('{}  {}  {}  {}  {}'.format(groupby.rjust(10), 'hits'.rjust(10), 'hits(%)'.rjust(7), 'bytes'.rjust(10), 'bytes(%)'.rjust(8)))
    # 打印结果
    for one_doc in mongo_result:
        hits = one_doc['hits']
        bytes_ = one_doc['bytes']
        date = one_doc['_id']
        print('{}  {}  {}%  {}  {}%'.format(date.rjust(10), str(hits).rjust(10),
              format(hits / total_dict['total_hits'] * 100, '.2f').rjust(6), get_human_size(bytes_).rjust(10),
              format(bytes_ / total_dict['total_bytes'] * 100, '.2f').rjust(7)))


def detail(mongo_col, arguments):
    """
    展示指定ip产生的各uri_abs 的hits/bytes/time情况
    mongo_col: 本次操作对应的集合名称
    arguments: docopt解析用户从log_show界面输入的参数而来的dict
    """
    ip = arguments['<ip>']
    limit = int(arguments['--limit'])

    match = match_condition(arguments['--server'], arguments['--from'], arguments['--to'], ip=ip)
    pipeline = [match['basic_match'], {'$project': {'requests.uri_abs': 1, 'requests.ips': 1}},
                {'$unwind': '$requests'}, {'$unwind': '$requests.ips'},
                {'$match': {'$and': [{'requests.ips.ip': ip}]}},
                {'$group': {'_id': '$requests.uri_abs', 'hits': {'$sum': '$requests.ips.hits'},
                            'bytes': {'$sum': '$requests.ips.bytes'}, 'time': {'$sum': '$requests.ips.time'}}}, {'$sort': {'hits': -1}}]
    if limit:
        pipeline.append({'$limit': limit})
    # print('detail pipeline:\n', pipeline)  # debug
    mongo_result = mongo_col.aggregate(pipeline)

    total_project = {'$project': {'requests.uri_abs': 1, 'requests.ips': 1}}
    total_dict = total_info(mongo_col, match, project=total_project, ip=ip)

    # 打印表头
    print('{}\nIP: {}'.format('=' * 20, ip))
    print('Total_hits: {}    Total_bytes: {}\n{}'.format(total_dict['total_hits'], get_human_size(total_dict['total_bytes']), '=' * 20))
    print('{}  {}  {}  {}  {}  uri_abs'.format(
          'hits'.rjust(8), 'hits(%)'.rjust(7), 'bytes'.rjust(9), 'bytes(%)'.rjust(8), 'time(%)'.rjust(7)))
    # 打印结果
    for one_doc in mongo_result:
        print('{}  {}%  {}  {}%  {}%  {}'.format(
            str(one_doc['hits']).rjust(8), format(one_doc['hits'] / total_dict['total_hits'] * 100, '.2f').rjust(6),
            get_human_size(one_doc['bytes']).rjust(9),
            format(one_doc['bytes'] / total_dict['total_bytes'] * 100, '.2f').rjust(7),
            format(one_doc['time'] / total_dict['total_time'] * 100, '.2f').rjust(6), one_doc['_id']))
