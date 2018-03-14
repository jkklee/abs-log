# -*- coding:utf-8 -*-
from common.common import *

requests_q4_enable = {'requests.min_time': 1, 'requests.q1_time': 1, 'requests.q2_time': 1, 'requests.q3_time': 1, 'requests.max_time': 1,
                      'requests.min_bytes': 1, 'requests.q1_bytes': 1, 'requests.q2_bytes': 1, 'requests.q3_bytes': 1, 'requests.max_bytes': 1}
request_q4_group_by = {'q1_time': {'$avg': '$requests.q1_time'}, 'q2_time': {'$avg': '$requests.q2_time'},
                       'q3_time': {'$avg': '$requests.q3_time'}, 'max_time': {'$avg': '$requests.max_time'},
                       'q1_bytes': {'$avg': '$requests.q1_bytes'}, 'q2_bytes': {'$avg': '$requests.q2_bytes'},
                       'q3_bytes': {'$avg': '$requests.q3_bytes'}, 'max_bytes': {'$avg': '$requests.max_bytes'}}
request_args_q4_group_by = {'q1_time': {'$avg': '$requests.args.q1_time'}, 'q2_time': {'$avg': '$requests.args.q2_time'},
                            'q3_time': {'$avg': '$requests.args.q3_time'}, 'max_time': {'$avg': '$requests.args.max_time'},
                            'q1_bytes': {'$avg': '$requests.args.q1_bytes'}, 'q2_bytes': {'$avg': '$requests.args.q2_bytes'},
                            'q3_bytes': {'$avg': '$requests.args.q3_bytes'}, 'max_bytes': {'$avg': '$requests.args.max_bytes'}}


def base_summary(what, limit, mongo_col, match, total_dict):
    """输出指定时间内集合中所有uri_abs按hits/bytes/times排序
    what: 'hits' or 'bytes' or 'time'
    limit: 限制显示多少行(int)
    mongo_col: 本次操作对应的集合名称
    match: 起止时间和指定server(最基本的过滤条件)
    total_dict: 指定条件内total_hits, total_bytes, total_time, invalid_hits (dict)
    """
    pipeline = [match['basic_match'], {'$project': {'requests.uri_abs': 1, 'requests.' + what: 1}}, {'$unwind': '$requests'},
                {'$group': {'_id': '$requests.uri_abs', what: {'$sum': '$requests.' + what}}}]
    pipeline[1]['$project'].update(requests_q4_enable)
    pipeline[-1]['$group'].update(request_q4_group_by)
    pipeline.append({'$sort': {what: -1}})
    # 限制条数时，$sort + $limit 可以减少mongodb内部的操作量，若不限制显示条数，此步的mongodb内部排序将无必要
    if limit:
        pipeline.append({'$limit': limit})
    # print('base_summary pipeline:\n', pipeline)  # debug

    mongo_result = mongo_col.aggregate(pipeline)
    # pymongo.command_cursor.CommandCursor 对象无法保留结果中的顺序，故而需要python再做一次排序，并存进list对象
    # mongo_result = sorted(mongo_result, key=lambda x: x[what], reverse=True)

    # 打印表头
    if what == 'hits':
        print('{0}\nTotal_{1}:{2} invalid_hits:{3}\n{0}'.format('=' * 20, what, total_dict['total_hits'], total_dict['invalid_hits']))
        print('{}  {}  {}  {}  {}'.format('hits'.rjust(10), 'percent'.rjust(7), 'time_distribution(s)'.center(37), 'bytes_distribution(B)'.center(44), 'uri_abs'))
    elif what == 'bytes':
        print('{0}\nTotal_{1}:{2}\n{0}'.format('='*20, what, get_human_size(total_dict['total_bytes'])))
        print('{}  {}  {}  {}  {}'.format('bytes'.rjust(10), 'percent'.rjust(7), 'time_distribution(s)'.center(37), 'bytes_distribution(B)'.center(44), 'uri_abs'))
    elif what == 'time':
        print('{0}\nTotal_{1}:{2}s\n{0}'.format('=' * 20, what, format(total_dict['total_time'], '.0f')))
        print('{}  {}  {}  {}  {}'.format('cum. time'.rjust(10), 'percent'.rjust(7), 'time_distribution(s)'.center(37), 'bytes_distribution(B)'.center(44), 'uri_abs'))
    # 打印结果
    for one_doc in mongo_result:
        uri = one_doc['_id']
        value = one_doc[what]
        if what == 'hits':
            print('{}  {}%  {}  {}  {}'.format(
                str(value).rjust(10), format(value / total_dict['total_hits'] * 100, '.2f').rjust(6),
                format('%25<{} %50<{} %75<{} %100<{}'.format(
                    round(one_doc['q1_time'], 2), round(one_doc['q2_time'], 2), round(one_doc['q3_time'], 2), round(one_doc['max_time'], 2))).ljust(37),
                format('%25<{} %50<{} %75<{} %100<{}'.format(
                    int(one_doc['q1_bytes']), int(one_doc['q2_bytes']), int(one_doc['q3_bytes']), int(one_doc['max_bytes']))).ljust(44), uri))
        elif what == 'bytes':
            print('{}  {}%  {}  {}  {}'.format(
                get_human_size(value).rjust(10), format(value / total_dict['total_bytes'] * 100, '.2f').rjust(6),
                format('%25<{} %50<{} %75<{} %100<{}'.format(
                    round(one_doc['q1_time'], 2), round(one_doc['q2_time'], 2), round(one_doc['q3_time'], 2), round(one_doc['max_time'], 2))).ljust(37),
                format('%25<{} %50<{} %75<{} %100<{}'.format(
                    int(one_doc['q1_bytes']), int(one_doc['q2_bytes']), int(one_doc['q3_bytes']), int(one_doc['max_bytes']))).ljust(44), uri))
        elif what == 'time':
            print('{}  {}%  {}  {}  {}'.format(
                format(value, '.0f').rjust(10), format(value / total_dict['total_time'] * 100, '.2f').rjust(6),
                format('%25<{} %50<{} %75<{} %100<{}'.format(
                    round(one_doc['q1_time'], 2), round(one_doc['q2_time'], 2), round(one_doc['q3_time'], 2), round(one_doc['max_time'], 2))).ljust(37),
                format('%25<{} %50<{} %75<{} %100<{}'.format(
                    int(one_doc['q1_bytes']), int(one_doc['q2_bytes']), int(one_doc['q3_bytes']), int(one_doc['max_bytes']))).ljust(44), uri))


def distribution_pipeline(groupby, match, uri_abs=None, args_abs=None):
    """为 distribution 函数提供pipeline
    match: pipeline中的match条件(match_condition由函数返回)
    """
    group_id = group_by_func(groupby)
    # 定义一个mongodb aggregate操作pipeline的模板
    pipeline = [match['basic_match'], {'$unwind': '$requests'}, {'$group': {'_id': group_id, 'hits': {'$sum': '$requests.hits'}, 'bytes': {'$sum': '$requests.bytes'}}}]

    if uri_abs and args_abs:
        pipeline.insert(1, {'$project': {'requests.uri_abs': 1, 'requests.args': 1}})
        pipeline.insert(3, {'$unwind': '$requests.args'})
        pipeline.insert(4, match['special_match'])
        # 修改aggregate操作$sum字段
        pipeline[-1]['$group']['hits']['$sum'] = '$requests.args.hits'
        pipeline[-1]['$group']['bytes']['$sum'] = '$requests.args.bytes'
        pipeline[-1]['$group'].update(request_args_q4_group_by)
        # print('have args:', pipeline)  # debug
    else:
        '''有或无uri_abs均走这套逻辑'''
        pipeline.insert(1, {'$project': {'requests.uri_abs': 1, 'requests.hits': 1,  'requests.bytes': 1}})
        pipeline[1]['$project'].update(requests_q4_enable)
        pipeline.insert(3, match['special_match'])
        pipeline[-1]['$group'].update(request_q4_group_by)
        # print('not have args', pipeline)  # debug
    pipeline.append({'$sort': {'_id': 1}})
    return pipeline


def distribution(mongo_col, arguments):
    """展示request按照指定period做group聚合的结果
    mongo_col: 本次操作对应的集合名称
    arguments: docopt解析用户从log_show界面输入的参数而来的dict
    """
    text = arguments['<request>']  # request(with/without args)
    groupby = arguments['--group_by']  # 聚合周期(minute, ten_min, hour, day)
    limit = int(arguments['--limit'])  # 限制显示多少行
    if text:
        uri_abs, args_abs = text_abstract(text, arguments['<site_name>'])
    else:
        uri_abs = None
        args_abs = None

    match = match_condition(arguments['--server'], arguments['--from'], arguments['--to'], uri_abs=uri_abs, args_abs=args_abs)
    pipeline = distribution_pipeline(groupby, match, uri_abs, args_abs)

    # 打印表头
    if uri_abs and args_abs:
        total_project = {'$project': {'invalid_hits': 1, 'requests.uri_abs': 1, 'requests.args': 1}}
        print('{0}\nuri_abs: {1}  args_abs: {2}'.format('=' * 20, uri_abs, args_abs))
    elif uri_abs:
        total_project = {'$project': {'invalid_hits': 1, 'requests.uri_abs': 1, 'requests.hits': 1, 'requests.bytes': 1, 'requests.time': 1}}
        print('{0}\nuri_abs: {1}'.format('=' * 20, uri_abs))
    else:
        total_project = {'$match': {}}  # 仅占位用
        print('=' * 20)

    total_dict = total_info(mongo_col, match, project=total_project, uri_abs=uri_abs, args_abs=args_abs)
    print('Total_hits: {}    Total_bytes: {}\n{}'.format(total_dict['total_hits'], get_human_size(total_dict['total_bytes']), '=' * 20))
    print('{}  {}  {}  {}  {}  {}  {}'.format((groupby if groupby else 'hour').rjust(10),
                                              'hits'.rjust(10), 'hits(%)'.rjust(7), 'bytes'.rjust(10), 'bytes(%)'.rjust(8),
                                              'time_distribution(s)'.center(37), 'bytes_distribution(B)'.center(44)))
    if limit:
        pipeline.append({'$limit': limit})
    # print("distribution pipeline:\n", pipeline)  # debug
    mongo_result = mongo_col.aggregate(pipeline)
    # mongo_result = sorted(mongo_result, key=lambda x: x['_id'])  # 按_id列排序,即按时间从小到大输出
    # 打印结果
    for one_doc in mongo_result:
        hits = one_doc['hits']
        bytes_ = one_doc['bytes']
        date = one_doc['_id']
        print('{}  {}  {}%  {}  {}%  {}  {}'.format(date.rjust(10), str(hits).rjust(10),
              format(hits / total_dict['total_hits'] * 100, '.2f').rjust(6), get_human_size(bytes_).rjust(10),
              format(bytes_ / total_dict['total_bytes'] * 100, '.2f').rjust(7),
              format('%25<{} %50<{} %75<{} %100<{}'.format(
                     round(one_doc['q1_time'], 2), round(one_doc['q2_time'], 2), round(one_doc['q3_time'], 2), round(one_doc['max_time'], 2))).ljust(37),
              format('%25<{} %50<{} %75<{} %100<{}'.format(
                     int(one_doc['q1_bytes']), int(one_doc['q2_bytes']), int(one_doc['q3_bytes']), int(one_doc['max_bytes']))).ljust(44)))


def detail_pipeline(match):
    """为 detail 函数提供pipeline
    match: pipeline中的match条件(match_condition由函数返回)
    """
    # 定义一个mongodb aggregate操作pipeline的模板
    pipeline = [match['basic_match'], {'$project': {'requests.args': 1, 'requests.uri_abs': 1}}, {'$unwind': '$requests'}, {'$unwind': '$requests.args'}]
    pipeline.append(match['special_match'])
    pipeline.append({'$group': {'_id': '$requests.args.args_abs', 'hits': {'$sum': '$requests.args.hits'}, 'bytes': {'$sum': '$requests.args.bytes'}, 'time': {'$sum': '$requests.args.time'}}})
    pipeline[-1]['$group'].update(request_args_q4_group_by)
    pipeline.append({'$sort': {'hits': -1}})
    return pipeline


def detail(mongo_col, arguments):
    """展示uri的各args(若有的话)的 hits/bytes/time情况
    mongo_col: 本次操作对应的集合名称
    arguments: docopt解析用户从log_show界面输入的参数而来的dict"""
    text = arguments['<uri>']  # 给定的uri
    limit = int(arguments['--limit'])  # 限制显示多少行
    if text:
        uri_abs = text_abstract(text, arguments['<site_name>'])[0]
    else:
        uri_abs = None

    total_project = {'$project': {'invalid_hits': 1, 'requests.uri_abs': 1, 'requests.hits': 1, 'requests.bytes': 1, 'requests.time': 1}}
    match = match_condition(arguments['--server'], arguments['--from'], arguments['--to'], uri_abs=uri_abs)
    total_dict = total_info(mongo_col, match, project=total_project, uri_abs=uri_abs)
    pipeline = detail_pipeline(match)
    if limit:
        pipeline.append({'$limit': limit})
    # print('pipeline:', pipeline)  # debug
    mongo_result = mongo_col.aggregate(pipeline)
    # mongo_result = sorted(mongo_result, key=lambda x: x['hits'], reverse=True)  # 按args的点击数排序

    # 打印表头
    print('{}\nuri_abs: {}'.format('=' * 20, uri_abs))
    print('Total_hits: {}    Total_bytes: {}\n{}'.format(total_dict['total_hits'], get_human_size(total_dict['total_bytes']), '=' * 20))
    print('{}  {}  {}  {}  {}  {}  {}  args_abs'.format(
          'hits'.rjust(8), 'hits(%)'.rjust(7), 'bytes'.rjust(9), 'bytes(%)'.rjust(8), 'time(%)'.rjust(7),
          'time_distribution(s)'.center(37), 'bytes_distribution(B)'.center(40)))
    # 打印结果
    for one_doc in mongo_result:
        args = one_doc['_id']
        print('{}  {}%  {}  {}%  {}%  {}  {}  {}'.format(
            str(one_doc['hits']).rjust(8), format(one_doc['hits'] / total_dict['total_hits'] * 100, '.2f').rjust(6),
            get_human_size(one_doc['bytes']).rjust(9),
            format(one_doc['bytes'] / total_dict['total_bytes'] * 100, '.2f').rjust(7),
            format(one_doc['time'] / total_dict['total_time'] * 100, '.2f').rjust(6),
            format('%25<{} %50<{} %75<{} %100<{}'.format(
                round(one_doc['q1_time'], 2), round(one_doc['q2_time'], 2), round(one_doc['q3_time'], 2), round(one_doc['max_time'], 2))).ljust(37),
            format('%25<{} %50<{} %75<{} %100<{}'.format(
                int(one_doc['q1_bytes']), int(one_doc['q2_bytes']), int(one_doc['q3_bytes']), int(one_doc['max_bytes']))).ljust(40),
            args if args != '' else '""'))
