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


def base_summary(what, limit, arguments, mongo_col):
    """输出指定时间内集合中所有uri_abs按hits/bytes/times排序
    what: 'hits' or 'bytes' or 'time'
    limit: 限制显示多少行(int)
    arguments: 用户从log_show界面输入的参数
    mongo_col: 本次操作对应的集合名称
    total_dict: 指定条件内total_hits, total_bytes, total_time, invalid_hits (dict)
    """
    additional_condition = base_condition(arguments['--server'], arguments['--from'], arguments['--to'])
    total_dict = total_info(arguments, mongo_col, additional_condition)

    pipeline = [{'$project': {'requests.uri_abs': 1, 'requests.' + what: 1}}, {'$unwind': '$requests'},
                {'$group': {'_id': '$requests.uri_abs', what: {'$sum': '$requests.' + what}}}]
    pipeline[0]['$project'].update(requests_q4_enable)
    pipeline[-1]['$group'].update(request_q4_group_by)
    pipeline.insert(0, additional_condition)
    # 限制条数时，$sort + $limit 可以减少mongodb内部的操作量，若不限制显示条数，此步的mongodb内部排序将无必要
    if limit:
        pipeline.extend([{'$sort': {what: -1}}, {'$limit': limit}])
    total_uri = mongo_col.aggregate(pipeline)
    # pymongo.command_cursor.CommandCursor 对象无法保留结果中的顺序，故而需要python再做一次排序，并存进list对象
    total_uri = sorted(total_uri, key=lambda x: x[what], reverse=True)

    if what == 'hits':
        print('{0}\nTotal_{1}:{2} invalid_hits:{3}\n{0}'.format('=' * 20, what, total_dict['total_hits'], total_dict['invalid_hits']))
        print('{}  {}  {}  {}  {}'.format('hits'.rjust(10), 'percent'.rjust(7), 'time_distribution(s)'.center(37), 'bytes_distribution(B)'.center(44), 'uri_abs'))
    elif what == 'bytes':
        print('{0}\nTotal_{1}:{2}\n{0}'.format('='*20, what, get_human_size(total_dict['total_bytes'])))
        print('{}  {}  {}  {}  {}'.format('bytes'.rjust(10), 'percent'.rjust(7), 'time_distribution(s)'.center(37), 'bytes_distribution(B)'.center(44), 'uri_abs'))
    elif what == 'time':
        print('{0}\nTotal_{1}:{2}s\n{0}'.format('=' * 20, what, format(total_dict['total_time'], '.0f')))
        print('{}  {}  {}  {}  {}'.format('cum. time'.rjust(10), 'percent'.rjust(7), 'time_distribution(s)'.center(37), 'bytes_distribution(B)'.center(44), 'uri_abs'))
    for one_doc in total_uri:
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


def distribution_pipeline(groupby, arguments, mongo_col, uri_abs=None, args_abs=None):
    """为 distribution 函数提供pipeline
    groupby: 聚合周期(minute, ten_min, hour, day)
    arguments: docopt解析用户从log_show界面输入的参数而来的dict
    mongo_col: 本次操作对应的集合名称
    """
    additional_condition = base_condition(arguments['--server'], arguments['--from'], arguments['--to'], uri_abs=uri_abs, args_abs=args_abs)
    total_dict = total_info(arguments, mongo_col, additional_condition, uri_abs=uri_abs, args_abs=args_abs)
    group_id = group_by_func(groupby)
    # 定义一个mongodb aggregate操作pipeline的模板
    pipeline = [{'$unwind': '$requests'}, {'$group': {'_id': group_id, 'hits': {'$sum': '$requests.hits'}, 'bytes': {'$sum': '$requests.bytes'}}}]

    if uri_abs and args_abs:
        pipeline.insert(0, {'$project': {'requests.uri_abs': 1, 'requests.args': 1}})
        pipeline.insert(2, {'$unwind': '$requests.args'})
        pipeline.insert(3, additional_condition)
        # 修改aggregate操作$sum字段
        pipeline[-1]['$group']['hits']['$sum'] = '$requests.args.hits'
        pipeline[-1]['$group']['bytes']['$sum'] = '$requests.args.bytes'
        pipeline[-1]['$group'].update(request_args_q4_group_by)
        # print('have args:', pipeline)  # debug
    else:
        '''有或无uri_abs均走这套逻辑'''
        pipeline.insert(0, {'$project': {'requests.uri_abs': 1, 'requests.hits': 1,  'requests.bytes': 1}})
        pipeline[0]['$project'].update(requests_q4_enable)
        pipeline.insert(2, additional_condition)
        pipeline[-1]['$group'].update(request_q4_group_by)
        # print('not have args', pipeline)  # debug
    return pipeline, total_dict


def distribution(text, groupby, limit, mongo_col, arguments):
    """展示request_uri(with args or don't have args)按照指定period做group聚合的结果
    text: request_uri(with args or don't have args)
    groupby: 聚合周期(minute, ten_min, hour, day)
    mongo_col: 本次操作对应的集合名称
    limit: 限制显示多少行(int)"""
    if text:
        try:
            uri_abs = text_abstract(text.split('?', 1)[0], 'uri')
            args_abs = text_abstract(text.split('?', 1)[1], 'args')
        except IndexError:
            uri_abs = text_abstract(text, 'uri')
            args_abs = None
    else:
        uri_abs = None
        args_abs = None
    pipeline, total_dict = distribution_pipeline(groupby, arguments, mongo_col, uri_abs=uri_abs, args_abs=args_abs)

    if uri_abs and args_abs:
        print('{0}\nuri_abs: {1}  args_abs: {2}'.format('=' * 20, uri_abs, args_abs))  # 表头
    elif uri_abs:
        print('{0}\nuri_abs: {1}'.format('=' * 20, uri_abs))  # 表头
    else:
        print('=' * 20)  # 表头
    print('Total_hits: {}    Total_bytes: {}\n{}'.format(total_dict['total_hits'], get_human_size(total_dict['total_bytes']), '=' * 20))
    print('{}  {}  {}  {}  {}  {}  {}'.format((groupby if groupby else 'hour').rjust(10),
                                              'hits'.rjust(10), 'hits(%)'.rjust(7), 'bytes'.rjust(10), 'bytes(%)'.rjust(8),
                                              'time_distribution(s)'.center(37), 'bytes_distribution(B)'.center(44)))
    if limit:
        pipeline.extend([{'$sort': {'_id': 1}}, {'$limit': limit}])
    # print("stage_ret['pipeline']:", stage_ret['pipeline'])  # debug
    dist_res = mongo_col.aggregate(pipeline)
    dist_res = sorted(dist_res, key=lambda x: x['_id'])  # 按_id列排序,即按时间从小到大输出

    for one_doc in dist_res:
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


def detail_pipeline(arguments, mongo_col, uri_abs):
    """为 detail 函数提供pipeline
    arguments: docopt解析用户从log_show界面输入的参数而来的dict
    mongo_col: 本次操作对应的集合名称
    uri_abs: 经抽象之后的uri
    """
    additional_condition = base_condition(arguments['--server'], arguments['--from'], arguments['--to'], uri_abs=uri_abs)
    total_dict = total_info(arguments, mongo_col, additional_condition, uri_abs=uri_abs)
    # 定义一个mongodb aggregate操作pipeline的模板
    pipeline = [{'$unwind': '$requests'}, {'$unwind': '$requests.args'}]
    pipeline.insert(2, additional_condition)
    return pipeline, total_dict


def detail(text, limit, mongo_col, arguments):
    """展示uri的各args(若有的话)的 hits/bytes/time情况
    text: text content
    limit: 限制显示多少行(int)
    mongo_col: 本次操作对应的集合名称
    arguments: docopt解析用户从log_show界面输入的参数而来的dict"""
    try:
        uri_abs = text_abstract(text.split('?', 1)[0], 'uri')
    except IndexError:
        uri_abs = text_abstract(text, 'uri')

    pipeline_hits, total_dict = detail_pipeline(arguments, mongo_col, uri_abs)
    pipeline_bytes = detail_pipeline(arguments, mongo_col, uri_abs)[0]
    pipeline_time = detail_pipeline(arguments, mongo_col, uri_abs)[0]
    pipeline_hits.append({'$group': {'_id': '$requests.args.args_abs', 'hits': {'$sum': '$requests.args.hits'}}})
    pipeline_bytes.append({'$group': {'_id': '$requests.args.args_abs', 'bytes': {'$sum': '$requests.args.bytes'}}})
    pipeline_bytes[-1]['$group'].update(request_args_q4_group_by)
    pipeline_time.append({'$group': {'_id': '$requests.args.args_abs', 'time': {'$sum': '$requests.args.time'}}})
    pipeline_time[-1]['$group'].update(request_args_q4_group_by)
    # print('pipeline_hits: {}\n pipeline_bytes: {}\n pipeline_time: {}\n'.format(pipeline_hits,pipeline_bytes,pipeline_time))  # debug

    print('{}\nuri_abs: {}'.format('=' * 20, uri_abs))  # 表头
    print('Total_hits: {}    Total_bytes: {}\n{}'.format(total_dict['total_hits'], get_human_size(total_dict['total_bytes']), '=' * 20))
    print('{}  {}  {}  {}  {}  {}  {}  args_abs'.format(
          'hits'.rjust(8), 'hits(%)'.rjust(7), 'bytes'.rjust(9), 'bytes(%)'.rjust(8), 'time(%)'.rjust(7),
          'time_distribution(s)'.center(37), 'bytes_distribution(B)'.center(40)))

    result_hits = sorted(mongo_col.aggregate(pipeline_hits), key=lambda x: x['hits'], reverse=True)  # 按args的点击数排序
    result_bytes = mongo_col.aggregate(pipeline_bytes)
    result_time = mongo_col.aggregate(pipeline_time)

    result_bytes_dict = {}
    result_time_dict = {}
    for one_doc in result_bytes:
        result_bytes_dict.setdefault(one_doc['_id'], {})
        result_bytes_dict[one_doc['_id']]['bytes'] = one_doc['bytes']
        result_bytes_dict[one_doc['_id']]['q1_bytes'] = one_doc['q1_bytes']
        result_bytes_dict[one_doc['_id']]['q2_bytes'] = one_doc['q2_bytes']
        result_bytes_dict[one_doc['_id']]['q3_bytes'] = one_doc['q3_bytes']
        result_bytes_dict[one_doc['_id']]['max_bytes'] = one_doc['max_bytes']
    for one_doc in result_time:
        result_time_dict.setdefault(one_doc['_id'], {})
        result_time_dict[one_doc['_id']]['time'] = one_doc['time']
        result_time_dict[one_doc['_id']]['q1_time'] = one_doc['q1_time']
        result_time_dict[one_doc['_id']]['q2_time'] = one_doc['q2_time']
        result_time_dict[one_doc['_id']]['q3_time'] = one_doc['q3_time']
        result_time_dict[one_doc['_id']]['max_time'] = one_doc['max_time']

    if limit:
        result_hits = result_hits[:limit]
    for one_doc in result_hits:
        args = one_doc['_id']
        hits = one_doc['hits']
        print('{}  {}%  {}  {}%  {}%  {}  {}  {}'.format(
            str(hits).rjust(8), format(hits / total_dict['total_hits'] * 100, '.2f').rjust(6),
            get_human_size(result_bytes_dict[args]['bytes']).rjust(9),
            format(result_bytes_dict[args]['bytes'] / total_dict['total_bytes'] * 100, '.2f').rjust(7),
            format(result_time_dict[args]['time'] / total_dict['total_time'] * 100, '.2f').rjust(6),
            format('%25<{} %50<{} %75<{} %100<{}'.format(
                round(result_time_dict[args]['q1_time'], 2), round(result_time_dict[args]['q2_time'], 2),
                round(result_time_dict[args]['q3_time'], 2), round(result_time_dict[args]['max_time'], 2))).ljust(37),
            format('%25<{} %50<{} %75<{} %100<{}'.format(
                int(result_bytes_dict[args]['q1_bytes']), int(result_bytes_dict[args]['q2_bytes']),
                int(result_bytes_dict[args]['q3_bytes']), int(result_bytes_dict[args]['max_bytes']))).ljust(40),
            args if args != '' else '""'))
