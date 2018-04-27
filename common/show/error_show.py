# -*- coding:utf-8 -*-
from common.common import *

errors_q4_group_by = {'q2_time': {'$avg': '$requests.errors.q2_time'}, 'q3_time': {'$avg': '$requests.errors.q3_time'}, 'max_time': {'$avg': '$requests.errors.max_time'},
                       'q2_bytes': {'$avg': '$requests.errors.q2_bytes'}, 'q3_bytes': {'$avg': '$requests.errors.q3_bytes'}, 'max_bytes': {'$avg': '$requests.errors.max_bytes'}}


def base_summary(limit, mongo_col, match, total_dict):
    """
    输出指定时间内集合中所有错误码(response_code>=400)的汇总信息, 以hits倒序排列展示
    limit: 限制显示多少行(int)
    mongo_col: 本次操作对应的集合名称
    match: common.match_condition()返回的过滤条件字典
    total_dict: 指定条件内total_hits, total_bytes, total_time, invalid_hits
    """
    pipeline = [match['basic_match'], {'$project': {'requests.errors': 1}}, {'$unwind': '$requests'}, {'$unwind': '$requests.errors'},
                {'$group': {'_id': '$requests.errors.error_code', 'hits': {'$sum': '$requests.errors.hits'},
                            'bytes': {'$sum': '$requests.errors.bytes'}, 'time': {'$sum': '$requests.errors.time'}}},
                {'$sort': {'hits': -1}}]
    pipeline[-2]['$group'].update(errors_q4_group_by)
    # 限制条数时,$sort + $limit 可以减少mongodb内部的操作量,若不限制显示条数,此步的mongodb内部排序将无必要
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
    print('{0}\nTotal_hits:{1} Errors:{2}\n{0}'.format('=' * 20, total_dict['total_hits'], total_dict['error_hits']))
    print('{}  {}  {}  {}  {}  {}'.format('error_code'.rjust(10), 'hits'.rjust(7), 'hits(%)'.rjust(7), 'bytes(%)'.rjust(8), 'time(%)'.rjust(7), 'time_distribution(s)'.center(30)))
    # 打印结果
    for one_doc in mongo_result:
        print('{}  {}  {}%  {}%  {}%  {}'.format(
            str(one_doc['_id']).rjust(10), str(one_doc['hits']).rjust(7),
            format(one_doc['hits'] / total_dict['total_hits'] * 100, '.2f').rjust(6),
            format(one_doc['bytes'] / total_dict['total_bytes'] * 100, '.2f').rjust(7),
            format(one_doc['time'] / total_dict['total_time'] * 100, '.2f').rjust(6),
            format('%50<{} %75<{} %100<{}'.format(round(one_doc['q2_time'], 2), round(one_doc['q3_time'], 2), round(one_doc['max_time'], 2))).ljust(30)))


def distribution(mongo_col, arguments):
    """
    展示error_code统计按照指定period做group聚合的结果
    mongo_col: 本次操作对应的集合名称
    arguments: docopt解析用户从log_show界面输入的参数而来的dict
    """
    groupby = arguments['--group_by']
    group_id = group_by_func(groupby)
    error_code = int(arguments['<error_code>'])
    limit = int(arguments['--limit'])

    match = match_condition(arguments['--server'], arguments['--from'], arguments['--to'], error_code=error_code)
    pipeline = [match['basic_match'], {'$project': {'requests.errors': 1}}, {'$unwind': '$requests'}, {'$unwind': '$requests.errors'},
                {'$match': {'$and': [{'requests.errors.error_code': error_code}]}},
                {'$group': {'_id': group_id, 'hits': {'$sum': '$requests.errors.hits'},
                            'bytes': {'$sum': '$requests.errors.bytes'}, 'time': {'$sum': '$requests.errors.time'}}},
                {'$sort': {'_id': 1}}]
    pipeline[-2]['$group'].update(errors_q4_group_by)
    if limit:
        pipeline.append({'$limit': limit})
    # print('distribution pipeline:\n', pipeline)  # debug
    mongo_result = mongo_col.aggregate(pipeline)

    total_project = {'$project': {'requests.errors': 1}}
    total_dict = total_info(mongo_col, match, project=total_project, error_code=error_code)

    # 打印表头
    print('{0}\nError_code:{1}  Total_hits:{2}  Total_bytes:{3}\n{0}'.format(
        '=' * 20, error_code, total_dict['total_hits'], get_human_size(total_dict['total_bytes'])))
    print('{}  {}  {}  {}  {}'.format(groupby.rjust(10), 'hits'.rjust(7), 'hits(%)'.rjust(7), 'bytes(%)'.rjust(8), 'time_distribution(s)'.center(30)))
    # 打印结果
    for one_doc in mongo_result:
        hits = one_doc['hits']
        bytes_ = one_doc['bytes']
        date = one_doc['_id']
        print('{}  {}  {}%  {}%  {}'.format(date.rjust(10), str(hits).rjust(7),
              format(hits / total_dict['total_hits'] * 100, '.2f').rjust(6),
              format(bytes_ / total_dict['total_bytes'] * 100, '.2f').rjust(7),
              format('%50<{} %75<{} %100<{}'.format(round(one_doc['q2_time'], 2), round(one_doc['q3_time'], 2), round(one_doc['max_time'], 2))).ljust(30)))


def detail(mongo_col, arguments):
    """
    展示指定error_code产生的各uri_abs的hits/bytes/time情况
    mongo_col: 本次操作对应的集合名称
    arguments: docopt解析用户从log_show界面输入的参数而来的dict
    """
    error_code = int(arguments['<error_code>'])
    limit = int(arguments['--limit'])

    match = match_condition(arguments['--server'], arguments['--from'], arguments['--to'], error_code=error_code)
    pipeline = [match['basic_match'], {'$project': {'requests.uri_abs': 1, 'requests.errors': 1}},
                {'$unwind': '$requests'}, {'$unwind': '$requests.errors'},
                {'$match': {'$and': [{'requests.errors.error_code': error_code}]}},
                {'$group': {'_id': '$requests.uri_abs', 'hits': {'$sum': '$requests.errors.hits'},
                            'bytes': {'$sum': '$requests.errors.bytes'}, 'time': {'$sum': '$requests.errors.time'}}},
                {'$sort': {'hits': -1}}]
    pipeline[-2]['$group'].update(errors_q4_group_by)
    if limit:
        pipeline.append({'$limit': limit})
    # print('detail pipeline:\n', pipeline)  # debug
    mongo_result = mongo_col.aggregate(pipeline)

    total_project = {'$project': {'requests.uri_abs': 1, 'requests.errors': 1}}
    total_dict = total_info(mongo_col, match, project=total_project, error_code=error_code)
    # 打印表头
    print('{0}\nError_code:{1}  Total_hits:{2}  Total_bytes:{3}  Total_time:{4}s\n{0}'.format(
        '=' * 20, error_code, total_dict['total_hits'], get_human_size(total_dict['total_bytes']), total_dict['total_time']))
    print('{}  {}  {}  {}  {}  uri_abs'.format('hits'.rjust(7), 'hits(%)'.rjust(7), 'bytes(%)'.rjust(8),
                                                  'time(%)'.rjust(7), 'time_distribution(s)'.center(30)))
    # 打印结果
    for one_doc in mongo_result:
        print('{}  {}%  {}%  {}%  {}  {}'.format(
            str(one_doc['hits']).rjust(7), format(one_doc['hits'] / total_dict['total_hits'] * 100, '.2f').rjust(6),
            format(one_doc['bytes'] / total_dict['total_bytes'] * 100, '.2f').rjust(7),
            format(one_doc['time'] / total_dict['total_time'] * 100, '.2f').rjust(6) if total_dict['total_time'] else 'null'.rjust(6),
            format('%50<{} %75<{} %100<{}'.format(round(one_doc['q2_time'], 2), round(one_doc['q3_time'], 2), round(one_doc['max_time'], 2))).ljust(30),
            one_doc['_id']))
