#!/bin/env python3
# -*- coding:utf-8 -*-
"""
Usage:
  log_show <site_name> [options] [ip|error_code]
  log_show <site_name> [options] distribution (request [<request_uri>]|ip <ip>)
  log_show <site_name> [options] detail (<request_uri>|<ip>)

Options:
  -h --help                   Show this screen.
  -f --from <start_time>      Start time. Format: %y%m%d[%H[%M]], %H and %M is optional
  -t --to <end_time>          End time. Format is same as --from
  -l --limit <num>            Number of lines in output, 0 means no limit. [default: 5]
  -s --server <server>        Web server hostname
  -g --group_by <group_by>    Group by every minute, every ten minutes, every hour or every day,
                              valid values: "minute", "ten_min", "hour", "day". [default: hour]

  distribution                Show distribution(about hits,bytes,time) of request_uri in every period,
                              or distribution of the specific ip in every period. Period is specific by --group_by
  detail                      Display details of args analyse of the request_uri(if it has args),
                              or details of the specific ip

  Notice: <request_uri> should inside quotation marks
"""

from docopt import docopt
from common.common import mongo_client, today, base_condition, group_by_func, text_abstract, get_human_size, time, total_info
from common.show import base_show, ip_show

arguments = docopt(__doc__)
# print(arguments)  #debug
# 判断--group_by合理性
if arguments['--group_by'] not in ('minute', 'ten_min', 'hour', 'day'):
    print("  Warning: --group_by must be one of {'minute', 'ten_min', 'hour', 'day'}")
    exit(10)

mongo_db = mongo_client[arguments['<site_name>']]
# mongodb集合
mongo_col = mongo_db['main']

# 根据参数执行动作
if arguments['distribution'] and arguments['request']:
    base_show.distribution(arguments['<request_uri>'], arguments['--group_by'], int(arguments['--limit']), mongo_col, arguments)
elif arguments['distribution'] and arguments['ip']:
    pass
elif arguments['detail'] and arguments['<request_uri>']:
    base_show.detail(arguments['<request_uri>'], int(arguments['--limit']), mongo_col, arguments)
elif arguments['detail'] and arguments['<ip>']:
    pass
elif arguments['ip']:
        pass
elif arguments['error_code']:
    pass
else:
    base_show.base_summary('hits', int(arguments['--limit']), arguments, mongo_col)
    base_show.base_summary('bytes', int(arguments['--limit']), arguments, mongo_col)
    base_show.base_summary('time', int(arguments['--limit']), arguments, mongo_col)
