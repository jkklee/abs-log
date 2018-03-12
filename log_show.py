#!/bin/env python3
# -*- coding:utf-8 -*-
"""
Usage:
  log_show <site_name> [options] [ip|error]
  log_show <site_name> [options] distribution (request [<request>]|ip <ip>|error <error_code>)
  log_show <site_name> [options] detail (request <uri>|ip <ip>|error <error_code> )

Options:
  -h --help                   Show this screen.
  -f --from <start_time>      Start time. Format: %y%m%d[%H[%M]], %H and %M is optional
  -t --to <end_time>          End time. Format is same as --from
  -l --limit <num>            Number of lines in output, 0 means no limit. [default: 5]
  -s --server <server>        Web server hostname
  -g --group_by <group_by>    Group by every minute, every ten minutes, every hour or every day,
                              valid values: "minute", "ten_min", "hour", "day". [default: hour]

  distribution                Show distribution(about hits,bytes,time) of all request;
                              or distribution of specific 'uri' or 'request_uri' in every period;
                              or distribution of the specific 'ip' in every period.
                              Period is specific by --group_by
  detail                      Show details of 'args' analyse of the specific 'uri'(if it has args);
                              or details of 'uri' analyse of the specific 'ip'

  Notice: it's best to put 'request_uri', 'uri' and 'ip' in quotation marks.
"""

from docopt import docopt
from common.common import mongo_client, match_condition, total_info
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
# 最基本的过滤条件
base_match = match_condition(arguments['--server'], arguments['--from'], arguments['--to'])

# 根据参数执行动作
if arguments['distribution'] and arguments['request']:
    base_show.distribution(arguments['<request>'], arguments['--group_by'], int(arguments['--limit']), mongo_col, arguments)
elif arguments['distribution'] and arguments['ip']:
    ip_show.distribution(mongo_col, arguments)
elif arguments['distribution'] and arguments['error']:
    print('To be implement...')

elif arguments['detail'] and arguments['request']:
    base_show.detail(arguments['<uri>'], int(arguments['--limit']), mongo_col, arguments)
elif arguments['detail'] and arguments['ip']:
    ip_show.detail(mongo_col, arguments)
elif arguments['detail'] and arguments['error']:
    print('To be implement...')

elif arguments['ip']:
    total_dict = total_info(mongo_col, base_match)
    ip_show.base_summary('last_cdn_ip', int(arguments['--limit']), mongo_col, base_match, total_dict)
    ip_show.base_summary('user_ip_via_cdn', int(arguments['--limit']), mongo_col, base_match, total_dict)
    ip_show.base_summary('user_ip_via_proxy', int(arguments['--limit']), mongo_col, base_match, total_dict)
    ip_show.base_summary('remote_addr', int(arguments['--limit']), mongo_col, base_match, total_dict)
elif arguments['error']:
    print('To be implement...')

else:
    total_dict = total_info(mongo_col, base_match)
    base_show.base_summary('hits', int(arguments['--limit']), mongo_col, base_match, total_dict)
    base_show.base_summary('bytes', int(arguments['--limit']), mongo_col, base_match, total_dict)
    base_show.base_summary('time', int(arguments['--limit']), mongo_col, base_match, total_dict)
