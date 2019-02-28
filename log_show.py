#!/bin/env python3
# -*- coding:utf-8 -*-
"""
Usage:
  log_show <site_name> [options] request [distribution [<request>]|detail <uri>]
  log_show <site_name> [options] ip [distribution <ip>|detail <ip>]
  log_show <site_name> [options] error [distribution <error_code>|detail <error_code>]

Options:
  -h --help                   Show this screen.
  -f --from <start_time>      Start time. Format: %y%m%d[%H[%M]], %H and %M is optional
  -t --to <end_time>          End time. Format is same as --from
  -l --limit <num>            Number of lines in output, 0 means no limit. [default: 5]
  -s --server <server>        Web server hostname
  -g --group_by <group_by>    Group by every minute, every ten minutes, every hour or every day,
                              valid values: "minute", "ten_min", "hour", "day". [default: hour]

  distribution                Show distribution(about hits,bytes,time,etc) of:
                              all or specific 'request', the specific 'ip', the specific 'error_code' in every period.
                              Period is specific by --group_by
  detail                      Show details of:
                              detail 'args' analyse of the specific 'uri'(if it has args);
                              detail 'uri' analyse of the specific 'ip' or 'error_code'

  Notice: it's best to put 'request_uri', 'uri' and 'ip' in quotation marks.
"""
from docopt import docopt

from common.common import mongo_client, match_condition, total_info
from common.show import request_show, ip_show, error_show

arguments = docopt(__doc__)
# print(arguments)  #debug
# 判断--group_by合理性
if arguments['--group_by'] not in ('minute', 'ten_min', 'hour', 'day'):
    exit("Error: --group_by must be one of {'minute', 'ten_min', 'hour', 'day'}")

db_name = arguments['<site_name>'].replace('.', '_')
if db_name not in mongo_client.list_database_names():
    exit("Error: {} (auto convert to {}) is not in mongodb".format(arguments['<site_name>'], db_name))

mongo_db = mongo_client[db_name]
# mongodb集合
mongo_col = mongo_db['main']
# 最基本的过滤条件
base_match = match_condition(arguments['--server'], arguments['--from'], arguments['--to'])

# 根据参数执行动作
if arguments['distribution'] and arguments['request']:
    request_show.distribution(mongo_col, arguments)
elif arguments['distribution'] and arguments['ip']:
    ip_show.distribution(mongo_col, arguments)
elif arguments['distribution'] and arguments['error']:
    error_show.distribution(mongo_col, arguments)

elif arguments['detail'] and arguments['request']:
    request_show.detail(mongo_col, arguments)
elif arguments['detail'] and arguments['ip']:
    ip_show.detail(mongo_col, arguments)
elif arguments['detail'] and arguments['error']:
    error_show.detail(mongo_col, arguments)

elif arguments['ip']:
    total_dict = total_info(mongo_col, base_match)
    ip_show.base_summary('last_cdn_ip', int(arguments['--limit']), mongo_col, base_match, total_dict)
    ip_show.base_summary('user_ip_via_cdn', int(arguments['--limit']), mongo_col, base_match, total_dict)
    ip_show.base_summary('user_ip_via_proxy', int(arguments['--limit']), mongo_col, base_match, total_dict)
    ip_show.base_summary('remote_addr', int(arguments['--limit']), mongo_col, base_match, total_dict)
elif arguments['error']:
    total_dict = total_info(mongo_col, base_match)
    error_show.base_summary(int(arguments['--limit']), mongo_col, base_match, total_dict)

else:
    total_dict = total_info(mongo_col, base_match)
    request_show.base_summary('hits', int(arguments['--limit']), mongo_col, base_match, total_dict)
    request_show.base_summary('bytes', int(arguments['--limit']), mongo_col, base_match, total_dict)
    request_show.base_summary('time', int(arguments['--limit']), mongo_col, base_match, total_dict)
