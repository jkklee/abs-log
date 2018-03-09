# -*- coding:utf-8 -*-
# ----日志格式
# 利用非贪婪匹配和分组匹配, 需要严格参照日志定义中的分隔符和引号(编写正则时，先不要换行，确保空格或引号与日志格式一致，最后考虑美观可以折行)
log_pattern = r'^(?P<remote_addr>.*?) - \[(?P<time_local>.*?) \+0800\] "(?P<request>.*?)"' \
              r' (?P<status>.*?) (?P<body_bytes_sent>.*?) (?P<request_time>.*?)' \
              r' "(?P<http_referer>.*?)" "(?P<http_user_agent>.*?)" - (?P<http_x_forwarded_for>.*)$'
# request的正则, 其实是由 "request_method request_uri server_protocol"三部分组成
request_uri_pattern = r'^(?P<request_method>(GET|POST|HEAD|DELETE|PUT|OPTIONS)?) ' \
                      r'(?P<request_uri>.*?) ' \
                      r'(?P<server_protocol>.*)$'

# ----日志相关
log_dir = '/data/nginx_log/'
# 要处理的站点(按需添加)
todo = ['www', 'm', 'user']

# ----mongodb
# 链接设置
mongo_host = '172.16.2.24'
mongo_port = 27017
# 存储设置
# mongodb存储结构为每个站点对应一个库一个集合(main), 每分钟每server产生一个统计结果文档(即分析粒度最小达到分钟级)
# 为了使mongodb数据集尽量小, 每分钟统计结果中, 只取点击数前MAX_URI_NUM的uri, 每个uri中点击数前MAX_ARG_NUM的args进行入库
MAX_URI_NUM = 80
MAX_ARG_NUM = 20
# ip统计, 每分钟统计结果中, 取点击数前MAX_IP_NUM的ip
MAX_IP_NUM = 30
# 保存几天的数据
LIMIT = 10

# ----uri或args抽象规则
"""
首先了解默认规则:
    uri中若 两个'/'之间 或 '/'和'.'之间 仅为数字则将其抽象为'*'
    args中所有参数的值抽象为'*'
举例:
    /abc/1.html ----> /abc/*.html
    /abc/11 ----> /abc/*
    /abc_11/33/22/page_11 ----> /abc11/*/*/page_11
    /abc/33/2a/11?uid=me&pass=321abc ----> /abc/*/2a/*?uid=*&pass=*
"""
# 自定义抽象规则:
"""
abs_special为dict: 其key为站点名(site_name); value即abs_special[site_name], 亦为dict
    abs_special[site_name]: 其key为能完全匹配某一类uri的正则表达式(uri_pattern); value即abs_special[site_name][uri_pattern], 亦为dict. 包含'uri_replace'和'arg_replace'两种key
        abs_special[site_name][uri_pattern]: 
            若其key为'uri_replace': 则表示将uri_pattern替换为abs_special[site_name][uri_pattern]['uri_replace']
            若其key为'arg_replace': 则abs_special[site_name][uri_pattern]['arg_replace']为dict, 其key为能完全匹配某一类args组合的正则表达式(arg_pattern)
                其value为用来替换arg_pattern的自定义规则字符串
替换字符串中要被抽象的部分均以单个'*'表示
对于替换字符串中药引用原字符串中内容的情况, 参照nginx rewrite规则, 将uri_pattern/arg_pattern正则中要保留的部分用()括起来表示分组，在替换字符串中可用\1,\2的形式进行引用

举例:
abs_special = {'api': {r'^/point/([0-9]+)/[0-9]+/[0-9]+\.json':
                           {'uri_replace': r'/viewPoint/\1/*/*.json',
                            'arg_replace': {r'^(channel=.+&version=.+)': r'\1'}},
                       r'^/v[0-9]/recommend\.json':
                           {'uri_replace': r'/v*/recommend.json'},
                       r'^/subscribe/read':
                           {'arg_replace':
                                {r'^uid=.+&type=.+&(channel=.+&version=.+)': r'uid=*&type=*&\1'}}}}
该配置表示api站点中:
    将uri '/point/123/456/789.html' 抽象为 '/point/123/*/*.html', 而不是默认规则的 '/point/*/*/*.html'
        将该uri的args 'channel=Android&version=2.7.3' 保留原样, 而不是默认规则的 'channel=*&version=*'
    将uri '/v3/recommend.json' 抽象为 '/v*/recommend.json', 而不是默认规则的 '/v3/recommend.json'
    将uri '/subscribe/read' 的args 'uid=111&channel=Android&version=2.7.3)' 抽象为 'uid=*channel=Android&version=2.7.3', 而不是默认规则的 'uid=*channel=*&version=*'
"""
abs_special = {}


