# -*- coding:utf-8 -*-
# ----nginx配置文件中定义的日志格式
log_format = '$remote_addr - [$time_local] "$request" $status $body_bytes_sent $request_time ' \
             '"$http_referer" "$http_user_agent" - $http_x_forwarded_for'

# ----日志相关
# 日志文件名格式必须为xxx.access[.log], 以便取得站点名称xxx(通过以'.access'进行分割取得)
log_dir = '/data/nginx_log/'
# 要处理的站点(按需添加)
todo = ['www', 'm', 'user']

# 错误日志级别(DEBUG, INFO, WARNING, ERROR)
error_level = 'INFO'

# ----mongodb
# 链接设置
mongo_host = '172.16.2.24'
mongo_port = 27017
# 存储设置
# mongodb存储结构为每个站点对应一个库一个集合(main), 每分钟每server产生一个统计结果文档(即分析粒度最小达到分钟级)
# 为了使mongodb数据集尽量小, 每分钟统计结果中, 取点击数前URI_STORE_MAX_NUM的uri进行入库,
# 同时若uri_abs在该分钟内点击数小于URI_STORE_MIN_HITS, 则该uri_abs不予入库
URI_STORE_MAX_NUM = 80
URI_STORE_MIN_HITS = 5
# ip统计, 每分钟统计结果中, 取点击数前IP_STORE_MAX_NUM的ip, 同时若ip在该分钟内点击数小于IP_STORE_MIN_HITS, 则该ip不予入库
IP_STORE_MAX_NUM = 30
IP_STORE_MIN_HITS = 3
# 保存几天的数据
LIMIT = 10

# ----uri或args抽象规则
"""
首先了解默认规则:
    uri中若 两个'/'之间 或 '/'和'.'之间仅由"0-9或-或_"组成,则将其抽象为'*'
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

