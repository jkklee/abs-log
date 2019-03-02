# -*- coding:utf-8 -*-

# nginx日志行类型: 'plaintext' or 'json'
LOG_TYPE = 'plaintext'

# nginx配置文件中定义的日志格式
# plaintext
LOG_FORMAT = '$remote_addr - [$time_local] "$request" '\
             '$status $body_bytes_sent $request_time "$http_referer" '\
             '"$http_user_agent" - $http_x_forwarded_for'
"""
# json
LOG_FORMAT = '{"timestamp":"$time_iso8601",'\
                     '"remote_addr":"$remote_addr",'\
                     '"scheme":"$scheme",'\
                     '"http_host":"$http_host",'\
                     '"method":"$request_method",'\
                     '"uri":"$uri",'\
                     '"args":"$args",'\
                     '"request_time":"$request_time",'\
                     '"status":"$status",'\
                     '"request_length":"$request_length",'\
                     '"body_bytes_sent":"$body_bytes_sent",'\
                     '"http_referer":"$http_referer",'\
                     '"http_user_agent":"$http_user_agent",'\
                     '"http_x_forwarded_for":"$http_x_forwarded_for"}'
"""

# ----日志相关
LOG_PATH = '/data/nginx_log/*access.log'
# 要排除的站点
EXCLUDE = ['www.access.log', 'm_access.log']

# 错误日志级别(DEBUG, INFO, WARNING, ERROR)
ERROR_LEVEL = 'INFO'

# ----mongodb
# 链接设置
MONGO_HOST = '172.16.2.24'
MONGO_PORT = 27017
# 存储设置
# mongodb存储结构为每个站点对应一个库一个集合(main), 每分钟每server产生一个统计结果文档(即分析粒度最小达到分钟级)
# 为了使mongodb数据集尽量小, 每分钟统计结果中, 取点击数前URI_STORE_MAX_NUM的uri进行入库,
URI_STORE_MAX_NUM = 80
# ip统计, 每分钟统计结果中, 取点击数前IP_STORE_MAX_NUM的ip
IP_STORE_MAX_NUM = 30

# mongodb中保存几天的数据
LIMIT = 10
# 批量插入: 累计多少分钟的处理结果执行一次入库(因为程序处理每分钟的原始日志生成一个该分钟的文档插入mongodb)
BATCH_INSERT = 100

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
abs_special为dict: 其key为日志文件名(log_name); value亦为dict
    abs_special[log_name]: 其key为能完全匹配某一类uri的正则表达式(uri_pattern); value即abs_special[site_name][uri_pattern], 亦为dict. 包含'uri_replace'和'arg_replace'两种key
        abs_special[site_name][uri_pattern]: 
            若其key为'uri_replace': 则表示将uri_pattern替换为abs_special[site_name][uri_pattern]['uri_replace']
            若其key为'arg_replace': 则abs_special[site_name][uri_pattern]['arg_replace']为dict, 其key为能完全匹配某一类args组合的正则表达式(arg_pattern)
                其value为用来替换arg_pattern的自定义规则字符串
替换字符串中要被抽象的部分均以单个'*'表示
对于替换字符串中药引用原字符串中内容的情况, 参照nginx rewrite规则, 将uri_pattern/arg_pattern正则中要保留的部分用()括起来表示分组，在替换字符串中可用\1,\2的形式进行引用

举例:
ABS_SPECIAL = {'api_access.log': {
    '^/point/([0-9]+)/[0-9]+/[0-9]+\.json': {
        'uri_replace': '/viewPoint/\1/*/*.json',
        'arg_replace': {'^(channel=.+&version=.+)': '\1'}},
    '^/v[0-9]/recommend\.json': {
        'uri_replace': '/v*/recommend.json'},
    '^/subscribe/read': {
        'arg_replace': {'^uid=.+&type=.+&(channel=.+&version=.+)': 'uid=*&type=*&\1'}}
    }
}
该配置表示api_access.log中:
    将uri '/point/123/456/789.html' 抽象为 '/point/123/*/*.html', 而不是默认规则的 '/point/*/*/*.html'
        将该uri的args 'channel=Android&version=2.7.3' 保留原样, 而不是默认规则的 'channel=*&version=*'
    将uri '/v3/recommend.json' 抽象为 '/v*/recommend.json', 而不是默认规则的 '/v3/recommend.json'
    将uri '/subscribe/read' 的args 'uid=111&channel=Android&version=2.7.3)' 抽象为 'uid=*channel=Android&version=2.7.3', 而不是默认规则的 'uid=*channel=*&version=*'
"""
ABS_SPECIAL = {}


