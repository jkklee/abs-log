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
exclude_ip = []

# ----mongodb
# 链接设置
mongo_host = '172.16.2.24'
mongo_port = 27017
# 存储设置
# mongodb存储结构为每个站点对应一个库, 每天对应一个集合, 每分钟每server产生一个统计结果文档(即分析粒度最小达到分钟级)
# 为了使mongodb数据集尽量小, 每分钟统计结果中, 只取点击数前MAX_URI_NUM的uri, 每个uri中点击数前MAX_ARG_NUM的args进行入库
MAX_URI_NUM = 100
MAX_ARG_NUM = 20

# 保存几天的数据
LIMIT = 10
