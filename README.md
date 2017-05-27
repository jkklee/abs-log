# web_log_analyse
This tool is not a generally said log analyse/statistics solution. It aim at trouble shooting and performance optimization based on web logs


日志分析在web系统中故障排查、性能分析方面有着非常重要的作用。目前，开源的ELK系统是成熟且功能强大的选择。但是部署及学习成本亦然不低，这里我实现了一个方法和功能相对简单但有针对性的实现。另外该脚本的侧重点不是通常的PV，UV等展示，而是短期内提供细粒度（分钟级别，一分钟内的日志做抽象和汇总）的异常定位和性能分析。

##### 先说一下我想实现这个功能的驱动力（痛点）吧：
我们有不少站点，前边有CDN，原站前面是F5，走到源站的访问总量日均PV约5000w。下面是我们经常面临一些问题：

 - CDN回源异常，可能导致我们源站带宽和负载都面临较大的压力。这时需要能快速的定位到是多了哪些回源IP（即CDN节点）或是某个IP的回源量异常，又或是哪些url的回源量异常
 - 在排除了CDN回源问题之后，根据zabbix监控对一些异常的流量或者负载波动按异常时段对比正常时段进行分析，定位到具体的某（几）类url。反馈给开发进行review以及优化
 - 有时zabbix会监控到应用服务器和DB或者缓存服务器之间的流量异常，这种问题一般定位起来是比较麻烦的，甚至波动仅仅是在一两分钟内，这就需要对日志有一个非常细的分析粒度
 - 我们希望能所有的应用服务器能过在本机分析日志（分布式的思想），然后将分析结果汇总到一起以便查看；并且还希望能尽可能的**实时**（将定时任务间隔设置短一些），以便发现问题后能尽快的通过此平台进行分析  
 -  **通用**和**性能**：对于不同的日志格式只需对脚本稍加改动即可分析；因为将日志分析放在应用服务器本机，所以脚本的性能和效率也要有保证，不能影响业务

 
### 实现思路：
比较简单，就是利用python的re模块通过正则表达式对日志进行分析处理，取得`uri`、`args`、`时间当前`、`状态码`、`响应大小`、`响应时间`、`用户IP`、`CDN ip`、`server name` 等信息存储进MongoDB。

#### 当然前提规范也是必须的：

 - 各台server的日志文件按统一路径存放
 - 日志格式保持一致(代码中规定格式为xxx.access.xxxx)
 - 每天的0点日志切割
 
我的nginx日志格式如下(日志格式决定了代码中的正则表达式，是可根据自己情况参考我的正则进行定制的)：
```
log_format  access  '$remote_addr - [$time_local] "$request" '
             '$status $body_bytes_sent $request_time "$http_referer" '
             '"$http_user_agent" - $http_x_forwarded_for';
```
#### 日志分析原理： 
通过Python的re模块，按照应用服务器的日志格式编写正则，例如按照我的日志格式，写出的正则如下（编写正则时，先不要换行，**确保空格或引号等与日志格式一致**，最后考虑美观可以折行）
```
log_pattern = r'^(?P<remote_addr>.*?) - \[(?P<time_local>.*?)\] "(?P<request>.*?)"' \
              r' (?P<status>.*?) (?P<body_bytes_sent>.*?) (?P<request_time>.*?)' \
              r' "(?P<http_referer>.*?)" "(?P<http_user_agent>.*?)" - (?P<http_x_forwarded_for>.*)$'
              
log_pattern_obj = re.compile(log_pattern)
```
用以上正则来整体匹配一行日志记录，然后各个部分可以通过`log_pattern_obj.search(log).group('remote_addr')`、`log_pattern_obj.search(log).group('body_bytes_sent')`等形式来访问  

#### 对于其他格式的nginx日志或者Apache日志，按照如上原则，都可以轻松的使用该脚本分析处理。

原理虽简单但实现起来却发现有好多坑，如果想靠空格或双引号来分割各段的话，主要问题是面对各种不规范的记录时(原因不一而足，而且也是样式繁多)，无法做到将各种异常都考虑在内，所以我采用了`re`模块而不是简单的`split()`函数的原因。代码里对一些“可以容忍”的异常记录通过一些判断逻辑予以处理；对于“无法容忍”的异常记录则返回空字符串并将日志记录于文件。

其实对于上述的这些不规范的请求，最好的办法是在nginx中定义日志格式时，用一个特殊字符作为分隔符，例如“|”。这样都不用Python的re模块，直接字符串分割就能正确的获取到各段(性能会好些)。

### 接下来看看使用效果：
#### 帮助信息
```
[ljk@demo ~]$ log_show --help
Usage:
  log_show <site_name> [options] [(-f <start_time>|-f <start_time> -t <end_time>)] [(-u <uri> [(--distribution|--detail)]|-r <request_uri>)]

Options:
  -h --help                       Show this screen.
  -f --from <start_time>          Start time.Format: %y%m%d[%H[%M]], %H and %M is optional
  -t --to <end_time>              End time.Same as --from
  -l --limit <num>                Number of lines in the output, 0 means no limit. [default: 20]
  -s --server <server>            Web server hostname
  -u --uri <uri>                  URI in request, must in a pair of quotes 
  -r --request_uri <request_uri>  Full original request URI (with arguments), must in a pair of quotes
  -g --group_by <group_by>        Group by every minute or every ten minutes or every hour or every day
                                  Valid values: "min", "ten_min", "hour", "day". [default: min]
  --distribution                  Display result of -u or -r within every period which --group_by specific
  --detail                        Display detail of args of -u or -r specific
```
#### 默认对指定站点今日已入库的数据进行分析，从访问次数、字节数、响应时间三个维度打印出前20（不加-l参数）个uri_abs(经抽象处理的不含参数的uri)
```
[ljk@demo ~]$ log_show  -l 5 api
====================
Total hits: 13003013
====================
      hits       percent    uri_abs
   2813039        21.63%    /subscribe/?/?/?
   1480372        11.38%    /chapter/?/?.json
   1445657        11.12%    /subscribe/read
   1243056         9.56%    /recommend/update
   1181373         9.09%    /view/?/?.json
====================
Total bytes: 24.16 GB
====================
     bytes       percent     avg_bytes    uri_abs
   4.54 GB        18.77%       4.08 KB    /point/?/?/?.json
   2.55 GB        10.56%       7.56 KB    /comment/?/?.json
   2.53 GB        10.47%       9.11 KB    /center/subscribe
   2.50 GB        10.36%       5.27 KB    /comic/?.json
   2.25 GB         9.30%       1.59 KB    /chapter/?/?.json
====================
Total cum. time: 1802117s
====================
 cum. time       percent      avg_time    uri_abs
   472374s        26.21%        0.647s    /comment/topcomment/?/?/?.json
   407161s        22.59%        2.344s    /old/comment/?/?/?/?.json
   207505s        11.51%        1.620s    /comment/?/?.json
   154559s         8.58%        0.437s    /comment/?/?/?/?.json
    95661s         5.31%        0.034s    /subscribe/?/?/?
```
#### 对执行的uri(without query strings)或request_uri(full uri)在个时间段的各项统计(时间段可按分/十分/时/天划分)
```
# 默认按分钟分组,默认显示20行, 通过'-l 0'参数可以显示所有结果 
[ljk@demo ~]$ log_show  -l 5 api -u "/subscribe/?/?/?"
====================
uri_abs: /subscribe/?/?/?
Total hits: 2813039    Total bytes: 107.31 MB    Avg_time: 0.034
====================
       min        hits  hits_percent       bytes  bytes_percent    avg_time
1705270000        7404         0.26%   289.22 KB          0.26%      0.039s
1705270001        7461         0.27%   291.45 KB          0.27%      0.038s
1705270002        7333         0.26%   286.45 KB          0.26%      0.038s
1705270003        7383         0.26%   288.40 KB          0.26%      0.035s
1705270004        7267         0.26%   283.87 KB          0.26%      0.035s
```
#### 对某一uri进行详细分析，查看其不同参数(query_string)的分布汇总
```
[ljk@demo ~]$ log_show api -u "/subscribe/?/?/?" --detail
====================
uri_abs: /subscribe/?/?/?
Total hits: 2813039    Total bytes: 107.31 MB    Avg_time: 0.034
====================
      hits  hits_percent       bytes  bytes_percent    avg_time  args_abs
   2141750        76.14%    81.70 MB         76.14%      0.034s  ""
    671289        23.86%    25.61 MB         23.86%      0.035s  channel=?
```
#### Note
其中`uri_abs`和`args_abs`是对uri和args进行抽象化（抽象出一个模式出来）处理之后的结果。  
 对uri：将路径中任意一段全部由数字组成的抽象为一个"?"；将文件名出去后缀部分全部由数字组成的部分抽象为一个"?"  
 对args：将所有的value替换成"？"  


以上只列举了几个例子，还支持指定时间段内的查询分析。
基本上除了UA部分（代码中已有捕捉，但是笔者用不到），其他的信息都以包含到表中。因此几乎可以对网站`流量`，`负载`,`响应时间`等方面的任何疑问给出数据上的支持。


### 使用说明：
该脚本的设计目标是将其放到web server的的计划任务里，定时（例如每30分钟或10分钟，自定义）执行，在需要时通过log_show进行需要的分析即可。  
`*/30 * * * * export LANG=zh_CN.UTF-8;python3 /root/log_analyse_parall.py &> /tmp/log_analyse.log`


