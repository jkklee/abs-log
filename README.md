# web_log_analyse
This tool aim at trouble shooting and performance optimization based on web logs, it's not a generally said log analyse/statistics solution. It preprocess logs on all web server with a specified period and save the intermediate results into mongodb for finally use(with `log_show.py`)

日志分析在日常中故障排查、性能分析方面有着非常重要的作用。该项目的侧重点不是通常的PV，UV等展示，而是在指定时间段内提供细粒度（最小分钟级别）的异常定位和性能分析。

## Dependencies
- Python 3.4+
- pymongo-3.7.2+
- MongoDB-server 3.4+

**先明确几个术语**：  
`uri`指不包含参数的请求；`request_uri`指原始的请求，包含参数；`args`指请求中的参数部分。（参照[nginx](http://nginx.org/en/docs/http/ngx_http_core_module.html#variables)中的定义）
`uri_abs`和`args_abs`是指对uri和args进行抽象处理后的字符串（以便分类），例如：  
`"/sub/0/100414/4070?channel=ios&version=1.4.5"`经抽象处理转换为`uri_abs:` "/sub/\*/\*/\*"，`args_abs:` "channel=\*&version=\*"

## 功能
1. 提供统一的日志分析入口：经由此入口，可查看站点在所有server上产生的日志的汇总分析；亦可根据`时间段`和`server`两个维度进行过滤
2. 支持对 `request_uri`，`ip` 和 `response_code` 三大类进行分析；每一类又基于`请求数`、`响应大小`、`响应时间`三个维度进行分析。另外不同子项又各有特点
3. `request_uri` 分析能直观展示哪类请求数量多、哪类请求耗时多、哪类请求占流量；另外可展示某一类请求在不同时间粒度里(minute, ten_min, hour, day)各指标随时间的分布变化；也可以针对某一 uri_abs 分析其不同 args_abs 各指标的分布
4. IP 分析将所有请求分为3种来源(from_cdn/proxy, from_reverse_proxy, from_client_directly)，三种来源各自展示其访问量前 N 的 IP 地址；并且可展示某一 IP 访问的各指标随时间的分布；也可针对某一 IP 分析其产生的不同 uri_abs 各指标的分布 

## 特点
1. **核心思想**: 对request_uri进行**抽象归类**，将其中变化的部分以 “\*” 表示，这样留下不变的部分就能代表具体的一类请求。实际上是换一种方式看待日志，从 “以具体的一行日志文本作为最小分析单位” 抽象上升到 “以某一功能点，某一接口或某一模块最为最小分析单位”
2. 兼容plaintext和json格式的日志内容
3. 配置方便，不需要写正则。只要将nginx中定义的log_format复制到config文件中即可
4. 通过4分位数概念以实现对`响应时间`和`响应大小`更准确的描述，因为对于日志中的响应时间，算数平均值的参考意义不大 
5. 支持定制抽象规则，可灵活指定请求中的某些部分是否要抽象处理以及该如何抽象处理
6. 高效，本着谁产生的日志谁处理的思想，日志分析脚本log_analyse要在web服务器上定时运行（有点类似分布式），因而log_analyse的高效率低资源也是重中之重。经测试，在笔者的服务器上（磁盘：3\*7200rpm RAID5，千兆局域网），处理速度在20000行/s~30000行/s之间
 
## 实现思路：
分析脚本（`log_analyse.py`）部署到各台web server，并通过crontab设置定时运行。`log_analyse.py`利用python的re模块通过正则表达式对日志进行分析处理，取得`uri`、`args`、`时间当前`、`状态码`、`响应大小`、`响应时间`、`server name` 等信息并进行初步加工然后存储进MongoDB。查看脚本（`log_show.py`）作为入口即可对所有web server的日志进行分析查看，至于实时性，取决于web server上`log_analyse.py`脚本的执行频率。

### 配置文件
 
日志格式决定了代码中的正则表达式，可根据自己情况参考`config.py`中的正则定义进行定制)。项目中预定义的日志格式对应如下：
```
LOG_FORMAT = '$remote_addr - [$time_local] "$request" '\
             '$status $body_bytes_sent $request_time "$http_referer" '\
             '"$http_user_agent" - $http_x_forwarded_for'
``` 

### 对于异常日志的处理  
如果想靠空格或双引号来分割各段的话，主要问题是面对各种不规范的记录时(原因不一而足，而且也是样式繁多)，无法做到将各种异常都考虑在内，所以项目中采用了`re`模块而不是简单的`split()`函数。代码里对一些“可以容忍”的异常记录通过一些判断逻辑予以处理；对于“无法容忍”的异常记录则返回空字符串并将日志记录于文件。  
其实对于上述的这些不规范的请求，最好的办法是在nginx中定义日志格式时，用一个特殊字符作为分隔符，例如“|”。这样就不需要re模块，直接字符串分割就能正确的获取到各段(性能会好些)。

## log_show.py使用说明：
### 帮助信息
```
[ljk@demo ~]$ log_show --help
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
```
所有示例均可通过`-f`，`-t`，`-s`参数对`起始时间`和`指定server`进行过滤  

### request子命令：
对指定站点今日已入库的数据进行分析
```
[ljk@demo ~]$ log_show api request -l 3
====================
Total_hits:999205 invalid_hits:581
====================
      hits  percent      time_distribution(s)         bytes_distribution(B)            uri_abs
    430210   43.06%  %50<0.03 %75<0.06 %100<2.82   %50<61 %75<63 %100<155              /api/record/getR
    183367   18.35%  %50<0.03 %75<0.06 %100<1.73   %50<196 %75<221 %100<344            /api/getR/com/*/*/*
    102299   10.24%  %50<0.02 %75<0.05 %100<1.77   %50<3862 %75<3982 %100<4512         /view/*/*/*/*.js
====================
Total_bytes:1.91 GB
====================
     bytes  percent      time_distribution(s)         bytes_distribution(B)            uri_abs
   1.23 GB   64.61%  %50<0.04 %75<0.1 %100<1.96    %50<17296 %75<31054 %100<691666     /api/NewCom/list
 319.05 MB   16.32%  %50<0.02 %75<0.05 %100<1.77   %50<3862 %75<3982 %100<4512         /view/*/*/*/*.js
 167.12 MB    8.55%  %50<0.19 %75<0.55 %100<2.93   %50<3078 %75<3213 %100<11327        /api/getR/com/*/*
====================
Total_time:117048s
====================
 cum. time  percent      time_distribution(s)         bytes_distribution(B)            uri_abs
     38747   33.10%  %50<0.03 %75<0.06 %100<2.82   %50<61 %75<63 %100<155              /api/record/getR
     22092   18.87%  %50<0.03 %75<0.06 %100<1.73   %50<196 %75<221 %100<344            /api/getR/com/*/*/*
     17959   15.34%  %50<0.19 %75<0.55 %100<2.93   %50<3078 %75<3213 %100<11327        /api/getRInfo/com/*/*
```
通过上例可观察指定时间内（默认当天0时至当前时间）hits/bytes/time三个维度的排名以及响应时间和响应大小的分布情况。例如，看到某个uri_abs只有比较少的hits确产生了比较大的bytes或耗费了较多的time，那么该uri_abs是否值得关注一下呢。  

### ip子命令：
显示基于ip地址的分析结果
```
[ljk@demo ~]$ log_show.py api ip -l 2
====================
From_cdn/Proxy:              hits  hits(%)       bytes  bytes(%)  time(%)
====================       199870    99.94   570.51 MB    99.99    99.99
          Last_cdn_ip
       xxx.57.xxx.189        1914     0.96   696.18 KB     0.12     0.68
      xxx.206.xxx.154        1741     0.87     1.56 MB     0.27     0.98
      User_ip_via_cdn
       xxx.249.xxx.56         787     0.39   154.82 KB     0.03     0.23
        xxx.60.xxx.86         183     0.09     1.05 MB     0.18     0.13
====================
From_reverse_proxy:          hits  hits(%)       bytes  bytes(%)  time(%)
====================           66     0.03    68.83 KB     0.01     0.01
    User_ip_via_proxy
       xxx.188.xxx.21           2     0.00     1.53 KB     0.00     0.00
          xxx.5.xxx.4           2     0.00    324.00 B     0.00     0.00
====================
From_client_directly:        hits  hits(%)       bytes  bytes(%)  time(%)
====================           64     0.03     8.32 KB     0.00     0.00
          Remote_addr
        192.168.1.202          29     0.01     58.00 B     0.00     0.00
        192.168.1.200          29     0.01     58.00 B     0.00     0.00
```
IP分析的思想是将请求按来源归为三大类：From_cdn/Proxy，From_reverse_proxy，From_client_directly，然后各自分类内按请求次数对IP地址进行排序 

### distribution 子命令：
1. 对 “所有request” 或 “指定uri/request_uri” 按 “分/十分/时/天” 为粒度进行聚合统计  
2. 对 “指定IP” 按 “分/十分/时/天” 为粒度进行聚合统计  

适用场景：查看request/IP随时间在各聚合粒度内各项指标的变化情况，例如针对某个uri发现其请求数（或带宽）变大，则可通过`distribution`子命令观察是某一段时间突然变大呢，还是比较平稳的变大  
```
# 示例1: 分析指定request的分布情况, 指定按minute进行分组聚合, 默认显示5行
[ljk@demo ~]$ python log_show.py api request distribution "/view/*/*.json" -g minute                
====================
uri_abs: /view/*/*.json
Total_hits: 17130    Total_bytes: 23.92 MB
====================
    minute        hits  hits(%)       bytes  bytes(%)      time_distribution(s)          bytes_distribution(B)            
1803091654        1543    9.01%     2.15 MB     8.98%  %50<0.03 %75<0.05 %100<1.07   %50<1593 %75<1645 %100<1982        
1803091655        1527    8.91%     2.13 MB     8.88%  %50<0.04 %75<0.05 %100<1.04   %50<1592 %75<1642 %100<2143        
1803091656        1464    8.55%     2.05 MB     8.57%  %50<0.04 %75<0.05 %100<1.03   %50<1592 %75<1642 %100<1952        
1803091657        1551    9.05%     2.15 MB     8.97%  %50<0.03 %75<0.04 %100<0.89   %50<1594 %75<1639 %100<1977        
1803091658        1458    8.51%     2.06 MB     8.61%  %50<0.03 %75<0.04 %100<2.35   %50<1596 %75<1644 %100<2146
```
通过上例，可展示"/view/\*/\*.json"在指定时间段内的分布情况，包括hits/bytes/time总量以及每个粒度内个指标相对于总量的占比；该子命令亦能展示各指标随时间的“趋势”。  
**说明：**  
minute字段为指定的聚合（group）粒度，1803091654 表示“18年03月09日16时54分”   
可通过`-g`参数指定聚合的粒度（minute/ten_min/hour/day）  
`distribution`子命令后可以跟具体的uri/request_uri（显示该uri/request_uri以指定粒度随时间的分布）或不跟uri（显示所有请求以指定粒度随时间的分布）  
```
# 示例2: 分析指定IP产生的请求数/带宽随时间分布情况, 默认聚合粒度为hour
[ljk@demo ~]$ python log_show.py api ip -t 180314 distribution "140.206.109.174" -l 0
====================
IP: 140.206.109.174
Total_hits: 10999    Total_bytes: 4.83 MB
====================
      hour        hits  hits(%)       bytes  bytes(%)
  18031306        1273   11.57%   765.40 KB    15.47%
  18031307        2133   19.39%  1004.74 KB    20.31%
  18031308        2211   20.10%     1.00 MB    20.74%
  18031309        2334   21.22%     1.05 MB    21.72%
  18031310        2421   22.01%   850.79 KB    17.20%
  18031311         627    5.70%   226.30 KB     4.57%
```
**说明：**  
hour字段表示默认的聚合粒度，18031306表示“18年03月13日06时”  
-l 0 表示不限制输出行数（即输出所有结果）

### detail 子命令：
1. 对某一uri进行详细分析，查看其不同参数（args）的各项指标分布  
2. 对某一IP进行详细分析，查看其产生的请求在不同uri_abs间的分布情  

适用场景：比如定位到某一类型的uri_abs在某方面（hits/bytes/time）有异常，就可以通过detail子命令对该类uri_abs进行更近一步的分析，精确定位到是哪种参数（args_abs）导致的异常；或者观察到某个IP访问异常，可以再深入一下该IP是泛泛的访问呢，还是只对某些uri感兴趣。
```
# 示例1:
[ljk@demo ~]$ python log_show.py api -f 180201 request detail "/recommend/update" -l 3
====================
uri_abs: /recommend/batchUpdate
Total_hits: 10069    Total_bytes: 7.62 MB
====================
    hits  hits(%)      bytes  bytes(%)  time(%)      time_distribution(s)          bytes_distribution(B)      args_abs
    4568   45.37%    3.46 MB    45.44%   47.96%  %50<0.06 %75<0.07 %100<0.47   %50<795 %75<845 %100<1484      uid=*&category_id=*&channel=*&version=*
    4333   43.03%    3.25 MB    42.64%   42.30%  %50<0.05 %75<0.07 %100<0.48   %50<791 %75<840 %100<1447      category_id=*&channel=*&uid=*&version=*
     389    3.86%  314.15 KB     4.03%    0.88%  %50<0.03 %75<0.04 %100<0.06   %50<802 %75<850 %100<1203      category_id=*&channel=*&version=*
```
通过上例可观察到"/recommend/update"这个uri所对应的不同参数各个指标的情况。另外还有一个附带的发现：开发在书写参数时相同的参数组合没有按同一个顺序书写，虽不影响功能，但在精准的进行应用性能监控的时候会造成一定困扰。  
**说明：**  
`detail`子命令后跟随uri（不含参数，含参数的话将忽略参数）  
```
# 示例2: 观察某个IP分别产生了多少种请求, 每种请求的(hits/bytes/time)指标
[ljk@demo ~]$ python log_show.py m -t 180314 ip detail "1.2.3.4"
====================
IP: 140.206.109.174
Total_hits: 10999    Total_bytes: 4.83 MB
====================
    hits  hits(%)      bytes  bytes(%)  time(%)  uri_abs
   10536   95.79%  405.47 KB     8.19%   92.01%  /introduction/watch
     147    1.34%    1.90 MB    39.31%    1.93%  /view/*/*.html
     138    1.25%  407.42 KB     8.23%    2.41%  /chapinfo/*/*.html
      42    0.38%  644.88 KB    13.03%    1.38%  /info/*.html
      30    0.27%  229.98 KB     4.65%    1.14%  /classify/*.json
```

## log_analyse.py部署说明：
该脚本的设计目标是将其放到web server的的计划任务里，定时（例如每30分钟或10分钟，自定义）执行，在需要时通过log_show.py进行分析即可。
```
*/15 * * * * export LANG=zh_CN.UTF-8;python3 /home/ljk/log_analyse.py &> /tmp/log_analyse.log
```

## Note
1. 其中`uri_abs`和`args_abs`是对uri和args进行抽象化（抽象出特定的请求模式，即将请求分类看待）处理之后的结果，默认规则如下    
 **uri**：将request_uri以"/"和"."分割为几段，若某一段全部由数字组成则将其抽象为一个"\*"    
 **args**：将所有的value替换成"*"  
2. `common/common.py`中还有一些其他有趣的函数
