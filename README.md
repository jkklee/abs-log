# web_log_analyse
This tool aim at trouble shooting and performance optimization based on web logs, it's not a generally said log analyse/statistics solution. It preprocess logs on all web server with a specified period and save the intermediate results into mongodb for finally use(with `log_show.py`)

日志分析在web系统中故障排查、性能分析方面有着非常重要的作用。该工具的侧重点不是通常的PV，UV等展示，而是在指定时间段内提供细粒度（最小分钟级别，即一分钟内的日志做**抽象**和**汇总**）的异常定位和性能分析。

**先明确几个术语**：  
`uri`指请求中不包含参数的部分；`request_uri`指原始的请求，包含参数或者无参数；`args`指请求中的参数部分。（参照[nginx](http://nginx.org/en/docs/http/ngx_http_core_module.html#variables)中的定义）  
`uri_abs`和`args_abs`是指对uri和args进行抽象处理后的字符串（以便分类）。例如`"/sub/0/100414/4070?channel=ios&version=1.4.5"`经抽象处理转换为`uri_abs: /sub/?/?/?  args_abs: channel=?&version=?`

### 特点
1. 提供一个日志分析的总入口：经由此入口，可查看某站点所有 server 产生日志的汇总分析；亦可根据`时间段`和`server`两个维度进行过滤
2. 侧重于以**某一类** uri 或其对应的**各类** args 为维度进行分析（参照3）
3. 对 request_uri 进行抽象处理，分为uri_abs和args_abs两部分，以实现对uri和uri中包含的args进行归类分析
4. 展示界面对归类的 uri 或 args 从`请求数`、`响应大小`、`响应时间`三个维度进行展示，哪些请求数量多、那些请求耗时多、哪些请求耗流量一目了然
5. 引入了4分位数的概念以实现对`响应时间`和`响应大小`更准确的描述，因为对于日志中的响应时间，算数平均值的参考意义不大
 
### 实现思路：
分析脚本（`log_analyse.py`）部署到各台 web server，并通过crontab设置定时运行。`log_analyse.py`利用python的re模块通过正则表达式对日志进行分析处理，取得`uri`、`args`、`时间当前`、`状态码`、`响应大小`、`响应时间`、`server name` 等信息存储进MongoDB。查看脚本（`log_show.py`）作为一个总入口即可对所有web server的日志进行分析查看，至于实时性，取决于web server上`log_analyse.py`脚本的执行频率。

#### 前提规范：
 - 各台server的日志文件按统一路径存放
 - 日志格式、日志命名规则保持一致(代码中规定格式为xxx.access.log)
 - 每天的0点日志切割
 
日志格式决定了代码中的正则表达式，是可根据自己情况参考`analyse_config.py`中的正则定义进行定制的)。项目中预定义的日志格式对应如下：
```
log_format  access  '$remote_addr - [$time_local] "$request" '
             '$status $body_bytes_sent $request_time "$http_referer" '
             '"$http_user_agent" - $http_x_forwarded_for';
``` 
#### 对于其他格式的nginx日志或者Apache日志，按照如上原则，稍作就可以使用该工具分析处理。

#### 对于异常日志的处理  
如果想靠空格或双引号来分割各段的话，主要问题是面对各种不规范的记录时(原因不一而足，而且也是样式繁多)，无法做到将各种异常都考虑在内，所以项目中采用了`re`模块而不是简单的`split()`函数的原因。代码里对一些“可以容忍”的异常记录通过一些判断逻辑予以处理；对于“无法容忍”的异常记录则返回空字符串并将日志记录于文件。  
其实对于上述的这些不规范的请求，最好的办法是在nginx中定义日志格式时，用一个特殊字符作为分隔符，例如“|”。这样就不需要re模块，直接字符串分割就能正确的获取到各段(性能会好些)。

### log_show.py使用说明：
#### 帮助信息
```
[ljk@demo ~]$ log_show --help
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
```

#### 默认对指定站点今日已入库的数据进行分析，默认按点击量倒序排序取前5名
```
[ljk@demo ~]$ log_show api -l 3
====================
Total_hits:999205 invalid_hits:581
====================
      hits  percent           time_distribution(s)                     bytes_distribution(B)              uri_abs
    430210   43.06%  %25<0.01 %50<0.03 %75<0.06 %100<2.82   %25<42 %50<61 %75<63 %100<155                 /api/record/getR
    183367   18.35%  %25<0.02 %50<0.03 %75<0.06 %100<1.73   %25<34 %50<196 %75<221 %100<344               /api/getR/com/?/?/?
    102299   10.24%  %25<0.02 %50<0.02 %75<0.05 %100<1.77   %25<3263 %50<3862 %75<3982 %100<4512          /view/?/?/?/?.js
====================
Total_bytes:1.91 GB
====================
     bytes  percent           time_distribution(s)                     bytes_distribution(B)              uri_abs
   1.23 GB   64.61%  %25<0.03 %50<0.04 %75<0.1 %100<1.96    %25<2549 %50<17296 %75<31054 %100<691666      /api/NewCom/list
 319.05 MB   16.32%  %25<0.02 %50<0.02 %75<0.05 %100<1.77   %25<3263 %50<3862 %75<3982 %100<4512          /view/?/?/?/?.js
 167.12 MB    8.55%  %25<0.15 %50<0.19 %75<0.55 %100<2.93   %25<2791 %50<3078 %75<3213 %100<11327         /api/getR/com/?/?
====================
Total_time:117048s
====================
 cum. time  percent           time_distribution(s)                     bytes_distribution(B)              uri_abs
     38747   33.10%  %25<0.01 %50<0.03 %75<0.06 %100<2.82   %25<42 %50<61 %75<63 %100<155                 /api/record/getR
     22092   18.87%  %25<0.02 %50<0.03 %75<0.06 %100<1.73   %25<34 %50<196 %75<221 %100<344               /api/getR/com/?/?/?
     17959   15.34%  %25<0.15 %50<0.19 %75<0.55 %100<2.93   %25<2791 %50<3078 %75<3213 %100<11327         /api/getRInfo/com/?/?
```
通过上例可观察指定时间内（默认当天0时至当前时间）hits/bytes/time三个维度的排名以及响应时间和响应大小的分布情况。例如，看到某个uri_abs只有比较少的hits确产生了比较大的bytes或耗费了较多的time，那么该uri_abs是否值得关注一下呢。  
**说明：**  
可通过`-f`，`-t`，`-s`参数对`起始时间`和`指定web server`进行过滤；并通过`-l`参数控制展示条数

#### distribution 子命令：
对“所有请求”或“指定uri”或“指定request_uri”在指定时间段内按“分/十分/时/天”为粒度进行聚合统计
```
# 默认按小时分组，默认显示5行
[ljk@demo ~]$ python log_show.py api distribution request "/"
====================
request_uri_abs: /
Total_hits: 76    Total_bytes: 2.11 KB
====================
      hour        hits  hits(%)       bytes  bytes(%)           time_distribution(s)                     bytes_distribution(B)
  18011911          16   21.05%    413.00 B    19.16%  %25<0.06 %50<0.06 %75<0.06 %100<0.06   %25<23 %50<26 %75<28 %100<28
  18011912          19   25.00%    518.00 B    24.03%  %25<0.02 %50<0.02 %75<0.02 %100<0.02   %25<26 %50<27 %75<28 %100<28
  18011913          23   30.26%    700.00 B    32.47%  %25<0.02 %50<0.1 %75<0.18 %100<0.18    %25<29 %50<29 %75<29 %100<29
  18011914          18   23.68%    525.00 B    24.35%  %25<0.02 %50<0.02 %75<0.02 %100<0.02   %25<28 %50<29 %75<30 %100<30
```
通过上例，可展示所有请求或特定请求在指定时间段内的分布情况，包括hits/bytes/time总量以及每个粒度内个指标相对于总量的占比。通过该子命令亦能展示各指标随时间的“趋势”。  
**说明：**  
hour字段为默认的聚合（group）粒度，18011911 表示`18年01月19日11时`  
可通过`-f`，`-t`，`-s`参数对`起始时间`和`指定server`进行过滤；通过`-g`参数指定聚合的粒度（minute/ten_min/hour/day）  
`request`子命令后可以跟具体的uri/request_uri(显示该uri/request_uri以指定粒度随时间的分布)或不跟uri(显示所有请求以指定粒度随时间的分布)

#### detail 子命令：
对某一uri进行详细分析，查看其不同参数(args)的各项指标分布。  
适用场景：比如定位到某一类型的uri_abs在某方面(hits/bytes/time)有异常，就可以通过detail子命令对该类uri_abs进行更近一步的分析，精确定位到是哪种参数（args_abs）导致的异常。
```
[ljk@demo ~]$ python log_show.py api -f 180201 detail "/recommend/update"
====================
uri_abs: /recommend/batchUpdate
Total_hits: 10069    Total_bytes: 7.62 MB
====================
    hits  hits(%)      bytes  bytes(%)  time(%)           time_distribution(s)                   bytes_distribution(B)            args_abs
    4568   45.37%    3.46 MB    45.44%   47.96%  %25<0.04 %50<0.06 %75<0.07 %100<0.47   %25<755 %50<795 %75<845 %100<1484         uid=?&category_id=?&channel=?&version=?
    4333   43.03%    3.25 MB    42.64%   42.30%  %25<0.03 %50<0.05 %75<0.07 %100<0.48   %25<752 %50<791 %75<840 %100<1447         category_id=?&channel=?&uid=?&version=?
     389    3.86%  314.15 KB     4.03%    0.88%  %25<0.02 %50<0.03 %75<0.04 %100<0.06   %25<766 %50<802 %75<850 %100<1203         category_id=?&channel=?&version=?
     352    3.50%  280.43 KB     3.60%    0.53%  %25<0.02 %50<0.03 %75<0.04 %100<0.06   %25<762 %50<804 %75<849 %100<1021         category_id=?&channel=?&uid=&version=?
     275    2.73%  216.74 KB     2.78%    1.59%  %25<0.04 %50<0.05 %75<0.07 %100<0.14   %25<763 %50<798 %75<847 %100<1098         category_id=?&uid=?&channel=?&version=?
```
通过上例可观察到"/recommend/update"这个uri所对应的不同参数各个指标的情况。另外还有一个附带的发现：开发在书写参数的时候没有完全统一规范，相同的参数组合没有按同一个顺序书写，虽不影响功能，但在精准的进行应用性能监控的时候会造成一定困扰。  
**说明：**  
`detail`子命令后跟随uri（不含参数，含参数的话将忽略参数；需为原始的uri，即经抽象处理之前的uri）  
可通过`-f`，`-t`，`-s`参数对`起始时间`和`指定server`进行过滤

### log_analyse.py部署说明：
该脚本的设计目标是将其放到web server的的计划任务里，定时（例如每30分钟或10分钟，自定义）执行，在需要时通过log_show.py进行分析即可。
`*/30 * * * * export LANG=zh_CN.UTF-8;python3 /root/log_analyse.py &> /tmp/log_analyse.log`


### Note
1. 其中`uri_abs`和`args_abs`是对uri和args进行抽象化（抽象出一个模式出来）处理之后的结果。  
 **uri**：将路径中任意一段全部由数字组成的抽象为一个"?"；将文件名出去后缀部分全部由数字组成的部分抽象为一个"?"  
 **args**：将所有的value替换成"？"  
2. `common_func.py`中还有一些其他有趣的函数
