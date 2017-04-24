# web_log_analyse
This tool is not a generally said log analyse/statistics solution. It aim at trouble shooting and performance optimization based on web logs


日志分析在web系统中故障排查、性能分析方面有着非常重要的作用。目前，开源的ELK系统是成熟且功能强大的选择。但是部署及学习成本亦然不低，这里我实现了一个方法上相对简单（但准确度和效率是有保证的）的实现。另外该脚本的侧重点不是通常的PV，UV等展示，而是短期内（如两三天历史）提供细粒度的异常定位和性能分析。

### 先说一下我想实现这个功能的驱动力（痛点）吧：
我们有不少站点，前边有CDN，原站前面是F5，走到源站的访问总量日均PV约5000w。下面是我们经常面临一些问题：

 - CDN回源异常，可能导致我们源站带宽和负载都面临较大的压力。这时需要能快速的定位到是多了哪些回源IP（即CDN节点）或是某个IP的回源量异常，又或是哪些url的回源量异常
 - 在排除了CDN回源问题之后，根据zabbix监控对一些异常的流量或者负载波动按异常时段对比正常时段进行分析，定位到具体的某（几）类url。反馈给开发进行review以及优化
 - 有时zabbix会监控到应用服务器和DB或者缓存服务器之间的流量异常，这种问题一般定位起来是比较麻烦的，甚至波动仅仅是在一两分钟内，这就需要对日志有一个非常精细的分析粒度
 - 我们希望能所有的应用服务器能过在本机分析日志（分布式的思想），然后将分析结果汇总到一起（**MySQL**）以便查看；并且还希望能尽可能的**实时**（将定时任务间隔设置低一些），以便发现问题后能尽快的通过此平台进行分析  
 -  **通用**和**性能**：对于不同的日志格式只需对脚本稍加改动即可分析；因为将日志分析放在应用服务器本机，所以脚本的性能和效率也要有保证，不能影响业务

 
### 实现思路：
比较简单，就是利用python的re模块通过正则表达式对日志进行分析处理，取得`uri`、`args`、`时间当前`、`状态码`、`响应大小`、`响应时间`、`用户IP`、`CDN ip`、`server name` 等信息存储进MySQL数据库。

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

#### 对于其他格式的nginx日志或者Apache日志，按照如上原则，并对数据库结构做相应调整，都可以轻松的使用该脚本分析处理。

原理虽简单但实现起来却发现有好多坑，如果想靠空格或双引号来分割各段的话，主要问题是面对各种不规范的记录时(原因不一而足，而且也是样式繁多)，无法做到将各种异常都考虑在内，所以我采用了`re`模块而不是简单的`split()`函数的原因。代码里对一些“可以容忍”的异常记录通过一些判断逻辑予以处理；对于“无法容忍”的异常记录则返回空字符串并将日志记录于文件。

其实对于上述的这些不规范的请求，最好的办法是在nginx中定义日志格式时，用一个特殊字符作为分隔符，例如“|”。这样都不用Python的re模块，直接字符串分割就能正确的获取到各段(性能会好些)。

### 接下来看看使用效果：
先看一行数据库里的记录
```
*************************** 9. row ***************************
            id: 9
        server: web6
       uri_abs: /chapter/?/?.json
 uri_abs_crc32: 443227294
      args_abs: channel=?&version=?
args_abs_crc32: 2972340533
    time_local: 2017-02-22 23:59:01
 response_code: 200
    bytes_sent: 218
  request_time: 0.028
       user_ip: 210.78.141.185
        cdn_ip: 27.221.112.163
request_method: GET
           uri: /chapter/14278/28275.json
          args: channel=ios&version=2.0.6
       referer:
```
其中`uri_abs`和`args_abs`是对uri和args进行抽象化（抽象出一个模式出来）处理之后的结果。  
 对uri：将其中所有的数字替换成"?"  
 对args：将所有的value替换成"？"  
`uri_abs_crc32`和`args_abs_crc32`两列是对抽象化结果进行crc32计算，这两列单纯只是为了在MySQL中对uri或args进行分类统计汇总时得到更好的性能。
  
现在还没有完成统一分析的入口脚本，所以还是以sql语句的形式来查询（对用户的sql功底有要求，不友好待改善）

#### 查询示例

 - 查询某站点日/小时pv（其实这一套东西的关注点并不在类似的基础的统计上）
```
select count(*) from www where time_local>='2016-12-09 00:00:00' and time_local<='2016-12-09 23:59:59'
```
 - 查询某类型url总量(or指定时间段内该url总量)
依据表中的url_abs_crc32字段
```
mysql> select count(*) from www where uri_abs_crc32=2043925204 and time_local > '2016-11-23 10:00:00' and time_local <'2016-11-23 23:59:59';
```
 - 平均响应时间排行（可基于总量分析；亦可根据时段对比分析）
```
mysql> select uri_abs,count(*) as num,sum(request_time) as total_time,sum(request_time)/count(*) as average_time from www group by uri_abs_crc32 order by num desc limit 5;
+------------------------------------------+---------+------------+--------------+
| uri_abs                                  | num     | total_time | average_time |
+------------------------------------------+---------+------------+--------------+
| /comicsum/comicshot.php                  | 2700716 |   1348.941 |    0.0004995 |
| /category/?.html                         |  284788 | 244809.877 |    0.8596215 |
| /                                        |   72429 |   1172.113 |    0.0161829 |
| /static/hits/?.json                      |   27451 |      7.658 |    0.0002790 |
| /dynamic/o_search/searchKeyword          |   26230 |   3757.661 |    0.1432581 |
+------------------------------------------+---------+------------+--------------+
10 rows in set (40.09 sec)
```
- 平均响应大小排行
```
mysql> select uri_abs,count(*) as num,sum(bytes_sent) as total_bytes,sum(bytes_sent)/count(*) as average_bytes from www group by uri_abs_crc32 order by num desc,average_bytes desc limit 10;    
+------------------------------------------+---------+-------------+---------------+
| uri_abs                                  | num     | total_bytes | average_bytes |
+------------------------------------------+---------+-------------+---------------+
| /comicsum/comicshot.php                  | 2700716 |    72889752 |       26.9890 |
| /category/?.html                         |  284788 |  3232744794 |    11351.4080 |
| /                                        |   72429 |  1904692759 |    26297.3776 |
| /static/hits/?.json                      |   27451 |     5160560 |      187.9917 |
| /dynamic/o_search/searchKeyword          |   26230 |     3639846 |      138.7665 |
+------------------------------------------+---------+-------------+---------------+
```
以上只列举了几个例子，基本上除了UA部分（代码中已有捕捉，但是笔者用不到），其他的信息都以包含到表中。因此几乎可以对网站`流量`，`负载`,`响应时间`等方面的任何疑问给出数据上的支持。

### 注意事项：
Python外部包依赖：pymysql  
MySQL（笔者5.6版本）将`innodb_file_format`设置为`Barracuda`（这个设置并不对其他库表产生影响，即使生产数据库设置也无妨）,以便在建表语句中可以通过`ROW_FORMAT=COMPRESSED`将innodb表这只为压缩模式，笔者实验开启压缩模式后，数据文件大小将近减小50%。

### 使用说明：
该脚本的设计目标是将其放到web server的的计划任务里，定时（例如每10分钟/30分钟，自定义）执行，在需要时通过MySQL进行需要的分析即可。
`*/30 * * * * export LANG=zh_CN.UTF-8;python3 /root/log_analyse_parall.py &> /tmp/log_analyse.log`

### 性能测试：
现在的版本，进过分析的数据最终存储到MySQL中，所以脚本执行过程中向MySQL的插入语句耗费了绝大多数执行时间，在笔者的4核4G的虚拟机上，以20w的真实日志数据进行单进程测试，结果如下
```
有入库    22.40s
无入库    4.56s
```
差别非常大，这也是笔者下一步打算更换MySQL存储的驱动力（例如MongoDB）
