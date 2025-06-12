--反作弊 首页接口调用数量

/*47.92.65.248    0.00425255448585435
116.7.66.199    0.0010882469331222793
111.10.132.83   9.98302885095338E-4
125.118.48.129  7.97531121051354E-4

2019-05-26
47.92.65.248    调用首页接口238次
2019-05-27
47.92.65.248   调用首页接口350次
111.10.135.141 调用首页接口58次，从中午开始每小时都有调用，疑似爬虫

2019-05-28
47.92.65.248    调用首页接口360次
2019-05-29
47.92.65.248    调用首页接口414次
47.92.65.248 */

cache table tmp as
select unix_timestamp(concat('2019-05-30 ',regexp_extract(time_local,'2019:([^ ]+)',1))) dt,coalesce(http_x_forwarded_for,client_real_ip,device_id) did
from bi_log.access_log_extract
where dt='2019-05-30' and http_user_agent not regexp 'smzdmapp' and http_user_agent not regexp '(?i)Android|Apple' and uri regexp 'homepage' and device_id is null;


select did,count(*) sl
from tmp
group by did
order by sl desc limit 30;

select from_unixtime(dt,'yyyy-MM-dd HH:mm:ss') a from tmp where did='111.10.135.141' order by a;
