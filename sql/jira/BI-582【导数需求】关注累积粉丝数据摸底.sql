CREATE TABLE `bi_test.zyl_tmp_190708`(
  `smzdm_id` string,
  `gz_pv` int,
  `gz_click` int,
  `gz_event` int)
PARTITIONED BY ( `dt` string)
LOCATION 'hdfs://cluster/bi/test/zyl_tmp_190708';

insert overwrite table bi_test.zyl_tmp_190708 partition(dt)
select dim16 smzdm_id,
sum(if(hits_appinfo_screenname regexp '首页/关注' and hits_type='APPVIEW',1,0)) gz_pv,
sum(if(hits_eventinfo_eventcategory='关注' and hits_eventinfo_eventaction='首页关注_站内文章点击',1,0)) gz_click,
sum(if(hits_eventinfo_eventaction like '%添加到购物车%' and dim69 regexp 'G1',1,0)) gz_event,
dt
from bi_dw_ga.fact_ga_hits_data
where dt BETWEEN '2019-03-01' AND '2019-07-07'
and (hits_appinfo_appid='com.smzdm.client.android' or hits_appinfo_appid='com.smzdm.client.ios')
and length(dim16)=10
group by dt,dim16;


--
create table bi_test.zyl_tmp_190710_1 as
select a.user_id,b.smzdm_id,b.nickname,b.creation_date,a.add_time,a.sl
from(
select user_id,count(*) sl,max(add_time) add_time
from(
select user_id,add_time from stg.db_dingyue_dingyue_baike where dt='2019-07-07'
union all
select user_id,creation_time add_time from stg.db_dingyue_rule_base_user where dt='2019-07-07'
union all
select user_id,creation_date add_time  from stg.db_user_follow_relate where dt='2019-07-07') a
group by user_id) a
inner join (select * from stg.db_udc_account where dt='2019-07-07') b on a.user_id=b.user_id;


create table bi_test.zyl_tmp_190710_2 as
select smzdm_id,max(dt) dt,
count(distinct if(gz_pv>0,dt,null)) a1,
max(if(gz_pv>0,dt,null)) a2,
sum(gz_pv) a3,
max(if(gz_click>0,dt,null)) b2,
sum(gz_click) b3,
max(if(gz_event>0,dt,null)) c2,
sum(gz_event) c3
from bi_test.zyl_tmp_190708
group by smzdm_id;

insert overwrite local directory '/data/tmp/zhaoyulong/data' row format delimited fields terminated by '\t'
select a.user_id,a.smzdm_id,a.nickname,a.creation_date,b.dt,a.add_time,a.sl,
b.a1,b.a2,b.a3,b.b2,b.b3,b.c2,b.c3
from bi_test.zyl_tmp_190710_1 a
left join bi_test.zyl_tmp_190710_2 b on a.smzdm_id=b.smzdm_id
order by user_id;

--用户id|用户长id|昵称|注册时间|用户最后访问APP的时间|用户最近一次有加关注行为的时间|有效关注数|最近1年内访问关注流的天数|最近一次访问关注流的时间|最近1年内关注流访问次数|最近一次在关注流产生文章点击的时间|最近1年内用户在关注流总文章点击次数|最近一次在关注流产生电商点击的时间|最近1年内用户关注流总电商点击次数
