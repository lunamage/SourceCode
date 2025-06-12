cache table tmp_article as
select if(channel_id in('1','2','5','21'),id,hash_id) id,
channel_id,
cate_level1,
cate_level2,
cate_level3,
cast(price as double) price,
mall,
article_type
from bi_dw_ga.dim_article_info
where channel_id in('1','2','5','21','6','8','11','31','66');

/*日期
流量
总曝光量
曝光用户数
3级品类曝光数
2级品类曝光数
1级品类曝光数
曝光好价文章平均价格
折后单价<20的文章曝光占比
折后单价<100的文章曝光占比
折后单价<500的文章曝光占比
折后单价<1000的文章曝光占比*/

create table bi_test.zyl_tmp_190529_5 as
select a.dt,
a.abtest,
count(*) bg,
count(distinct a.device_id) user,
count(distinct a.device_id,b.cate_level3) user_cate3,
count(distinct a.device_id,b.cate_level2) user_cate2,
count(distinct a.device_id,b.cate_level1) user_cate1,
avg(b.price) avg_price,
sum(if(b.price<20,1,0))/count(*) bl1,
sum(if(b.price<100,1,0))/count(*) bl2,
sum(if(b.price<500,1,0))/count(*) bl3,
sum(if(b.price<1000,1,0))/count(*) bl4
from(
select a.dt,a.article_id,a.abtest,a.device_id
from bi_dw_ga.fact_recsys_display_data_sdk a
where a.dt between '2019-05-30' and '2019-05-30') a
left join tmp_article b on a.article_id=b.id
group by a.dt,a.abtest;

create table bi_test.zyl_tmp_190529_6 as
select a.dt,
a.abtest,
count(*) bg,
count(distinct a.device_id) user,
count(distinct a.device_id,b.cate_level3) user_cate3,
count(distinct a.device_id,b.cate_level2) user_cate2,
count(distinct a.device_id,b.cate_level1) user_cate1,
avg(b.price) avg_price,
sum(if(b.price<20,1,0))/count(*) bl1,
sum(if(b.price<100,1,0))/count(*) bl2,
sum(if(b.price<500,1,0))/count(*) bl3,
sum(if(b.price<1000,1,0))/count(*) bl4
from(
select a.dt,regexp_extract(a.hits_eventinfo_eventlabel,'推荐_([^_]+)_([^_]+)',2) article_id,a.dim49 abtest,a.fullvisitorid device_id
from bi_dw_ga.fact_ga_hits_data a
where a.dt between '2019-05-30' and '2019-05-30' and (a.hits_appinfo_appid='com.smzdm.client.android' or a.hits_appinfo_appid='com.smzdm.client.ios')
and a.hits_eventinfo_eventcategory ='首页' and a.hits_eventinfo_eventaction='首页站内文章点击' and a.hits_eventinfo_eventlabel regexp '^推荐' and a.hits_appinfo_appversion regexp '^8.(7|8|9)|^9.') a
left join tmp_article b on a.article_id=b.id
group by a.dt,a.abtest;

create table bi_test.zyl_tmp_190529_7 as
select dt,mall,abtest,count(*) sl,count(distinct device_id) sl2,count(distinct article_id) sl3
from bi_dw_ga.fact_recsys_display_data_sdk a
where a.dt between '2019-05-30' and '2019-05-30'
group by dt,mall,abtest;


insert overwrite local directory '/data/tmp/zhaoyulong/data' row format delimited fields terminated by '\t'
select * from bi_test.zyl_tmp_190529_5 order by dt,abtest;

insert overwrite local directory '/data/tmp/zhaoyulong/data' row format delimited fields terminated by '\t'
select * from bi_test.zyl_tmp_190529_6 order by dt,abtest;

insert overwrite local directory '/data/tmp/zhaoyulong/data' row format delimited fields terminated by '\t'
select * from bi_test.zyl_tmp_190529_7 order by dt,abtest


create table bi_test.zyl_tmp_190530_1 as
select a.dt,
a.stat_hour,
a.abtest,
count(*) bg,
count(distinct a.device_id) user,
count(distinct a.device_id,b.cate_level3) user_cate3,
count(distinct a.device_id,b.cate_level2) user_cate2,
count(distinct a.device_id,b.cate_level1) user_cate1,
avg(b.price) avg_price,
sum(if(b.price<20,1,0))/count(*) bl1,
sum(if(b.price<100,1,0))/count(*) bl2,
sum(if(b.price<500,1,0))/count(*) bl3,
sum(if(b.price<1000,1,0))/count(*) bl4
from(
select a.dt,a.article_id,a.abtest,a.device_id,a.stat_hour
from bi_dw_ga.fact_recsys_display_data_sdk a
where a.dt between '2019-05-27' and '2019-05-29') a
left join tmp_article b on a.article_id=b.id
group by a.dt,a.stat_hour,a.abtest;


insert overwrite local directory '/data/tmp/zhaoyulong/data' row format delimited fields terminated by '\t'
select * from bi_test.zyl_tmp_190530_1 order by dt,abtest;


/*字段名	解释
日期
流量
总会话次数
总曝光数
会话深度	1/2/3/4/5/6/7/8/9… 120，120以上
三级品类区间	1-5，6-10… 以5为间隔一直66-70，70以上，一共分15个区间
会话次数	*/

create table bi_test.zyl_tmp_190529_8 as
select a.dt,b.tv,count(*) sl,count(distinct a.sid) sl2
from bi_ods_ga.ods_app_sdk_log a
lateral view json_tuple(a.ecp,'tv','ctp','sp','a','c') b as tv,ctp,sp,a,c
where a.dt between '2019-05-20' and '2019-05-28' and a.ec='01' and a.ea='01' and b.sp='0'
group by a.dt,b.tv;




create table bi_test.zyl_tmp_190529_9 as
select dt,tv,
case when sl between 1 and 10 then '1-10'
when sl between 11 and 20 then '11-20'
when sl between 21 and 30 then '21-30'
when sl between 31 and 40 then '31-40'
when sl between 41 and 50 then '41-50'
when sl between 51 and 60 then '51-60'
when sl between 61 and 70 then '61-70'
when sl between 71 and 80 then '71-80'
when sl between 81 and 90 then '81-90'
when sl between 91 and 100 then '91-100'
when sl between 101 and 110 then '101-110'
when sl between 111 and 120 then '111-120' else '120+' end t1,
case when sl2 between 1 and 5 then '1-5'
when sl2 between 11 and 15 then '11-15'
when sl2 between 21 and 25 then '21-25'
when sl2 between 31 and 35 then '31-35'
when sl2 between 41 and 45 then '41-45'
when sl2 between 51 and 55 then '51-55'
when sl2 between 61 and 65 then '61-65'
when sl2 between 6 and 10 then '6-10'
when sl2 between 16 and 20 then '16-20'
when sl2 between 26 and 30 then '26-30'
when sl2 between 36 and 40 then '36-40'
when sl2 between 46 and 50 then '46-50'
when sl2 between 56 and 60 then '56-60'
when sl2 between 66 and 70 then '66-70' else '70+' end t2,
count(*) c
from(
select a.dt,a.tv,a.sid,count(*) sl,count(distinct b.cate_level3) sl2
from(
select a.dt,b.tv,a.sid,b.a id
from bi_ods_ga.ods_app_sdk_log a
lateral view json_tuple(a.ecp,'tv','ctp','sp','a','c') b as tv,ctp,sp,a,c
where a.dt between '2019-05-20' and '2019-05-28' and a.ec='01' and a.ea='01' and b.sp='0') a
inner join tmp_article b on a.id=b.id
group by a.dt,a.tv,a.sid) a
group by dt,tv,
case when sl between 1 and 10 then '1-10'
when sl between 11 and 20 then '11-20'
when sl between 21 and 30 then '21-30'
when sl between 31 and 40 then '31-40'
when sl between 41 and 50 then '41-50'
when sl between 51 and 60 then '51-60'
when sl between 61 and 70 then '61-70'
when sl between 71 and 80 then '71-80'
when sl between 81 and 90 then '81-90'
when sl between 91 and 100 then '91-100'
when sl between 101 and 110 then '101-110'
when sl between 111 and 120 then '111-120' else '120+' end,
case when sl2 between 1 and 5 then '1-5'
when sl2 between 11 and 15 then '11-15'
when sl2 between 21 and 25 then '21-25'
when sl2 between 31 and 35 then '31-35'
when sl2 between 41 and 45 then '41-45'
when sl2 between 51 and 55 then '51-55'
when sl2 between 61 and 65 then '61-65'
when sl2 between 6 and 10 then '6-10'
when sl2 between 16 and 20 then '16-20'
when sl2 between 26 and 30 then '26-30'
when sl2 between 36 and 40 then '36-40'
when sl2 between 46 and 50 then '46-50'
when sl2 between 56 and 60 then '56-60'
when sl2 between 66 and 70 then '66-70' else '70+' end;

insert overwrite local directory '/data/tmp/zhaoyulong/data' row format delimited fields terminated by '\t'
select a.dt,a.tv,a.sl,a.sl2,b.t1,b.t2,b.c
from bi_test.zyl_tmp_190529_8 a left join bi_test.zyl_tmp_190529_9 b on a.dt=b.dt and a.tv=b.tv;

/*字段名	解释
日期
流量
总会话次数
总曝光数
会话深度	1/2/3/4/5/6/7/8/9… 200，200以上
平均价格梯度	只统计好价内容，0-20,21-40以20为间隔一直到500,500以上，一共分26个区间
会话次数	*/

create table bi_test.zyl_tmp_190529_10 as
select dt,tv,
case when sl between 1 and 10 then '1-10'
when sl between 11 and 20 then '11-20'
when sl between 21 and 30 then '21-30'
when sl between 31 and 40 then '31-40'
when sl between 41 and 50 then '41-50'
when sl between 51 and 60 then '51-60'
when sl between 61 and 70 then '61-70'
when sl between 71 and 80 then '71-80'
when sl between 81 and 90 then '81-90'
when sl between 91 and 100 then '91-100'
when sl between 101 and 110 then '101-110'
when sl between 111 and 120 then '111-120' else '120+' end t1,
case when sl2 between 0 and 20 then '0-20'
when sl2 between 21 and 40 then '21-40'
when sl2 between 41 and 60 then '41-60'
when sl2 between 61 and 80 then '61-80'
when sl2 between 81 and 100 then '81-100'
when sl2 between 101 and 120 then '101-120'
when sl2 between 121 and 140 then '121-140'
when sl2 between 141 and 160 then '141-160'
when sl2 between 161 and 180 then '161-180'
when sl2 between 181 and 200 then '181-200'
when sl2 between 201 and 220 then '201-220'
when sl2 between 221 and 240 then '221-240'
when sl2 between 241 and 260 then '241-260'
when sl2 between 261 and 280 then '261-280'
when sl2 between 281 and 300 then '281-300'
when sl2 between 301 and 320 then '301-320'
when sl2 between 321 and 340 then '321-340'
when sl2 between 341 and 360 then '341-360'
when sl2 between 361 and 380 then '361-380'
when sl2 between 381 and 400 then '381-400'
when sl2 between 401 and 420 then '401-420'
when sl2 between 421 and 440 then '421-440'
when sl2 between 441 and 460 then '441-460'
when sl2 between 461 and 480 then '461-480'
when sl2 between 481 and 500 then '481-500' else '500+' end t2,
count(*) c
from(
select a.dt,a.tv,a.sid,count(*) sl,avg(b.price) sl2
from(
select a.dt,b.tv,a.sid,b.a id
from bi_ods_ga.ods_app_sdk_log a
lateral view json_tuple(a.ecp,'tv','ctp','sp','a','c') b as tv,ctp,sp,a,c
where a.dt between '2019-05-20' and '2019-05-28' and a.ec='01' and a.ea='01' and b.sp='0') a
inner join (select * from tmp_article where channel_id in(1,2,5) and article_type='优惠单品') b on a.id=b.id
group by a.dt,a.tv,a.sid) a
group by dt,tv,
case when sl between 1 and 10 then '1-10'
when sl between 11 and 20 then '11-20'
when sl between 21 and 30 then '21-30'
when sl between 31 and 40 then '31-40'
when sl between 41 and 50 then '41-50'
when sl between 51 and 60 then '51-60'
when sl between 61 and 70 then '61-70'
when sl between 71 and 80 then '71-80'
when sl between 81 and 90 then '81-90'
when sl between 91 and 100 then '91-100'
when sl between 101 and 110 then '101-110'
when sl between 111 and 120 then '111-120' else '120+' end,
case when sl2 between 0 and 20 then '0-20'
when sl2 between 21 and 40 then '21-40'
when sl2 between 41 and 60 then '41-60'
when sl2 between 61 and 80 then '61-80'
when sl2 between 81 and 100 then '81-100'
when sl2 between 101 and 120 then '101-120'
when sl2 between 121 and 140 then '121-140'
when sl2 between 141 and 160 then '141-160'
when sl2 between 161 and 180 then '161-180'
when sl2 between 181 and 200 then '181-200'
when sl2 between 201 and 220 then '201-220'
when sl2 between 221 and 240 then '221-240'
when sl2 between 241 and 260 then '241-260'
when sl2 between 261 and 280 then '261-280'
when sl2 between 281 and 300 then '281-300'
when sl2 between 301 and 320 then '301-320'
when sl2 between 321 and 340 then '321-340'
when sl2 between 341 and 360 then '341-360'
when sl2 between 361 and 380 then '361-380'
when sl2 between 381 and 400 then '381-400'
when sl2 between 401 and 420 then '401-420'
when sl2 between 421 and 440 then '421-440'
when sl2 between 441 and 460 then '441-460'
when sl2 between 461 and 480 then '461-480'
when sl2 between 481 and 500 then '481-500' else '500+' end;

insert overwrite local directory '/data/tmp/zhaoyulong/data' row format delimited fields terminated by '\t'
select a.dt,a.tv,a.sl,a.sl2,b.t1,b.t2,b.c
from bi_test.zyl_tmp_190529_8 a left join bi_test.zyl_tmp_190529_10 b on a.dt=b.dt and a.tv=b.tv;


/*字段名	解释
日期
流量
总会话次数
总曝光数
会话深度	1/2/3/4/5/6/7/8/9… 200，200以上
商城名称	"淘系/京东系/其他商城
""jd"":""183,3949,4579,5005,5123,5199,8547,9019"",
""tm"":""247,2537,2897,5417,6753,8116,2971,241,4005"""
商城曝光占比	*/

create table bi_test.zyl_tmp_190529_11 as
select a.dt,a.tv,
case when b.sl between 1 and 10 then '1-10'
when b.sl between 11 and 20 then '11-20'
when b.sl between 21 and 30 then '21-30'
when b.sl between 31 and 40 then '31-40'
when b.sl between 41 and 50 then '41-50'
when b.sl between 51 and 60 then '51-60'
when b.sl between 61 and 70 then '61-70'
when b.sl between 71 and 80 then '71-80'
when b.sl between 81 and 90 then '81-90'
when b.sl between 91 and 100 then '91-100'
when b.sl between 101 and 110 then '101-110'
when b.sl between 111 and 120 then '111-120' else '120+' end t1,
a.mall,
sum(a.sl2) c
from(
select a.dt,a.tv,a.sid,
case when b.mall in('聚划算','天猫精选','天猫超市','天猫国际','飞猪','95095医药','天猫电器城','天猫国际官方直营','淘宝心选') then '淘宝系'
when b.mall in('京东','京东全球购','京东全球购eBay精选','京东金融','京东到家','京东众筹','京东酒店','京东国际') then '京东系' else '其他商城' end mall,
count(*) sl2
from(
select a.dt,b.tv,a.sid,b.a id
from bi_ods_ga.ods_app_sdk_log a
lateral view json_tuple(a.ecp,'tv','ctp','sp','a','c') b as tv,ctp,sp,a,c
where a.dt between '2019-05-20' and '2019-05-28' and a.ec='01' and a.ea='01' and b.sp='0') a
inner join tmp_article  b on a.id=b.id
group by a.dt,a.tv,a.sid,case when b.mall in('聚划算','天猫精选','天猫超市','天猫国际','飞猪','95095医药','天猫电器城','天猫国际官方直营','淘宝心选') then '淘宝系'
when b.mall in('京东','京东全球购','京东全球购eBay精选','京东金融','京东到家','京东众筹','京东酒店','京东国际') then '京东系' else '其他商城' end) a
left join
(select a.dt,a.tv,a.sid,count(*) sl
from(
select a.dt,b.tv,a.sid,b.a id
from bi_ods_ga.ods_app_sdk_log a
lateral view json_tuple(a.ecp,'tv','ctp','sp','a','c') b as tv,ctp,sp,a,c
where a.dt between '2019-05-20' and '2019-05-28' and a.ec='01' and a.ea='01' and b.sp='0') a
inner join tmp_article  b on a.id=b.id
group by a.dt,a.tv,a.sid) b on a.dt=b.dt and a.tv=b.tv and a.sid=b.sid
group by a.dt,a.tv,case when b.sl between 1 and 10 then '1-10'
when b.sl between 11 and 20 then '11-20'
when b.sl between 21 and 30 then '21-30'
when b.sl between 31 and 40 then '31-40'
when b.sl between 41 and 50 then '41-50'
when b.sl between 51 and 60 then '51-60'
when b.sl between 61 and 70 then '61-70'
when b.sl between 71 and 80 then '71-80'
when b.sl between 81 and 90 then '81-90'
when b.sl between 91 and 100 then '91-100'
when b.sl between 101 and 110 then '101-110'
when b.sl between 111 and 120 then '111-120' else '120+' end,
a.mall;

create table bi_test.zyl_tmp_190529_12 as
select dt,tv,
case when sl between 1 and 10 then '1-10'
when sl between 11 and 20 then '11-20'
when sl between 21 and 30 then '21-30'
when sl between 31 and 40 then '31-40'
when sl between 41 and 50 then '41-50'
when sl between 51 and 60 then '51-60'
when sl between 61 and 70 then '61-70'
when sl between 71 and 80 then '71-80'
when sl between 81 and 90 then '81-90'
when sl between 91 and 100 then '91-100'
when sl between 101 and 110 then '101-110'
when sl between 111 and 120 then '111-120' else '120+' end t1,
sum(sl2) sl
from(
select dt,tv,sl,sl sl2
from (
select a.dt,a.tv,a.sid,count(*) sl
from(
select a.dt,b.tv,a.sid,b.a id
from bi_ods_ga.ods_app_sdk_log a
lateral view json_tuple(a.ecp,'tv','ctp','sp','a','c') b as tv,ctp,sp,a,c
where a.dt between '2019-05-20' and '2019-05-28' and a.ec='01' and a.ea='01' and b.sp='0') a
inner join tmp_article  b on a.id=b.id
group by a.dt,a.tv,a.sid) a
group by dt,tv,sl) a
group by dt,tv,case when sl between 1 and 10 then '1-10'
when sl between 11 and 20 then '11-20'
when sl between 21 and 30 then '21-30'
when sl between 31 and 40 then '31-40'
when sl between 41 and 50 then '41-50'
when sl between 51 and 60 then '51-60'
when sl between 61 and 70 then '61-70'
when sl between 71 and 80 then '71-80'
when sl between 81 and 90 then '81-90'
when sl between 91 and 100 then '91-100'
when sl between 101 and 110 then '101-110'
when sl between 111 and 120 then '111-120' else '120+' end;

insert overwrite local directory '/data/tmp/zhaoyulong/data' row format delimited fields terminated by '\t'
select a.dt,a.tv,a.sl,a.sl2,b.t1,b.t2,b.c
from bi_test.zyl_tmp_190529_8 a
left join
(select a.dt,a.tv,a.t1,a.mall t2,a.c/b.c c
from bi_test.zyl_tmp_190529_11 a
left join (select dt,tv,t1,sum(c) c from bi_test.zyl_tmp_190529_11 group by dt,tv,t1) b on a.dt=b.dt and a.tv=b.tv and a.t1=b.t1) b on a.dt=b.dt and a.tv=b.tv;








select urhoclick_brand_24h,urhoclick_brand_15m,urhoclick_brand_150s,creation_date,brandid
from home_exposure_summary_playback_v4
where dt='2019-05-26' limit 100;

select sum(if(urhoclick_brand_24h is not null and urhoclick_brand_24h<>0,1,0)) sl1,
sum(if(urhoclick_brand_15m is not null and urhoclick_brand_15m<>0,1,0)) sl2,
sum(if(urhoclick_brand_150s is not null and urhoclick_brand_150s<>0,1,0)) sl3
from home_exposure_summary_playback_v4
where dt='2019-05-26';
