/*
需求背景：

实时特征上线后效果未达到预期，需要导出分流量实时特征生效的请求记录，做进一步分析。



需求说明：

时间范围：0422-0506

用户范围：分流为q流和Z7流

导出字段：日期   流量   urhoimp_cate1_5m有值/缺失   用户数   曝光事件数    详情点击数    电商点击数

 注：用户数根据did去重 */

 drop table bi_test.zyl_tmp_190509_2;
 drop table bi_test.zyl_tmp_190509_3;
 drop table bi_test.zyl_tmp_190509_4;

 --点击数据
 create table bi_test.zyl_tmp_190509_2 as
 select distinct dt,
 if(hits_eventinfo_eventaction='添加到购物车','event','click') type,
 dim16,
 if(hits_eventinfo_eventaction='添加到购物车',hits_product_productsku,regexp_extract(hits_eventinfo_eventlabel,'_([^_]+)_([^_]+)',2)) article_id
 from bi_dw_ga.fact_ga_hits_data
 where dt between '2019-04-22' and '2019-05-06' and hits_appinfo_appversion regexp '^8.[7-9]|^9.' and dim16<>'无' and dim16<>''
 and (hits_eventinfo_eventaction='首页站内文章点击' and hits_eventinfo_eventlabel regexp '^推荐' and dim95<>'是'
 or hits_ecommerceaction_action_type='3' and hits_eventinfo_eventaction='添加到购物车' and dim64 regexp '首页_推荐_feed流');

create table bi_test.zyl_tmp_190509_3 as
select a.dt,
a.ab_test,
a.user_id,
a.article_id,
if(b.urhoimp_cate1_5m is null or b.urhoimp_cate1_5m='','缺失','有值') bz,
a.device_id
from bi_test.feature_log_extract a
lateral view json_tuple(a.featuremap,'urhoimp_cate1_5m') b as urhoimp_cate1_5m
where a.dt between '2019-04-22' and '2019-05-06';


create table bi_test.zyl_tmp_190509_4 as
select a.dt,
a.ab_test,
a.bz,
a.device_id,
if(b.dim16 is null,0,1) is_click,
if(c.dim16 is null,0,1) is_event
from bi_test.zyl_tmp_190509_3 a
left join (select dt,dim16,article_id from bi_test.zyl_tmp_190509_2 where type='click') b on a.dt=b.dt and a.user_id=b.dim16 and a.article_id=b.article_id
left join (select dt,dim16,article_id from bi_test.zyl_tmp_190509_2 where type='event') c on a.dt=b.dt and a.user_id=c.dim16 and a.article_id=c.article_id;


insert overwrite local directory '/data/tmp/zhaoyulong/data' row format delimited fields terminated by '\t'
select dt,ab_test,bz,
count(distinct device_id) uv,
count(*) sl,
sum(is_click) is_click,
sum(is_event) is_event
from bi_test.zyl_tmp_190509_4
group by dt,ab_test,bz;
