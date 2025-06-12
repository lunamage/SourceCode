--1、导出coupon表中，createtime晚于等于20190619的文章ID（coupon表中的ID字段）
create table bi_test.zyl_tmp_190712_1 as
select id
from stg.db_exchange_coupon
where dt='2019-07-11' and create_time >='2019-06-19';

--2、导出券频道同步到好价频道的文章
--统计周期：coupon表中，create_time为20190619-20190709的券文章对应的数据
--导出字段：券ID(coupon表中取id）,对应好价文章ID（根据券ID，从coupon_to_youhui_log日志表中取对应的youhui_id）、youhui发布日期、优惠所属频道（优惠、海淘、发现）
create table bi_test.zyl_tmp_190712_2 as
select b.coupon_id,b.youhui_id,c.pubdate,case when channel_id='1' then '优惠' when channel_id='2' then '发现' else '海淘' end channel
from(select id
from stg.db_exchange_coupon
where dt='2019-07-11' and create_time between '2019-06-19' and '2019-07-09 23:59:59') a
inner join stg.db_exchange_coupon_to_youhui_log b on a.id=b.coupon_id
inner join (select * from bi_dw_ga.dim_article_info where channel_id in(1,2,5)) c on b.youhui_id=c.id;

--3、coupon表中的文章在关注频道的曝光、点击（曝光和点击均用SDK记录的数据）
--统计周期：20190619-20190709
--统计数据范围：1中导出的id
--导出字段：日期、文章ID、曝光数（口径如下）、点击数（口径如下）
/*曝光数据规则

t='show' 且 ec='02' 且 ea in ('01','02')

取ecp中的a做为文章id用文章id

取av作为版本

点击数据规则

t='event' 且 ec=关注

ea=首页关注_点击

用el字段作为文章id

 用av作为版本

用取到的文章id去匹配业务库的好价文章id和优惠券id */
cache table tmp as
select a.dt,
if(a.ec='02','baoguang','click') type,
if(a.ec='02',b.a,a.el) id,
count(*) sl
from bi_ods_ga.ods_app_sdk_log a
lateral view json_tuple(a.ecp,'tv','ctp','sp','a','c') b as tv,ctp,sp,a,c
where a.dt between '2019-06-19' and '2019-07-09'
and (a.ec='02' and a.ea in('01','02') or a.ec='关注' and a.ea='首页关注_点击')
group by a.dt,if(a.ec='02','baoguang','click'),if(a.ec='02',b.a,a.el);


create table bi_test.zyl_tmp_190712_3 as
select b.dt,b.id,
max(if(type='baoguang',sl,0)) sl1,
max(if(type='click',sl,0)) sl2
from bi_test.zyl_tmp_190712_1 a
inner join tmp b on a.id=b.id
group by b.dt,b.id;

--4、youhui表中的文章在关注频道的曝光、点击（曝光和点击均用SDK记录的数据）
--统计周期：20190619-20190709
--统计数据范围：1中导出的id
--导出字段：日期、文章ID、曝光数（口径如上）、点击数（口径如上）
create table bi_test.zyl_tmp_190712_4 as
select b.dt,b.id,
max(if(type='baoguang',sl,0)) sl1,
max(if(type='click',sl,0)) sl2
from bi_test.zyl_tmp_190712_2 a
inner join tmp b on a.youhui_id=b.id
group by b.dt,b.id;

--5、券文章的PV
--统计周期：coupon表中，create_time为20190619-20190709范围内的券文章，在20190619-20190709的详情页PV
--包含字段：日期（PV发生的日期）、券文章ID（coupon表中的ID）、PV（因为GA中的PV条数上百万，如果GA直接导出，每次只能导出5000条，所以提给BI同学导数）
--PV口径： useragent 正则 smzdmapp    url 正则  coupon_id=
create table bi_test.zyl_tmp_190712_5 as
select a.dt,regexp_extract(a.hits_page_pagepath,'coupon_id=([0-9]+)',1) id,count(*) pv
from bi_dw_ga.fact_ga_hits_data a
inner join bi_test.zyl_tmp_190712_1 b on regexp_extract(a.hits_page_pagepath,'coupon_id=([0-9]+)',1)=b.id
where a.dt between '2019-06-19' and '2019-07-09' and a.dim8 regexp 'smzdmapp' and a.hits_page_pagepath regexp 'coupon_id='
group by a.dt,regexp_extract(a.hits_page_pagepath,'coupon_id=([0-9]+)',1);

--6、插入了券频道券的好价文章
--统计周期：发布时间为20190619-20190709范围内的好价文章
--统计范围：以下表中，type=2的数据
--包含字段：好价文章ID（以下表中的article_id）、插入的券频道券ID(以下表中的relate_article_id)
create table bi_test.zyl_tmp_190712_6 as
select a.article_id,a.relate_article_id
from stg.db_youhui_youhui_coupon_item a
inner join(select id from bi_dw_ga.dim_article_info where channel_id in(1,2,5) and pubdate between '2019-06-19' and '2019-07-09 23:59:59') b on a.article_id=b.id
where a.type='2';

--7、 6中的好价文章ID，在新老版本APP的PV和电商点击数
--新版本：V9.5.0以及以后的版本
--老版本：V9.5.0之前的所有版本
--统计周期：20190619-20190709
--好价详情PV：文章ID是第4部分中导出的优惠文章   V201下
--好价详情电商点击：文章ID是第4部分中导出的优惠文章 V201下
--新版本导出字段：日期（PV或者电商点击日期）、优惠文章ID、PV、电商点击
--老版本导出字段：日期（PV或者电商点击日期）、优惠文章ID、PV、电商点击

cache table tmp2 as
select dt,
if(hits_type='APPVIEW','pv','event') type,
if(hits_type='APPVIEW',article_id,hits_product_productsku) id,
if(hits_appinfo_appversion regexp '^9.5','new','old') v,
count(*) sl
from bi_dw_ga.fact_ga_hits_data
where dt between '2019-06-19' and '2019-07-09' and hits_appinfo_appid in('com.smzdm.client.android','com.smzdm.client.ios')
and (hits_type='APPVIEW' and hits_appinfo_screenname regexp '好价' and article_id<>''
or hits_ecommerceaction_action_type=3 and hits_eventinfo_eventaction like '%添加到购物车%' and dim9 in('youhui','haitao','faxian'))
group by dt,
if(hits_type='APPVIEW','pv','event'),
if(hits_type='APPVIEW',article_id,hits_product_productsku),
if(hits_appinfo_appversion regexp '^9.5','new','old');


create table bi_test.zyl_tmp_190712_7 as
select a.dt,a.v,a.id,
max(if(type='pv',sl,0)) pv,
max(if(type='event',sl,0)) event
from tmp2 a
inner join bi_test.zyl_tmp_190712_6 b on a.id=b.article_id
group by a.dt,a.v,a.id;


insert overwrite local directory '/data/tmp/zhaoyulong/data' row format delimited fields terminated by '\t'
select * from bi_test.zyl_tmp_190712_1 order by id;
insert overwrite local directory '/data/tmp/zhaoyulong/data' row format delimited fields terminated by '\t'
select * from bi_test.zyl_tmp_190712_2 order by coupon_id;
insert overwrite local directory '/data/tmp/zhaoyulong/data' row format delimited fields terminated by '\t'
select * from bi_test.zyl_tmp_190712_3 order by dt;
insert overwrite local directory '/data/tmp/zhaoyulong/data' row format delimited fields terminated by '\t'
select * from bi_test.zyl_tmp_190712_4 order by dt;
insert overwrite local directory '/data/tmp/zhaoyulong/data' row format delimited fields terminated by '\t'
select * from bi_test.zyl_tmp_190712_5 order by dt;
insert overwrite local directory '/data/tmp/zhaoyulong/data' row format delimited fields terminated by '\t'
select * from bi_test.zyl_tmp_190712_6 order by article_id;
insert overwrite local directory '/data/tmp/zhaoyulong/data' row format delimited fields terminated by '\t'
select * from bi_test.zyl_tmp_190712_7 order by dt;
