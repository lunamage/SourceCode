表头：日期/分钟/商城/一级品类/二级品类/分端/频道/文章数/电商点击/详情页PV/价格区间

create table bi_test.zyl_tmp_190614_1 as
select from_unixtime(unix_timestamp(visitstarttime)+cast(cast(hits_time as int)/1000 as int),'yyyy-MM-dd HH:mm:00') stat_dt,
case when a.hits_page_hostname regexp '(\.m.smzdm)|^(m|h5)' then 'wap' when a.hits_appinfo_appid='com.smzdm.client.android' or a.hits_appinfo_appid='com.smzdm.client.ios' then 'app' else 'pc' end splatform,
if(hits_type in('PAGE','APPVIEW'),'pv','event') type,
if(hits_type in('PAGE','APPVIEW'),article_id,hits_product_productsku) id,
count(*) sl
from bi_dw_ga.fact_ga_hits_data a
where dt between '2018-06-15' and '2018-06-19'
and (hits_page_hostname not regexp 'zhi.smzdm.com|go.smzdm.com' and hits_type='PAGE' and hits_page_hostname regexp '^(www|m).smzdm.com' and hits_page_pagepath regexp '/jingxuan|/p/' and hits_page_pagepath not regexp '/gourl/|/url|/business' and hits_page_pagepath not regexp 'm.smzdm.com/list|list_preview/[0-9]+' and article_id regexp '[0-9]+'
or hits_type='APPVIEW' and hits_appinfo_appid in('com.smzdm.client.android','com.smzdm.client.ios') and hits_appinfo_screenname regexp '好价' and hits_appinfo_screenname not regexp '好价\/闲值' and article_id regexp '[0-9]+'
or hits_ecommerceaction_action_type=3 and a.dim9 in('youhui','haitao','faxian','geren') and (a.hits_eventinfo_eventcategory like '%电商点击%' or (a.hits_eventinfo_eventaction like '%添加到购物车%' and (hits_appinfo_appid='com.smzdm.client.android' or hits_appinfo_appid='com.smzdm.client.ios'))))
group by from_unixtime(unix_timestamp(visitstarttime)+cast(cast(hits_time as int)/1000 as int),'yyyy-MM-dd HH:mm:00'),
case when a.hits_page_hostname regexp '(\.m.smzdm)|^(m|h5)' then 'wap' when a.hits_appinfo_appid='com.smzdm.client.android' or a.hits_appinfo_appid='com.smzdm.client.ios' then 'app' else 'pc' end,
if(hits_type in('PAGE','APPVIEW'),'pv','event'),
if(hits_type in('PAGE','APPVIEW'),article_id,hits_product_productsku);

create table bi_test.zyl_tmp_190614_2 as
select a.stat_dt,a.splatform,b.mall,b.cate_level1,b.cate_level2,b.channel_id,b.price,
count(distinct a.id) article_count,
sum(a.pv) pv,
sum(a.event) event
from(
select stat_dt,splatform,id,
max(if(type='pv',sl,0)) pv,
max(if(type='event',sl,0)) event
from bi_test.zyl_tmp_190614_1
group by stat_dt,splatform,id) a
inner join (select id,
channel_id,
mall,
cate_level1,
cate_level2,
case when article_type='优惠单品' and cast(price as int)<=10 and cast(price as int)>0  then '(0,10]'
when article_type='优惠单品' and cast(price as int)<=20 then '(10-20]'
when article_type='优惠单品' and cast(price as int)<=40 then '(20-40]'
when article_type='优惠单品' and cast(price as int)<=70 then '(40-70]'
when article_type='优惠单品' and cast(price as int)<=110 then '(70-110]'
when article_type='优惠单品' and cast(price as int)<=200 then '(110-200]'
when article_type='优惠单品' and cast(price as int)<=370 then '(200-370]'
when article_type='优惠单品' and cast(price as int)<=880 then '(370-880]'
when article_type='优惠单品' and cast(price as int)<=2300 then '(880-2300]'
when article_type='优惠单品' and cast(price as int)>2300 then '2300+' else '0' end price
from bi_dw_ga.dim_article_info where channel_id in(1,2,5,21)) b on a.id=b.id
group by a.stat_dt,a.splatform,b.mall,b.cate_level1,b.cate_level2,b.channel_id,b.price
order by stat_dt,splatform;


------------------------------------------------
insert overwrite table app_olap_ctr_test partition(dt)
select concat('{\"stat_dt\":\"',stat_d,' ',stat_h,':00:00\",',
'\"abtest\":\"',nvl(abtest,''),'\",',
'\"articleid\":\"',nvl(articleid,''),'\",',
'\"channel\":\"',nvl(channel,''),'\",',
'\"channel_item\":\"',nvl(channel_item,''),'\",',
'\"mall_type\":\"',nvl(mall_type,''),'\",',
'\"mall\":\"',nvl(mall,''),'\",',
'\"brand\":\"',nvl(brand,''),'\",',
'\"cate_level1\":\"',nvl(cate_level1,''),'\",',
'\"cate_level2\":\"',nvl(cate_level2,''),'\",',
'\"cate_level3\":\"',nvl(cate_level3,''),'\",',
'\"cate_level4\":\"',nvl(cate_level4,''),'\",',
'\"pubdate\":\"',nvl(pubdate,''),'\",',
'\"digital_price\":\"',nvl(digital_price,''),'\",',
'\"tags_name\":\"',nvl(tags_name,''),'\",',
'\"exposure\":',nvl(exposure,''),',',
'\"click\":',nvl(click,''),',',
'\"event\":',nvl(event,''),'}') json,
dt
from app_olap_ctr
where dt='2019-06-30'
order by json;
