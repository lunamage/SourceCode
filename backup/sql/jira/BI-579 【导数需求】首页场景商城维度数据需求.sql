--维度:频道、一级品类、二级品类、三级品类、价格区间、商城
cache table tmp_article as
select id,
channel_id,
nvl(cate_level1,'其他') cate_level1,
case when price=0 then '0'
when price>0 and price<=20 then '(0,20]'
when price>20 and price<=50 then '(20,50]'
when price>50 and price<=100 then '(50,100]'
when price>100 and price<=200 then '(100,200]'
when price>200 and price<=500 then '(200,500]'
when price>500 and price<=1000 then '(500,1000]'
when price>1000 and price<=2000 then '(1000,2000]'
when price>2000 and price<=5000 then '(2000,5000]'
when price>5000 and price<=10000 then '(5000,10000]'
when price>10000 then '10000+' else '其他' end price_range,
nvl(mall,'其他') mall
from (select if(channel_id in('1','2','5','21'),id,hash_id) id,channel_id,cate_level1,cate_level2,cate_level3,cast(price as double) price,mall from bi_dw_ga.dim_article_info where channel_id in('1','2','5','21','6','8','11','31','66')) a;


--指标:曝光文章数、曝光数、有效曝光数
cache table temp_exposure as
select nvl(a.dt,'all') dt,
nvl(b.mall,'all') mall,
nvl(b.cate_level1,'all') cate_level1,
nvl(b.price_range,'all') price_range,
count(*) exposure_count,
count(distinct article_id) article_count
from
(select dt,article_id
from bi_dw_ga.fact_recsys_display_data
where dt in('2018-06-01','2018-06-18','2018-11-01','2018-11-11') )a
inner join tmp_article b on a.article_id=b.id
group by a.dt,b.cate_level1,b.price_range,b.mall
grouping sets((a.dt,b.mall,b.cate_level1),(a.dt,b.mall,b.price_range));

--指标:详情点击、电商点击
cache table temp_ga as
select nvl(a.dt,'all') dt,
nvl(b.mall,'all') mall,
nvl(b.cate_level1,'all') cate_level1,
nvl(b.price_range,'all') price_range,
sum(if(a.type='click',1,0)) click_count,
sum(if(a.type='event',1,0)) event_count
from(select dt,if(hits_eventinfo_eventaction='首页站内文章点击','click','event') type,
if(hits_eventinfo_eventaction='首页站内文章点击',regexp_extract(hits_eventinfo_eventlabel,'推荐_([^_]+)_([^_]+)',2),hits_product_productsku) article_id
from bi_dw_ga.fact_ga_hits_data
where dt in('2018-06-01','2018-06-18','2018-11-01','2018-11-11') and hits_appinfo_appid in('com.smzdm.client.android','com.smzdm.client.ios') and hits_appinfo_appversion regexp '^8.(7|8|9)|^9.'
and (hits_eventinfo_eventcategory ='首页' and hits_eventinfo_eventaction='首页站内文章点击' and hits_eventinfo_eventlabel regexp '^推荐'
or hits_ecommerceaction_action_type='3' and hits_eventinfo_eventaction='添加到购物车' and dim64 regexp '首页_推荐_feed流')) a
inner join tmp_article b on a.article_id=b.id
group by a.dt,b.cate_level1,b.price_range,b.mall
grouping sets((a.dt,b.mall,b.cate_level1),(a.dt,b.mall,b.price_range));

cache table tmp_cube as
select a.dt,
a.cate_level1,
a.price_range,
a.mall,
a.exposure_count,
a.article_count,
nvl(b.click_count,0) click_count,
nvl(b.event_count,0) event_count
from temp_exposure a
left join temp_ga b on a.dt=b.dt and a.cate_level1=b.cate_level1 and a.price_range=b.price_range and a.mall=b.mall;

--
create table bi_test.zyl_tmp_190529_1 as
select dt,
mall,
case when mall in('聚划算','天猫精选','天猫超市','天猫国际','飞猪','95095医药','天猫电器城','天猫国际官方直营','淘宝心选') then '淘宝系'
when mall in('京东','京东全球购','京东全球购eBay精选','京东金融','京东到家','京东众筹','京东酒店','京东国际') then '京东系' else '其他商城' end dim2,
cate_level1,
exposure_count,
article_count,
click_count,
event_count
from tmp_cube
where mall<>'all' and cate_level1<>'all';

create table bi_test.zyl_tmp_190529_2 as
select dt,
mall,
case when mall in('聚划算','天猫精选','天猫超市','天猫国际','飞猪','95095医药','天猫电器城','天猫国际官方直营','淘宝心选') then '淘宝系'
when mall in('京东','京东全球购','京东全球购eBay精选','京东金融','京东到家','京东众筹','京东酒店','京东国际') then '京东系' else '其他商城' end dim2,
price_range,
exposure_count,
article_count,
click_count,
event_count
from tmp_cube
where mall<>'all' and price_range<>'all';

-------------
--指标:曝光文章数、曝光数、有效曝光数
cache table temp_exposure as
select nvl(a.dt,'all') dt,
nvl(b.mall,'all') mall,
nvl(b.cate_level1,'all') cate_level1,
nvl(b.price_range,'all') price_range,
count(*) exposure_count,
count(distinct article_id) article_count
from
(select dt,article_id
from bi_dw_ga.fact_recsys_display_data_sdk
where dt in('2019-05-24') )a
inner join tmp_article b on a.article_id=b.id
group by a.dt,b.cate_level1,b.price_range,b.mall
grouping sets((a.dt,b.mall,b.cate_level1),(a.dt,b.mall,b.price_range));

--指标:详情点击、电商点击
cache table temp_ga as
select nvl(a.dt,'all') dt,
nvl(b.mall,'all') mall,
nvl(b.cate_level1,'all') cate_level1,
nvl(b.price_range,'all') price_range,
sum(if(a.type='click',1,0)) click_count,
sum(if(a.type='event',1,0)) event_count
from(select dt,if(hits_eventinfo_eventaction='首页站内文章点击','click','event') type,
if(hits_eventinfo_eventaction='首页站内文章点击',regexp_extract(hits_eventinfo_eventlabel,'推荐_([^_]+)_([^_]+)',2),hits_product_productsku) article_id
from bi_dw_ga.fact_ga_hits_data
where dt in('2019-05-24') and hits_appinfo_appid in('com.smzdm.client.android','com.smzdm.client.ios') and hits_appinfo_appversion regexp '^8.(7|8|9)|^9.'
and (hits_eventinfo_eventcategory ='首页' and hits_eventinfo_eventaction='首页站内文章点击' and hits_eventinfo_eventlabel regexp '^推荐'
or hits_ecommerceaction_action_type='3' and hits_eventinfo_eventaction='添加到购物车' and dim64 regexp '首页_推荐_feed流')) a
inner join tmp_article b on a.article_id=b.id
group by a.dt,b.cate_level1,b.price_range,b.mall
grouping sets((a.dt,b.mall,b.cate_level1),(a.dt,b.mall,b.price_range));

cache table tmp_cube as
select a.dt,
a.cate_level1,
a.price_range,
a.mall,
a.exposure_count,
a.article_count,
nvl(b.click_count,0) click_count,
nvl(b.event_count,0) event_count
from temp_exposure a
left join temp_ga b on a.dt=b.dt and a.cate_level1=b.cate_level1 and a.price_range=b.price_range and a.mall=b.mall;


create table bi_test.zyl_tmp_190529_3 as
select dt,
mall,
case when mall in('聚划算','天猫精选','天猫超市','天猫国际','飞猪','95095医药','天猫电器城','天猫国际官方直营','淘宝心选') then '淘宝系'
when mall in('京东','京东全球购','京东全球购eBay精选','京东金融','京东到家','京东众筹','京东酒店','京东国际') then '京东系' else '其他商城' end dim2,
cate_level1,
exposure_count,
article_count,
click_count,
event_count
from tmp_cube
where mall<>'all' and cate_level1<>'all';

create table bi_test.zyl_tmp_190529_4 as
select dt,
mall,
case when mall in('聚划算','天猫精选','天猫超市','天猫国际','飞猪','95095医药','天猫电器城','天猫国际官方直营','淘宝心选') then '淘宝系'
when mall in('京东','京东全球购','京东全球购eBay精选','京东金融','京东到家','京东众筹','京东酒店','京东国际') then '京东系' else '其他商城' end dim2,
price_range,
exposure_count,
article_count,
click_count,
event_count
from tmp_cube
where mall<>'all' and price_range<>'all';





insert overwrite local directory '/data/tmp/zhaoyulong/data' row format delimited fields terminated by '\t'
select *
from (select dt,mall,dim2,cate_level1,exposure_count,article_count,click_count,event_count from bi_test.zyl_tmp_190529_1 union all select dt,mall,dim2,cate_level1,exposure_count,article_count,click_count,event_count from bi_test.zyl_tmp_190529_3) a
order by dt;

insert overwrite local directory '/data/tmp/zhaoyulong/data' row format delimited fields terminated by '\t'
select *
from (select dt,mall,dim2,price_range,exposure_count,article_count,click_count,event_count from bi_test.zyl_tmp_190529_2 union all select dt,mall,dim2,price_range,exposure_count,article_count,click_count,event_count from bi_test.zyl_tmp_190529_4) a
order by dt;


---精选发布量和曝光文章量

insert overwrite local directory '/data/tmp/zhaoyulong/data' row format delimited fields terminated by '\t'
select pubdate,
nvl(mall,'其他') mall,
nvl(cate_level1,'其他') cate_level1,
count(*) sl
from (select id,channel_id,cate_level1,cate_level2,cate_level3,cast(price as double) price,mall,to_date(pubdate) pubdate from bi_dw_ga.dim_article_info where channel_id in('1','5') and to_date(pubdate) in('2018-06-01','2018-06-18','2018-11-01','2018-11-11','2019-05-24')) a
group by pubdate,nvl(mall,'其他'),nvl(cate_level1,'其他');



insert overwrite local directory '/data/tmp/zhaoyulong/data' row format delimited fields terminated by '\t'
select pubdate,
nvl(mall,'其他') mall,
case when price=0 then '0'
when price>0 and price<=20 then '(0,20]'
when price>20 and price<=50 then '(20,50]'
when price>50 and price<=100 then '(50,100]'
when price>100 and price<=200 then '(100,200]'
when price>200 and price<=500 then '(200,500]'
when price>500 and price<=1000 then '(500,1000]'
when price>1000 and price<=2000 then '(1000,2000]'
when price>2000 and price<=5000 then '(2000,5000]'
when price>5000 and price<=10000 then '(5000,10000]'
when price>10000 then '10000+' else '其他' end price_range,
count(*) sl
from (select id,channel_id,cate_level1,cate_level2,cate_level3,cast(price as double) price,mall,to_date(pubdate) pubdate from bi_dw_ga.dim_article_info where channel_id in('1','5') and to_date(pubdate) in('2018-06-01','2018-06-18','2018-11-01','2018-11-11','2019-05-24')) a
group by pubdate,nvl(mall,'其他'),case when price=0 then '0'
when price>0 and price<=20 then '(0,20]'
when price>20 and price<=50 then '(20,50]'
when price>50 and price<=100 then '(50,100]'
when price>100 and price<=200 then '(100,200]'
when price>200 and price<=500 then '(200,500]'
when price>500 and price<=1000 then '(500,1000]'
when price>1000 and price<=2000 then '(1000,2000]'
when price>2000 and price<=5000 then '(2000,5000]'
when price>5000 and price<=10000 then '(5000,10000]'
when price>10000 then '10000+' else '其他' end;
