/*
需在在2天的不同时间，导出两次

6月19日11点左右，导出最近24小时发布的文章
6月20日15点左右，导出最近24小时发布的文章

本需求取数为APP端文章数据
取出规定日期前24小时内发布文章id 左链接 sdk电商点击记录到文章id，频道为导数时候业务库记录的频道

APP版本限制在9.4及以上

t=event
ec=增强型电子商务
ea=添加到购物车

ecp中11限制为 youhui|haitao|faxian

取ecp中自定义维度4 做为文章id，自定义维度11作为频道
*/
--文章ID	发布时间	提优时间	导数时间	频道channel	SDK记录的APP端电商点击数

insert overwrite local directory '/data/tmp/zhaoyulong/data' row format delimited fields terminated by '\t'
select a.id,a.pubdate,a.choiceness_date,a.channel,b.sl
from(
select id,pubdate,choiceness_date,case when channel='1' then 'youhui' when channel='2' then 'faxian' else 'haitao' end channel
from stg.db_youhui_youhui
where dt='2019-06-19' and pubdate between '2019-06-18 11:00:00' and '2019-06-19 11:00:00'
and channel in('1','2','5')) a
left join (
select b.cd4 id,count(*) sl
from bi_ods_ga.ods_app_sdk_log a
lateral view json_tuple(a.ecp,'4','11') b as cd4,cd11
where a.dt between '2019-06-18' and '2019-06-19' and a.av regexp '^9.[4-9]'
and a.it between '2019-06-18 11:00:00' and '2019-06-19 11:00:00'
and a.ec='增强型电子商务' and a.ea='添加到购物车' and b.cd11 regexp 'youhui|haitao|faxian'
group by b.cd4) b on a.id=b.id
order by id;


insert overwrite local directory '/data/tmp/zhaoyulong/data' row format delimited fields terminated by '\t'
select a.id,a.pubdate,a.choiceness_date,a.channel,b.sl
from(
select id,pubdate,choiceness_date,case when channel='1' then 'youhui' when channel='2' then 'faxian' else 'haitao' end channel
from stg.db_youhui_youhui
where dt='2019-06-20' and pubdate between '2019-06-19 15:00:00' and '2019-06-20 15:00:00'
and channel in('1','2','5')) a
left join (
select b.cd4 id,count(*) sl
from bi_ods_ga.ods_app_sdk_log a
lateral view json_tuple(a.ecp,'4','11') b as cd4,cd11
where a.dt between '2019-06-19' and '2019-06-20' and a.av regexp '^9.[4-9]'
and a.it between '2019-06-19 15:00:00' and '2019-06-20 15:00:00'
and a.ec='增强型电子商务' and a.ea='添加到购物车' and b.cd11 regexp 'youhui|haitao|faxian'
group by b.cd4) b on a.id=b.id
order by id;
