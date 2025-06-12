create table bi_test.zyl_tmp_190613_1 as
select concat(substr(a.it,1,13),':00:00') dt,
case when a.ec='01' then 'exposure' when a.ec='首页' then 'click' else 'event' end type,
if(a.ec='01',b.tv,b.cd13) abtest,
count(*) sl
from bi_ods_ga.ods_app_sdk_log a
lateral view json_tuple(a.ecp,'a','c','tv','p','sp','atp','20','4','21','13') b as a,c,tv,p,sp,atp,cd20,cd4,cd21,cd13
where a.dt='2019-06-13' and a.it between '2019-06-13' and concat('2019-06-13',' 23:59:59')
and (a.ec='01' and a.ea='01' and b.sp='0' and b.atp in('3') or a.ec='首页' and a.ea='首页站内文章点击' and b.cd20 in('3') or a.ec='增强型电子商务' and a.ea='添加到购物车' and b.cd21 regexp '^首页_推荐' and b.cd20 in('3'))
group by concat(substr(a.it,1,13),':00:00'),
case when a.ec='01' then 'exposure' when a.ec='首页' then 'click' else 'event' end,
if(a.ec='01',b.tv,b.cd13);


insert overwrite local directory '/data/tmp/zhaoyulong/data' row format delimited fields terminated by '\t'
select dt,abtest,
sum(if(type='exposure',sl,0)) exposure,
sum(if(type='click',sl,0)) click,
sum(if(type='event',sl,0)) event
from bi_test.zyl_tmp_190613_1
group by dt,abtest order by abtest,dt;
