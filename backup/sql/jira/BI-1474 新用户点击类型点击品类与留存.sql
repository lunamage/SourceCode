--
create table bi_test.zyl_tmp_200219_1 as
select a.dt,a.did,a.is_event,a.is_click,b.yq_click,b.fyq_click
from(
select a.dt,a.did,
max(if(a.ec='增强型电子商务' and a.ea='添加到购物车',1,0)) is_event,
max(if(a.t='screenview' and a.sn regexp '/好价[/p|/P|/发现|国内|/海淘|过期]*/([0-9]+)',1,0)) is_click
from bi_ods_ga.ods_app_sdk_log a
lateral view json_tuple(a.ecp,'a','c','tv','p','sp','atp','20','4','21','13') b as a,c,tv,p,sp,atp,cd20,cd4,cd21,cd13
where a.dt between '2020-01-17' and '2020-01-27' and isnp='1'
group by a.dt,a.did) a
left join(
SELECT a.dt,a.did,if(max(is_click2)=1,1,0) yq_click,if(min(is_click2)=0,1,0) fyq_click
from(
select a.dt,a.did,if(b.title regexp '口罩|消毒|洗手液|酒精|疫',1,0) is_click2
from(
select a.dt,a.did,
regexp_extract(a.sn,'/好价[/p|/P|/发现|国内|/海淘|过期]*/([0-9]+)',1) id
from bi_ods_ga.ods_app_sdk_log a
lateral view json_tuple(a.ecp,'a','c','tv','p','sp','atp','20','4','21','13') b as a,c,tv,p,sp,atp,cd20,cd4,cd21,cd13
where a.dt between '2020-01-17' and '2020-01-27' and isnp='1' and a.t='screenview' and a.sn regexp '/好价[/p|/P|/发现|国内|/海淘|过期]*/([0-9]+)') a
left join(select * from bi_dw_ga.dim_article_info where channel_id in('1','2','5','21')) b on a.id=b.id) a
group by a.dt,a.did) b on a.dt=b.dt and a.did=b.did;
--
create table bi_test.zyl_tmp_200219_2 as
select a.dt,a.did,a.is_event,a.is_click,b.yq_click,b.fyq_click
from(
select a.dt,a.did,
max(if(a.ec='增强型电子商务' and a.ea='添加到购物车',1,0)) is_event,
max(if(a.t='screenview' and a.sn regexp '/好价[/p|/P|/发现|国内|/海淘|过期]*/([0-9]+)',1,0)) is_click
from bi_ods_ga.ods_app_sdk_log a
lateral view json_tuple(a.ecp,'a','c','tv','p','sp','atp','20','4','21','13') b as a,c,tv,p,sp,atp,cd20,cd4,cd21,cd13
where a.dt between '2020-01-28' and '2020-02-07' and isnp='1'
group by a.dt,a.did) a
left join(
SELECT a.dt,a.did,if(max(is_click2)=1,1,0) yq_click,if(min(is_click2)=0,1,0) fyq_click
from(
select a.dt,a.did,if(b.title regexp '口罩|消毒|洗手液|酒精|疫',1,0) is_click2
from(
select a.dt,a.did,
regexp_extract(a.sn,'/好价[/p|/P|/发现|国内|/海淘|过期]*/([0-9]+)',1) id
from bi_ods_ga.ods_app_sdk_log a
lateral view json_tuple(a.ecp,'a','c','tv','p','sp','atp','20','4','21','13') b as a,c,tv,p,sp,atp,cd20,cd4,cd21,cd13
where a.dt between '2020-01-28' and '2020-02-07' and isnp='1' and a.t='screenview' and a.sn regexp '/好价[/p|/P|/发现|国内|/海淘|过期]*/([0-9]+)') a
left join(select * from bi_dw_ga.dim_article_info where channel_id in('1','2','5','21')) b on a.id=b.id) a
group by a.dt,a.did) b on a.dt=b.dt and a.did=b.did;
--
create table bi_test.zyl_tmp_200219_3 as
select a.dt,a.did,a.is_event,a.is_click,b.yq_click,b.fyq_click
from(
select a.dt,a.did,
max(if(a.ec='增强型电子商务' and a.ea='添加到购物车',1,0)) is_event,
max(if(a.t='screenview' and a.sn regexp '/好价[/p|/P|/发现|国内|/海淘|过期]*/([0-9]+)',1,0)) is_click
from bi_ods_ga.ods_app_sdk_log a
lateral view json_tuple(a.ecp,'a','c','tv','p','sp','atp','20','4','21','13') b as a,c,tv,p,sp,atp,cd20,cd4,cd21,cd13
where a.dt between '2020-02-08' and '2020-02-17' and isnp='1'
group by a.dt,a.did) a
left join(
SELECT a.dt,a.did,if(max(is_click2)=1,1,0) yq_click,if(min(is_click2)=0,1,0) fyq_click
from(
select a.dt,a.did,if(b.title regexp '口罩|消毒|洗手液|酒精|疫',1,0) is_click2
from(
select a.dt,a.did,
regexp_extract(a.sn,'/好价[/p|/P|/发现|国内|/海淘|过期]*/([0-9]+)',1) id
from bi_ods_ga.ods_app_sdk_log a
lateral view json_tuple(a.ecp,'a','c','tv','p','sp','atp','20','4','21','13') b as a,c,tv,p,sp,atp,cd20,cd4,cd21,cd13
where a.dt between '2020-02-08' and '2020-02-17' and isnp='1' and a.t='screenview' and a.sn regexp '/好价[/p|/P|/发现|国内|/海淘|过期]*/([0-9]+)') a
left join(select * from bi_dw_ga.dim_article_info where channel_id in('1','2','5','21')) b on a.id=b.id) a
group by a.dt,a.did) b on a.dt=b.dt and a.did=b.did;

--
create table bi_test.zyl_tmp_200219_4 as
select distinct a.dt,a.did
from bi_ods_ga.ods_app_sdk_log a
where a.dt between '2020-01-17' and '2020-01-27';

--
create table bi_test.zyl_tmp_200219_5 as
select distinct a.dt,a.did
from bi_ods_ga.ods_app_sdk_log a
where a.dt between '2020-01-28' and '2020-02-07';

--
create table bi_test.zyl_tmp_200219_6 as
select distinct a.dt,a.did
from bi_ods_ga.ods_app_sdk_log a
where a.dt between '2020-02-08' and '2020-02-17';


cache table tmp as
select dt,did from bi_test.zyl_tmp_200219_4
union all
select dt,did from bi_test.zyl_tmp_200219_5
union all
select dt,did from bi_test.zyl_tmp_200219_6;


--日期	产生电商点击的用户数	次日留存量	3日留存量	7日留存量
create table bi_test.zyl_tmp_200219_11 as
select a.dt,count(*) sl,count(distinct b.did) sl2,count(distinct c.did) sl3,count(distinct d.did) sl7
from(
select dt,did from bi_test.zyl_tmp_200219_1 where is_event=1
union all
select dt,did from bi_test.zyl_tmp_200219_2 where is_event=1
union all
select dt,did from bi_test.zyl_tmp_200219_3 where is_event=1) a
left join tmp b on a.did=b.did and a.dt=date_sub(b.dt,1)
left join tmp c on a.did=c.did and a.dt=date_sub(c.dt,2)
left join tmp d on a.did=d.did and a.dt=date_sub(d.dt,6)
group by a.dt;
--日期	仅产生详情点击的用户数	次日留存量	3日留存量	7日留存量
create table bi_test.zyl_tmp_200219_12 as
select a.dt,count(*) sl,count(distinct b.did) sl2,count(distinct c.did) sl3,count(distinct d.did) sl7
from(
select dt,did from bi_test.zyl_tmp_200219_1 where is_event=0 and is_click=1
union all
select dt,did from bi_test.zyl_tmp_200219_2 where is_event=0 and is_click=1
union all
select dt,did from bi_test.zyl_tmp_200219_3 where is_event=0 and is_click=1) a
left join tmp b on a.did=b.did and a.dt=date_sub(b.dt,1)
left join tmp c on a.did=c.did and a.dt=date_sub(c.dt,2)
left join tmp d on a.did=d.did and a.dt=date_sub(d.dt,6)
group by a.dt;
--日期	未产生详情点击的用户数	次日留存量	3日留存量	7日留存量
create table bi_test.zyl_tmp_200219_13 as
select a.dt,count(*) sl,count(distinct b.did) sl2,count(distinct c.did) sl3,count(distinct d.did) sl7
from(
select dt,did from bi_test.zyl_tmp_200219_1 where is_click=0
union all
select dt,did from bi_test.zyl_tmp_200219_2 where is_click=0
union all
select dt,did from bi_test.zyl_tmp_200219_3 where is_click=0) a
left join tmp b on a.did=b.did and a.dt=date_sub(b.dt,1)
left join tmp c on a.did=c.did and a.dt=date_sub(c.dt,2)
left join tmp d on a.did=d.did and a.dt=date_sub(d.dt,6)
group by a.dt;
--日期	仅产生疫情相关详情点击的用户数	次日留存量	3日留存量	7日留存量
create table bi_test.zyl_tmp_200219_14 as
select a.dt,count(*) sl,count(distinct b.did) sl2,count(distinct c.did) sl3,count(distinct d.did) sl7
from(
select dt,did from bi_test.zyl_tmp_200219_1 where yq_click=1 and fyq_click=0
union all
select dt,did from bi_test.zyl_tmp_200219_2 where yq_click=1 and fyq_click=0
union all
select dt,did from bi_test.zyl_tmp_200219_3 where yq_click=1 and fyq_click=0) a
left join tmp b on a.did=b.did and a.dt=date_sub(b.dt,1)
left join tmp c on a.did=c.did and a.dt=date_sub(c.dt,2)
left join tmp d on a.did=d.did and a.dt=date_sub(d.dt,6)
group by a.dt;
--日期	非仅产生疫情相关详情点击的用户数	次日留存量	3日留存量	7日留存量
create table bi_test.zyl_tmp_200219_15 as
select a.dt,count(*) sl,count(distinct b.did) sl2,count(distinct c.did) sl3,count(distinct d.did) sl7
from(
select dt,did from bi_test.zyl_tmp_200219_1 where yq_click<>1 or fyq_click<>0
union all
select dt,did from bi_test.zyl_tmp_200219_2 where yq_click<>1 or fyq_click<>0
union all
select dt,did from bi_test.zyl_tmp_200219_3 where yq_click<>1 or fyq_click<>0) a
left join tmp b on a.did=b.did and a.dt=date_sub(b.dt,1)
left join tmp c on a.did=c.did and a.dt=date_sub(c.dt,2)
left join tmp d on a.did=d.did and a.dt=date_sub(d.dt,6)
group by a.dt;



insert overwrite local directory '/data/tmp/zhaoyulong/data' row format delimited fields terminated by '\t'
select a.dt,a.sl,a.sl2,a.sl3,a.sl7,
b.sl,b.sl2,b.sl3,b.sl7,
c.sl,c.sl2,c.sl3,c.sl7,
d.sl,d.sl2,d.sl3,d.sl7,
e.sl,e.sl2,e.sl3,e.sl7
from bi_test.zyl_tmp_200219_11 a
left join bi_test.zyl_tmp_200219_12 b on a.dt=b.dt
left join bi_test.zyl_tmp_200219_13 c on a.dt=c.dt
left join bi_test.zyl_tmp_200219_14 d on a.dt=d.dt
left join bi_test.zyl_tmp_200219_15 e on a.dt=e.dt
order by dt;
