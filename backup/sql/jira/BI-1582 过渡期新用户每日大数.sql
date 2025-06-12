--表头：日期 流量 用户量 曝光 详情点击人数 详情点击次数 电商点击人数 电商点击次数

create table bi_test.zyl_tmp_200313_1 as
select dt,type,count(distinct device_id) uv1,count(*) sl1,
count(distinct if(is_click='1',device_id,null)) uv2,sum(is_click) sl2,
count(distinct if(is_event='1',device_id,null)) uv3,sum(is_event) sl3
from bi_ctr.home_exposure_sample_old
where dt between '2020-03-17' and '2020-03-31'
group by dt,type order by dt,type;
--

create table bi_test.zyl_tmp_200313_2 as
select distinct dt,device_id did
from bi_ctr.home_exposure_sample_old
where dt between '2020-03-17' and '2020-03-31';

select dt,count(*) from bi_test.zyl_tmp_200313_2 group by dt order by dt;

create table bi_test.zyl_tmp_200313_3 as
select a.dt,a.abtest,count(distinct a.did) uv1,sum(a.sl1) sl1,
count(distinct if(a.sl2=1,a.did,null)) uv2,sum(a.sl2) sl2,
count(distinct if(a.sl3=1,a.did,null)) uv3,sum(a.sl3) sl3
from(
select dt,case when a.anid<>'' then a.anid else a.did end did,
if(a.ec='01',b.tv,b.cd13) abtest,
if(a.ec='01' and a.ea='01' and b.sp='0',1,0) sl1,
if(a.ec='首页' and a.ea='首页站内文章点击' and a.el regexp '^推荐_',1,0) sl2,
if(a.ec='增强型电子商务' and a.ea='添加到购物车' and b.cd21 regexp '^首页_推荐_feed流_',1,0) sl3
from bi_ods_ga.ods_app_sdk_log a
lateral view json_tuple(a.ecp,'a','c','tv','p','sp','atp','20','4','21','13') b as a,c,tv,p,sp,atp,cd20,cd4,cd21,cd13
where a.dt between '2020-03-17' and '2020-03-31'
and (a.ec='01' and a.ea='01' and b.sp='0' or a.ec='首页' and a.ea='首页站内文章点击' and a.el regexp '^推荐_' or a.ec='增强型电子商务' and a.ea='添加到购物车' and b.cd21 regexp '^首页_推荐_feed流_')) a
inner join bi_test.zyl_tmp_200313_2 b on a.dt=b.dt and a.did=b.did
group by a.dt,a.abtest;


------
create table bi_test.zyl_tmp_200313_4 as
select a.dt,a.abtest,count(distinct a.did) uv1,sum(a.sl1) sl1,
count(distinct if(a.sl2=1,a.did,null)) uv2,sum(a.sl2) sl2,
count(distinct if(a.sl3=1,a.did,null)) uv3,sum(a.sl3) sl3
from(
select dt,case when a.anid<>'' then a.anid else a.did end did,
if(a.ec='01',b.tv,b.cd13) abtest,
if(a.ec='01' and a.ea='01' and b.sp='0',1,0) sl1,
if(a.ec='首页' and a.ea='首页站内文章点击' and a.el regexp '^推荐_',1,0) sl2,
if(a.ec='增强型电子商务' and a.ea='添加到购物车' and b.cd21 regexp '^首页_推荐_feed流_',1,0) sl3
from bi_ods_ga.ods_app_sdk_log a
lateral view json_tuple(a.ecp,'a','c','tv','p','sp','atp','20','4','21','13') b as a,c,tv,p,sp,atp,cd20,cd4,cd21,cd13
where a.dt between '2020-02-16' and '2020-02-25'
and (a.ec='01' and a.ea='01' and b.sp='0' or a.ec='首页' and a.ea='首页站内文章点击' and a.el regexp '^推荐_' or a.ec='增强型电子商务' and a.ea='添加到购物车' and b.cd21 regexp '^首页_推荐_feed流_')) a
inner join bi_test.zyl_tmp_200313_2 b on a.dt=b.dt and a.did=b.did
group by a.dt,a.abtest;

create table bi_test.zyl_tmp_200313_5 as
select a.dt,a.abtest,count(distinct a.did) uv1,sum(a.sl1) sl1,
count(distinct if(a.sl2=1,a.did,null)) uv2,sum(a.sl2) sl2,
count(distinct if(a.sl3=1,a.did,null)) uv3,sum(a.sl3) sl3
from(
select dt,case when a.anid<>'' then a.anid else a.did end did,
if(a.ec='01',b.tv,b.cd13) abtest,
if(a.ec='01' and a.ea='01' and b.sp='0',1,0) sl1,
if(a.ec='首页' and a.ea='首页站内文章点击' and a.el regexp '^推荐_',1,0) sl2,
if(a.ec='增强型电子商务' and a.ea='添加到购物车' and b.cd21 regexp '^首页_推荐_feed流_',1,0) sl3
from bi_ods_ga.ods_app_sdk_log a
lateral view json_tuple(a.ecp,'a','c','tv','p','sp','atp','20','4','21','13') b as a,c,tv,p,sp,atp,cd20,cd4,cd21,cd13
where a.dt between '2020-02-26' and '2020-03-04'
and (a.ec='01' and a.ea='01' and b.sp='0' or a.ec='首页' and a.ea='首页站内文章点击' and a.el regexp '^推荐_' or a.ec='增强型电子商务' and a.ea='添加到购物车' and b.cd21 regexp '^首页_推荐_feed流_')) a
inner join bi_test.zyl_tmp_200313_2 b on a.dt=b.dt and a.did=b.did
group by a.dt,a.abtest;

create table bi_test.zyl_tmp_200313_6 as
select a.dt,a.abtest,count(distinct a.did) uv1,sum(a.sl1) sl1,
count(distinct if(a.sl2=1,a.did,null)) uv2,sum(a.sl2) sl2,
count(distinct if(a.sl3=1,a.did,null)) uv3,sum(a.sl3) sl3
from(
select dt,case when a.anid<>'' then a.anid else a.did end did,
if(a.ec='01',b.tv,b.cd13) abtest,
if(a.ec='01' and a.ea='01' and b.sp='0',1,0) sl1,
if(a.ec='首页' and a.ea='首页站内文章点击' and a.el regexp '^推荐_',1,0) sl2,
if(a.ec='增强型电子商务' and a.ea='添加到购物车' and b.cd21 regexp '^首页_推荐_feed流_',1,0) sl3
from bi_ods_ga.ods_app_sdk_log a
lateral view json_tuple(a.ecp,'a','c','tv','p','sp','atp','20','4','21','13') b as a,c,tv,p,sp,atp,cd20,cd4,cd21,cd13
where a.dt between '2020-03-17' and '2020-03-31'
and (a.ec='01' and a.ea='01' and b.sp='0' or a.ec='首页' and a.ea='首页站内文章点击' and a.el regexp '^推荐_' or a.ec='增强型电子商务' and a.ea='添加到购物车' and b.cd21 regexp '^首页_推荐_feed流_')) a
inner join bi_test.zyl_tmp_200313_2 b on a.dt=b.dt and a.did=b.did
group by a.dt,a.abtest;


insert overwrite local directory '/data/tmp/zhaoyulong/data' row format delimited fields terminated by '\t'
select *
from(
select dt,abtest,uv1,sl1,uv2,sl2,uv3,sl3 from bi_test.zyl_tmp_200313_3
union All
select dt,abtest,uv1,sl1,uv2,sl2,uv3,sl3 from bi_test.zyl_tmp_200313_4
union All
select dt,abtest,uv1,sl1,uv2,sl2,uv3,sl3 from bi_test.zyl_tmp_200313_5
union All
select dt,abtest,uv1,sl1,uv2,sl2,uv3,sl3 from bi_test.zyl_tmp_200313_6) a
order by dt,abtest;


insert overwrite local directory '/data/tmp/zhaoyulong/data' row format delimited fields terminated by '\t'
select *
from(
select dt,abtest,uv1,sl1,uv2,sl2,uv3,sl3 from bi_test.zyl_tmp_200313_3) a
order by dt,abtest;
