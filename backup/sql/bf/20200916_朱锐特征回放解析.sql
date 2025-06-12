
create table bi_test.zyl_tmp_200907_3 as
select md5(concat(a.user_id,nvl(a.ab_test,''),nvl(a.pid,''),nvl(b.article_hash_id,a.article_id),nvl(a.channel_id,''))) id,
score,fstModelScore,modelScoreMap
from(
select d.ab_test,d.pid,d.user_id,datas_item.article_id,datas_item.channel_id,datas_item.score,datas_item.fstModelScore,modelScoreMap
from (
select b.ab_test,b.device_id,b.pid,b.timestamp,b.user_id, split(regexp_replace(regexp_extract(b.datas,'^\\\[(.+)\\\]$',1),'\\\}\\\,\\\{', '\\\}\\\|\\\|\\\{'),'\\\|\\\|') as str
from bi_test.zyl_tmp_200910 a
lateral view json_tuple(a.json_data,'device_id','pid','user_id','datas','ab_test','timestamp') b as device_id,pid,user_id, datas,ab_test,timestamp
where a.json_data regexp '(?i)modelScoreMap') d
lateral view explode(d.str) c as col
lateral view json_tuple(c.col,'article_id','channel_id','featureMap','f','score','fstModelScore','modelScoreMap') datas_item as article_id,channel_id,featureMap,f,score,fstModelScore,modelScoreMap) a
left join (select article_id,article_hash_id from stg.db_smzdm_article_article_index where dt='2020-09-09') b on a.article_id=b.article_id and a.channel_id in(11,6,8,31,66);



--------
select * from bi_test.zyl_tmp_200907_3 limit 10;

drop table bi_test.zyl_tmp_200907_4;
create table bi_test.zyl_tmp_200907_4 as
select id,score,fstModelScore,modelScoreMap
from(
select row_number()over(partition by id order by fstModelScore) r,*
from bi_test.zyl_tmp_200907_3) a
where r=1;




cache table ga_click_event as
select distinct case when a.ec='首页' then 'click' else 'event' end type,
a.uid dim16,
case when a.ec='01' then b.a when a.ec='首页' then regexp_extract(a.el,'_([^_]+)_([^_]+)',2) else b.cd4 end article_id
from bi_ods_ga.ods_app_sdk_log a
lateral view json_tuple(a.ecp,'a','c','tv','p','sp','atp','20','4','21','13') b as a,c,tv,p,sp,atp,cd20,cd4,cd21,cd13
where a.dt='2020-09-04' and a.it between '2020-09-04' and concat('2020-09-04',' 23:59:59') and a.uid<>'' and a.uid<>'0'
and (a.ec='首页' and a.ea='首页站内文章点击' and a.el regexp '^推荐_'
or a.ec='增强型电子商务' and a.ea='添加到购物车' and b.cd21 regexp '^首页_推荐_feed流_');


--曝光
cache table temp_exposure as
select distinct
md5(concat(user_id,nvl(type,''),nvl(pid,''),nvl(article_id,''),nvl(remark4,''))) id,
creation_date,
user_id,
device_id,
type,
remark3,
version,
article_id,
remark4,
pid
from(
select row_number()over(partition by a.uid,b.a order by a.it) r,
a.it creation_date,
a.uid user_id,
case when a.imei<>'' then a.imei else a.did end device_id,
b.tv type,
b.p remark3,
a.av version,
b.a article_id,
b.c remark4,
b.pid
from bi_ods_ga.ods_app_sdk_log a
lateral view json_tuple(a.ecp,'a','c','tv','p','sp','pid') b as a,c,tv,p,sp,pid
where a.dt='2020-09-04' and a.ec='01' and a.ea='01' and b.sp='0' and a.it between '2020-09-04' and concat('2020-09-04',' 23:59:59') and a.uid<>'' and a.uid<>'0' and b.c in('1','2','5','6','8','11','31','66')) a
where r=1;


--曝光关联
cache table temp_click_event as
select a.id,
if(b.dim16 is null,'0','1') is_click,
if(c.dim16 is null,'0','1') is_event
from temp_exposure a
left join (select dim16,article_id from ga_click_event where type='click') b on a.user_id=b.dim16 and a.article_id=b.article_id
left join (select dim16,article_id from ga_click_event where type='event') c on a.user_id=c.dim16 and a.article_id=c.article_id;

drop table bi_test.zyl_tmp_200907_0;
create table bi_test.zyl_tmp_200907_0 as
select a.id,
a.creation_date,
a.user_id,
a.device_id,
'' remark,
a.type,
'' remark2,
a.remark3,
a.version,
a.article_id,
a.remark4,
a.pid,
b.is_click,
b.is_event,
'2020-09-04' dt
from temp_exposure a
left join temp_click_event b on a.id=b.id;

drop table bi_test.zyl_tmp_200907_1;
create table bi_test.zyl_tmp_200907_1 as
select * from (
select a.id,
a.creation_date,
a.user_id,
a.device_id,
a.remark,
a.type,
a.remark2,
a.remark3,
a.version,
a.article_id,
a.remark4,
a.pid,
a.is_click,
a.is_event,
a.dt,
row_number()over(partition by id order by creation_date) r
from bi_test.zyl_tmp_200907_0 a) a
where r=1;



cache table tmp as
select id,user_id,if(u_article_orders>0,1,0) is_order
from(
select id,
user_id,
sum(orders)over(partition by user_id,article_id order by creation_date range between current row and 259200 FOLLOWING) u_article_orders
from(
select a.id,unix_timestamp(a.creation_date) creation_date,a.user_id,a.article_id,0 orders
from bi_test.zyl_tmp_200907_1 a
union all
select 'smzdm' id,unix_timestamp(a.order_time) creation_date,substr(a.smzdm_id,1,10) suserid,a.article_id,nvl(a.sales,0) orders
from dwa.dwa_gmv_order a
where a.dt>='2020-04' and a.smzdm_id>'0' and a.article_id>'0' and a.status=1 and a.flag=1) a) a
where id<>'smzdm';


--模型打分，最终打分，pid，user_id, create_time ,article_id，是否被点击            按照 user_id ,pid, 最终打分 排序     按照 最终打分降序      先一天数据吧
create table bi_test.zyl_tmp_200907_6 as
select b.fstModelScore,b.score,a.pid,a.user_id,a.creation_date,a.article_id,a.is_event
from bi_test.zyl_tmp_200907_1 a
inner join bi_test.zyl_tmp_200907_4 b on a.id=b.id;

create table bi_test.zyl_tmp_200907_5 as
select b.fstModelScore,b.score,a.pid,a.user_id,a.creation_date,a.article_id,a.is_click,a.is_event
from bi_test.zyl_tmp_200907_1 a
inner join bi_test.zyl_tmp_200907_4 b on a.id=b.id;

create table bi_test.zyl_tmp_200907_7 as
select b.fstModelScore,b.score,a.pid,a.user_id,a.creation_date,a.article_id,a.is_click,a.is_event,c.is_order,b.modelScoreMap
from bi_test.zyl_tmp_200907_1 a
inner join bi_test.zyl_tmp_200907_4 b on a.id=b.id
left join tmp c on a.id=c.id;

--------------
CREATE TABLE bi_test.`zyl_tmp_200929_1`(
  `json_data` string COMMENT 'json')
location '/bi/ori_log/server_feature_log_pc/2020/09/29'
;

create table bi_test.zyl_tmp_200929_2 as
select d.ab_test,d.pid,d.user_id,d.device_id,d.timestamp,d.pre_module_size,datas_item.article_id,datas_item.channel_id,datas_item.featureMap
from (
select b.ab_test,b.device_id,b.pid,b.timestamp,b.user_id,b.pre_module_size,split(regexp_replace(regexp_extract(b.datas,'^\\\[(.+)\\\]$',1),'\\\}\\\,\\\{', '\\\}\\\|\\\|\\\{'),'\\\|\\\|') as str
from bi_test.`zyl_tmp_200929_1` a
lateral view json_tuple(a.json_data,'device_id','pid','user_id','datas','ab_test','timestamp','pre_module_size') b as device_id,pid,user_id, datas,ab_test,timestamp,pre_module_size) d
lateral view explode(d.str) c as col
lateral view json_tuple(c.col,'article_id','channel_id','featureMap') datas_item as article_id,channel_id,featureMap;
