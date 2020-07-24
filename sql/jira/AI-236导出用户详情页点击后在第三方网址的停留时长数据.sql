--sec_creation_date：产生二跳的时间
--user_id
--device_id
--channel
--channel_id
--mall
--mallid
--user_level：用户等级
--regist_time
--price
--level1
--level2
--level3
--level4
--level1_id
--level2_id
--level3_id
--level4_id
--articleid
--jump_time：第三方网站浏览时长（秒）统计口径：辛苦@王爱国支持提供，关联需求：https://jira-team.smzdm.com/browse/APP-97952
--if_order：是否下订单（1/0）
--gmv：在第三方网站产生的GMV（元）
--cps：在第三方网站产生的CPS（元）
--【统计时间范围】：2020-07-01 ～ 2020-07-21

cache table tmp as
select a.it,
a.uid,
a.did,
cd11 channel,
case cd11 when 'youhui' then '1' when 'faxian' then '2' else '5' end channel_id,
cd4 id
from bi_ods_ga.ods_app_sdk_log a
lateral view json_tuple(a.ecp,'11','4') b as cd11,cd4
where a.dt='${tx_date}' and a.it between '${tx_date}' and concat('${tx_date}',' 23:59:59') and a.uid<>'' and a.uid<>'0'
and a.ec='增强型电子商务' and a.ea='添加到购物车' and cd11 in('youhui','faxian','haitao');

cache table tmp_user as
select a.smzdm_id,a.regist_time,b.user_level
from (select user_id,smzdm_id,creation_date regist_time from stg.db_udc_account where dt=date_sub(current_date,1)) a
left join(select user_id,floor(sqrt(exp/20+4)-2) user_level from stg.db_udc_point where dt=date_sub(current_date,1)) b on a.user_id=b.user_id;

cache table tmp_article as
SELECT id,
mall_id,
mall,
price,
cate_level1_id,
cate_level1,
cate_level2_id,
cate_level2,
cate_level3_id,
cate_level3,
cate_level4_id,
cate_level4
from bi_dw_ga.dim_article_info
where channel_id in('1','2','5');


cache table tmp_gmv as
select substr(a.smzdm_id,1,10) suserid,
a.article_id,
if(sum(sales)>0,1,0) sales,
sum(gmv) gmv,
sum(cps) cps
from dwa.dwa_gmv_order a
where a.dt>='2020-07' and a.smzdm_id>'0' and a.article_id>'0' and a.status=1 and a.flag=1
and a.order_time between '${tx_date}' and date_add('${tx_date}',3)
group by substr(a.smzdm_id,1,10),a.article_id;

cache table tmp2 as
select a.uid,
a.did,
cd4 id,
sum(nvl(cast(a.el as int),0)) jump_time
from bi_ods_ga.ods_app_sdk_log a
lateral view json_tuple(a.ecp,'4') b as cd4
where a.dt='${tx_date}' and a.it between '${tx_date}' and concat('${tx_date}',' 23:59:59') and a.uid<>'' and a.uid<>'0'
and a.ec='电商点击后跳出' and a.ea='离开时长'
group by a.uid,
a.did,
cd4;

insert overwrite table bi_test.zyl_tmp_ai236 partition(dt)
select
a.it sec_creation_date,
a.uid user_id,
a.did device_id,
a.channel,
a.channel_id,
c.mall,
c.mall_id mallid,
b.user_level,
b.regist_time,
c.price,
c.cate_level1 level1,
c.cate_level2 level2,
c.cate_level3 level3,
c.cate_level4 level4,
c.cate_level1_id level1_id,
c.cate_level2_idlevel2_id,
c.cate_level3_idlevel3_id,
c.cate_level4_idlevel4_id,
a.id articleid,
e.jump_time,
d.sales if_order,
d.gmv,
d.cps,
'${tx_date}' dt
from tmp a
left join tmp_user b on a.uid=b.smzdm_id
left join tmp_article c on a.id=c.id
left join tmp_gmv d on a.uid=d.suserid and a.id=d.article_id
left join tmp2 e on a.uid=e.uid and a.did=e.did;
