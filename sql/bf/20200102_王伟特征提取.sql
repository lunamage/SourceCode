create table bi_test.zyl_tmp_200103_1 as
select
a.dt,
a.log_time,
a.user_id,
a.article_id,
b.channelid,
a.ab_test,
b.price,
b.level1,
b.level2,
b.level3,
b.level4,
a.f,
a.calscore,
a.conversionpercent
from bi_log.feature_log_extract a
lateral view json_tuple(a.featuremap,'channelid','price','level1','level2','level3','level4') b
as channelid,price,level1,level2,level3,level4
where a.dt between '2019-12-31' and '2020-01-01';




cache table tmp_pid as
select
split(a.col2,'_')[3] pid
,a.col3 pidtime
,a.col4 pidmod
,a.col2 as pid_list
,a.sarticleid
,a.suserid
,row_number()over(partition by a.col2 order by a.col3) rn
from bi_dw_gmv.dw_zdmdb_link a
where a.dt>='2019-11'
and a.col2 regexp '^mm';


cache table tmp_tm as
select
a.ordernumber
,a.adate
,nvl(c.sarticleid,'') sarticleid
,nvl(c.suserid,'') suserid
,from_unixtime(c.pidtime) pidtime
,row_number()over(partition by a.ordernumber,a.productid order by unix_timestamp(a.adate)-c.pidtime) rn
from bi_stg_gmv.cps_tmallorder a
inner join tmp_pid c
on a.adid=c.pid
and unix_timestamp(a.adate)-c.pidtime > 0
where a.dt=current_date
and a.statusid<>13
and a.adname regexp 'S-'
and adate>='2019-12-01';


create table bi_test.zyl_tmp_200106_1 as
select *
from(
select dt,
creation_date,
user_id,
article_id,
channelid,
ab_test,
price,
level1,
level2,
level3,
level4,
f,
calscore,
conversionpercent,
max(order)over(partition by user_id,article_id order by creation_date range between current row and 259200 FOLLOWING) is_buy
from(
select dt,
cast(log_time as bigint) creation_date,
user_id,
article_id,
channelid,
ab_test,
price,
level1,
level2,
level3,
level4,
f,
calscore,
conversionpercent,
0 order
from bi_test.zyl_tmp_200103_1
union all
select 'smzdm' dt,
unix_timestamp(a.ordertime) creation_date,
substr(a.suserid,1,10) user_id,
a.sarticleid article_id,
'' channelid,
'' ab_test,
'' price,
'' level1,
'' level2,
'' level3,
'' level4,
'' f,
'' calscore,
'' conversionpercent,
1 order
from bi_dw_gmv.dw_t_order a
where a.dt>='2019-12' and a.suserid>'0' and a.sarticleid>'0' and a.status=1 and a.flag=1 and mall<>'淘系'
union all
select 'smzdm' dt,
unix_timestamp(adate)creation_date,
substr(suserid,1,10) user_id,
sarticleid article_id,
'' channelid,
'' ab_test,
'' price,
'' level1,
'' level2,
'' level3,
'' level4,
'' f,
'' calscore,
'' conversionpercent,
1 order
from tmp_tm where rn = 1 and sarticleid<>'' and suserid<> ''
) a) a
where dt<>'smzdm';


-----
create table bi_test.zyl_tmp_2010106 as
select a.dt,
a.creation_date,
a.user_id,
a.article_id,
a.channelid,
a.ab_test,
a.price,
a.level1,
a.level2,
a.level3,
a.level4,
a.f,
a.calscore,
a.conversionpercent,
a.is_buy,
b.mall
from bi_test.zyl_tmp_200106_1 a
left join (select * from bi_dw_ga.dim_article_info where channel_id in('1','2','5')) b on a.article_id=b.id;
