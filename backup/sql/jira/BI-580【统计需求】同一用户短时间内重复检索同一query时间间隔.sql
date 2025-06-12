create table bi_test.zyl_tmp_190604_1 as
select device_id,s,log_time
from(
select a.log_time,a.device_id,a.session_id,b.s,row_number()over(partition by a.device_id,a.session_id,b.s order by a.log_time) r
from bi_ods_ga.ods_search_log a
lateral view json_tuple(a.extra,'s','f','version','c') b as s,f,version,c
where a.dt between '2019-05-18' and '2019-05-24' and b.f in('iphone','android') and b.version regexp '^8.7|^8.8|^8.9|^9.' and b.c='new_home'
and b.s is not null and a.session_id is not null and a.session_id<>'' and a.device_id is not null and a.device_id<>'') a
where r=1;

create table bi_test.zyl_tmp_190604_2 as
select device_id,s,log_time
from(
select a.log_time,a.device_id,a.session_id,b.s,row_number()over(partition by a.device_id,a.session_id,b.s order by a.log_time) r
from bi_ods_ga.ods_search_log a
lateral view json_tuple(a.extra,'s','f','version','c') b as s,f,version,c
where a.dt between '2019-05-27' and '2019-06-02' and b.f in('iphone','android') and b.version regexp '^8.7|^8.8|^8.9|^9.' and b.c='new_home'
and b.s is not null and a.session_id is not null and a.session_id<>'' and a.device_id is not null and a.device_id<>'') a
where r=1;

create table bi_test.zyl_tmp_190604_3 as
select a.device_id,a.s,unix_timestamp(a.log_time) log_time
from(
select device_id,s,log_time
from bi_test.zyl_tmp_190604_1) a
inner join(
select device_id,s
from bi_test.zyl_tmp_190604_1
group by device_id,s
having(count(*)>1)) b on a.device_id=b.device_id and a.s=b.s;

create table bi_test.zyl_tmp_190604_4 as
select a.device_id,a.s,unix_timestamp(a.log_time) log_time
from(
select device_id,s,log_time
from bi_test.zyl_tmp_190604_2) a
inner join(
select device_id,s
from bi_test.zyl_tmp_190604_2
group by device_id,s
having(count(*)>1)) b on a.device_id=b.device_id and a.s=b.s;


create table bi_test.zyl_tmp_190604_5 as
select *
from(
select device_id,
s,
row_number()over(partition by device_id,s order by log_time)-1 r,
log_time-LAG(log_time,1)over(partition by device_id,s order by log_time) t
from bi_test.zyl_tmp_190604_3) a
where t is not null;

create table bi_test.zyl_tmp_190604_6 as
select *
from(
select device_id,
s,
row_number()over(partition by device_id,s order by log_time)-1 r,
log_time-LAG(log_time,1)over(partition by device_id,s order by log_time) t
from bi_test.zyl_tmp_190604_4) a
where t is not null;

create table bi_test.zyl_tmp_190604_7 as
select device_id,s,cast(avg(t) as int) t
from bi_test.zyl_tmp_190604_5
group by device_id,s;

create table bi_test.zyl_tmp_190604_8 as
select device_id,s,cast(avg(t) as int) t
from bi_test.zyl_tmp_190604_6
group by device_id,s;

create table bi_test.zyl_tmp_190604_9 as
select s,cast(avg(t) as int) t
from bi_test.zyl_tmp_190604_5
group by s;

create table bi_test.zyl_tmp_190604_10 as
select s,cast(avg(t) as int) t
from bi_test.zyl_tmp_190604_6
group by s;


insert overwrite local directory '/data/tmp/zhaoyulong/data' row format delimited fields terminated by '\t'
select * from bi_test.zyl_tmp_190604_5 order by device_id;

insert overwrite local directory '/data/tmp/zhaoyulong/data' row format delimited fields terminated by '\t'
select * from bi_test.zyl_tmp_190604_6 order by device_id;

insert overwrite local directory '/data/tmp/zhaoyulong/data' row format delimited fields terminated by '\t'
select * from bi_test.zyl_tmp_190604_7 order by device_id;

insert overwrite local directory '/data/tmp/zhaoyulong/data' row format delimited fields terminated by '\t'
select * from bi_test.zyl_tmp_190604_8 order by device_id;

insert overwrite local directory '/data/tmp/zhaoyulong/data' row format delimited fields terminated by '\t'
select * from bi_test.zyl_tmp_190604_9 order by s;

insert overwrite local directory '/data/tmp/zhaoyulong/data' row format delimited fields terminated by '\t'
select * from bi_test.zyl_tmp_190604_10 order by s;
