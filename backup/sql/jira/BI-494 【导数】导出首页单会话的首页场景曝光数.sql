https://jira-team.smzdm.com/browse/BI-406

create table bi_test.zyl_tmp_190505_2 as
select dt,sl,count(*) session_count
from(
select a.dt,a.sid,count(*) sl
from bi_ods_ga.ods_app_sdk_log a
lateral view json_tuple(a.ecp,'tv','ctp','sp','a','c') b as tv,ctp,sp,a,c
where a.dt between '2019-04-21' and '2019-05-04' and a.ec='01' and a.ea='01' and b.sp='0'
and a.av regexp '^9.' and b.tv='q'
group by a.dt,a.sid) a
group by dt,sl;

insert overwrite local directory '/data/tmp/zhaoyulong/data' row format delimited fields terminated by '\t'
select * from bi_test.zyl_tmp_190505_2;
