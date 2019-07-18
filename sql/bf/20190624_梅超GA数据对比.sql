create table bi_test.zyl_tmp_190716_1 as
select dt,hits_hour,
sum(if(hits_type='PAGE',1,0)) web_pv,
sum(if(hits_type='APPVIEW',1,0)) app_pv,
sum(if(hits_type='EVENT',1,0)) event_count,
count(distinct fullvisitorid) uv
from bi_ods_ga.ods_ga_hits_data_v1
where dt between '2019-06-12' and '2019-07-15'
group by dt,hits_hour order by dt,cast(hits_hour as int);

create table bi_test.zyl_tmp_190716_2 as
select dt,hits_hour,
sum(if(hits_type='PAGE',1,0)) web_pv,
sum(if(hits_type='APPVIEW',1,0)) app_pv,
sum(if(hits_type='EVENT',1,0)) event_count,
count(distinct fullvisitorid) uv
from bi_ods_ga.ods_ga_hits_data
where dt between '2019-06-12' and '2019-07-15'
group by dt,hits_hour order by dt,cast(hits_hour as int);


insert overwrite local directory '/data/tmp/zhaoyulong/data' row format delimited fields terminated by '\t'
select * from bi_test.zyl_tmp_190716_1 order by dt,cast(hits_hour as int);

insert overwrite local directory '/data/tmp/zhaoyulong/data' row format delimited fields terminated by '\t'
select * from bi_test.zyl_tmp_190716_2 order by dt,cast(hits_hour as int);
