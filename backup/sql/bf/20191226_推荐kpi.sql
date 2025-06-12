

create table bi_test.zyl_tmp_191223_1 as
select to_date(order_time) d,count(distinct smzdm_id) uv
from dwa.dwa_gmv_order
where split(mid_id,'_')[0] regexp 'T'
and length(split(mid_id,'_')[0])<5
and flag=1
and status=1
and dt between '2017-01' and '2019-12'
and to_date(order_time) between '2017-01-01' and '2019-12-22'
group by to_date(order_time) order by d;

create table bi_test.zyl_tmp_191223_2 as
select to_date(order_time) d,count(distinct smzdm_id) uv
from dwa.dwa_gmv_order
where  flag=1
and status=1
and dt between '2017-01' and '2019-12'
and to_date(order_time) between '2017-01-01' and '2019-12-22'
group by to_date(order_time) order by d;


insert overwrite local directory '/data/tmp/zhaoyulong/data' row format delimited fields terminated by '\t'
select * from bi_test.zyl_tmp_191223_2 order by d;



create table bi_test.zyl_tmp_rec_click_1 as
select a.dt,count(distinct fullvisitorid) uv
from bi_dw_ga.fact_ga_hits_data a
where a.dt between '2017-01-01' and '2017-03-31' and (hits_appinfo_appid='com.smzdm.client.android' or hits_appinfo_appid='com.smzdm.client.ios')
and a.hits_eventinfo_eventcategory ='首页' and a.hits_eventinfo_eventaction='首页站内文章点击' and a.hits_eventinfo_eventlabel regexp '^推荐' and a.hits_appinfo_appversion regexp '^8.(7|8|9)|^9.'
group by a.dt;

create table bi_test.zyl_tmp_rec_event_1 as
select a.dt,count(distinct fullvisitorid) uv
from bi_dw_ga.fact_ga_hits_data a
where a.dt between '2017-01-01' and '2017-03-31' and (hits_appinfo_appid='com.smzdm.client.android' or hits_appinfo_appid='com.smzdm.client.ios')
and a.hits_ecommerceaction_action_type='3'
and a.hits_eventinfo_eventaction='添加到购物车' and a.dim64 regexp '首页_推荐_feed流' and a.hits_appinfo_appversion regexp '^8.(7|8|9)|^9.'
group by a.dt;

create table bi_test.zyl_tmp_all_click_1 as
select a.dt,count(distinct fullvisitorid) uv
from bi_dw_ga.fact_ga_hits_data a
where a.dt between '2017-01-01' and '2017-03-31' and a.hits_appinfo_appid in('com.smzdm.client.android','com.smzdm.client.ios')
and a.hits_appinfo_screenname regexp '(/P/)|(/[0-9]+[0-9]+(/)?$)' and a.hits_appinfo_screenname not regexp '首页' and a.hits_appinfo_appversion regexp '^8.(7|8|9)|^9.' and a.hits_type='APPVIEW'
group by a.dt;

create table bi_test.zyl_tmp_all_event_1 as
select a.dt,count(distinct fullvisitorid) uv
from bi_dw_ga.fact_ga_hits_data a
where a.dt between '2017-01-01' and '2017-03-31' and a.hits_appinfo_appid in('com.smzdm.client.android','com.smzdm.client.ios')
and a.hits_ecommerceaction_action_type='3'
and a.hits_eventinfo_eventaction='添加到购物车' and a.hits_appinfo_appversion regexp '^8.(7|8|9)|^9.'
group by a.dt;



sh /data/source/data_warehouse/ga/script/pub/exec_sparksql2.sh bushu 4 10 40 /data/source/data_warehouse/ga/script/tmp/ bushu1
sh /data/source/data_warehouse/ga/script/pub/exec_sparksql2.sh bushu 4 10 40 /data/source/data_warehouse/ga/script/tmp/ bushu2
sh /data/source/data_warehouse/ga/script/pub/exec_sparksql2.sh bushu 4 10 40 /data/source/data_warehouse/ga/script/tmp/ bushu3

insert overwrite local directory '/data/tmp/zhaoyulong/data' row format delimited fields terminated by '\t'
select * from(
select dt,uv from zyl_tmp_all_event_1
union all
select dt,uv from zyl_tmp_all_event_2
union all
select dt,uv from zyl_tmp_all_event_3
union all
select dt,uv from zyl_tmp_all_event_4
union all
select dt,uv from zyl_tmp_all_event_5
union all
select dt,uv from zyl_tmp_all_event_6
union all
select dt,uv from zyl_tmp_all_event_7
union all
select dt,uv from zyl_tmp_all_event_8
union all
select dt,uv from zyl_tmp_all_event_9
union all
select dt,uv from zyl_tmp_all_event_10
union all
select dt,uv from zyl_tmp_all_event_11
union all
select dt,uv from zyl_tmp_all_event_12) a
order by dt;
