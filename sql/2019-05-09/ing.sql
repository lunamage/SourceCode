关注数：select count(*) from stg.db_dingyue_rule_user_defined where status='1' and creation_time between '2019-05-08' and '2019-05-08 23:59:59';
取消关注数：select count(*) from rule_user_defined where status='2' and modification_time between '2019-05-08' and '2019-05-08 23:59:59';

select to_date(creation_time) date,'gz_rule' type,count(*) sl from stg.db_dingyue_user_follow_log where action_type=1 and user_id not in(339483,2948655,2908329) and type<>'user' group by to_date(creation_time)


select to_date(creation_time) `date`,'gz_rule' type,count(*) sl from stg.db_dingyue_rule_user_defined where dt='2019-05-09' and status='1'



sh /data/source/data_warehouse/ga/script/pub/exec_sparksql2.sh app_recommend_resource_type 4 10 30 /data/source/data_warehouse/ga/script/app/recommend/ app_recommend_resource_type 2019-05-07
sh /data/source/data_warehouse/ga/script/pub/exec_sqoop_export_delete.sh 10.9.28.135:3320/app wdDBUser '2Gb(tv+-n' app_recommend_resource_type '/bi/app_ga/app_recommend_resource_type/dt=' "delete from app_recommend_resource_type where stat_dt=" 2019-05-07



--新增关注
select count(*) sl from stg.db_dingyue_rule_user_defined where dt='${tx_date}' and status='1' and user_id not in(339483,2948655,2908329) and creation_time between '2019-05-09' and '2019-05-09 23:59:59';
--取消关注
select count(*) sl from stg.db_dingyue_rule_user_defined where dt='${tx_date}' and status='2' and user_id not in(339483,2948655,2908329) and modification_time between '2019-05-09' and '2019-05-09 23:59:59';

sh /data/source/data_warehouse/ga/script/pub/exec_sparksql2.sh app_dingyue_core_data 4 20 10 /data/source/data_warehouse/ga/script/app/dingyue/ app_dingyue_core_data 2019-05-09
sh /data/source/data_warehouse/ga/script/pub/exec_sqoop_export_truncate.sh 10.9.28.135:3320/app wdDBUser '2Gb(tv+-n' app_dingyue_core_data '/bi/app_ga/app_dingyue_core_data/*/*' "truncate table app_dingyue_core_data"



select a.date stat_dt,b.app_dingyue_users,b.sendopen_users,b.app_dingyue_sendopen_users,b.gz_yhs,
b.app_dingyue_sendopen_users+b.gz_yhs gz_yh,a.uv,(b.app_dingyue_sendopen_users+b.gz_yhs)/a.uv bl
from(SELECT date,SUM(active_users) uv
FROM dev_ga_data_warehouse.t_fact_youmeng_basedata
WHERE date BETWEEN '2019-05-08' AND '2019-05-09'
AND appkey IN ('5193498156240b3d29009066' , '507cd5cd5270157cfc00001c', '510f7b545270152389000046', '507d0fa95270157cf000005f')
GROUP BY date) a
left join(
select stat_dt,
max(case when type='app_dingyue_users' then value else 0 end) app_dingyue_users,
max(case when type='sendopen_users' then value else 0 end) sendopen_users,
max(case when type='app_dingyue_sendopen_users' then value else 0 end) app_dingyue_sendopen_users,
max(case when type='gz_users' then value else 0 end) gz_yhs
from app.app_ga_dingyue_statistics
where stat_dt between '2019-05-08' and '2019-05-09'
and type in('app_dingyue_users','sendopen_users','app_dingyue_sendopen_users','gz_users')
group by stat_dt) b on a.date=b.stat_dt
order by a.date;

sh /data/source/data_warehouse/ga/script/pub/exec_sparksql2.sh app_dingyue_type 4 10 30 /data/source/data_warehouse/ga/script/app/dingyue/ app_dingyue_type 2019-05-09
sh /data/source/data_warehouse/ga/script/pub/exec_sqoop_export_delete.sh 10.9.28.135:3320/app wdDBUser '2Gb(tv+-n' app_dingyue_type '/bi/app_ga/app_dingyue_type/dt=' "delete from app_dingyue_type where stat_dt=" 2019-05-09
