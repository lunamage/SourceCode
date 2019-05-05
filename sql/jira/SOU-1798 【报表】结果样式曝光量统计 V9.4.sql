--https://jira-team.smzdm.com/browse/SOU-1798

/*表1
日期
指标项：slt，每个自然天为一个统计单位
样式
指标项：ecp中的srm，枚举所有样式
过滤条件：
  t = 'show'
  and ds = 'app'
  and av REGEXP '^9.[4-9]'
  and ec = '04'
  and srm not regexp '^通用_'
  and spp '_综合$'
曝光量
指标项：ea事件数量，符合“样式”过滤条件的事件数量
唯一ssid数
指标项：ecp中的ssid唯一数量，符合“样式”过滤条件的事件中 ssid排重后的数量
唯一did数
指标项：did唯一数量，符合“样式”过滤条件的事件中 did排重后的数量 */

select b.srm,count(*) sl1,count(distinct b.ssid) sl2,count(distinct a.did) sl3
from bi_ods_ga.ods_app_sdk_log a
lateral view json_tuple(a.ecp,'srm','spp','ssid') b as srm,spp,ssid
where a.dt='2019-05-04' and a.ec='04' and a.av regexp '^9.[4-9]'
and a.it between '2019-05-04' and concat('2019-05-04',' 23:59:59')
and b.srm not regexp '^通用_' and b.spp regexp '综合'
group by b.srm;


CREATE EXTERNAL TABLE `app_sdk_search_style`(
  `stat_dt` date COMMENT '日期',
  `srm` string COMMENT '样式',
  `exposure_count` bigint COMMENT '曝光量',
  `ssid_count` bigint COMMENT '唯一ssid数',
  `did_count` bigint COMMENT '唯一did数')
PARTITIONED BY ( `dt` string)
LOCATION 'hdfs://cluster/bi/app_ga/app_sdk_search_style';

CREATE TABLE `app_sdk_search_style` (
  `stat_dt` date COMMENT '日期',
  `srm` varchar(5000) COMMENT '样式',
  `exposure_count` bigint(20) COMMENT '曝光量',
  `ssid_count` bigint(20) COMMENT '唯一ssid数',
  `did_count` bigint(20) COMMENT '唯一did数',
  KEY `idx_app_sdk_search_style01` (`stat_dt`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


sh /data/source/data_warehouse/ga/script/pub/exec_sparksql2.sh app_sdk_search 4 10 30 /data/source/data_warehouse/ga/script/app/search/ app_sdk_search 2019-05-03
sh /data/source/data_warehouse/ga/script/pub/exec_sqoop_export_delete.sh 10.9.28.135:3320/app wdDBUser '2Gb(tv+-n' app_sdk_search_style '/bi/app_ga/app_sdk_search_style/dt=' "delete from app_sdk_search_style where stat_dt=" 2019-05-03

sh /data/source/data_warehouse/ga/script/pub/exec_sparksql2.sh app_sdk_search 4 10 30 /data/source/data_warehouse/ga/script/app/search/ app_sdk_search 2019-05-02
sh /data/source/data_warehouse/ga/script/pub/exec_sqoop_export_delete.sh 10.9.28.135:3320/app wdDBUser '2Gb(tv+-n' app_sdk_search_style '/bi/app_ga/app_sdk_search_style/dt=' "delete from app_sdk_search_style where stat_dt=" 2019-05-02

sh /data/source/data_warehouse/ga/script/pub/exec_sparksql2.sh app_sdk_search 4 10 30 /data/source/data_warehouse/ga/script/app/search/ app_sdk_search 2019-05-01
sh /data/source/data_warehouse/ga/script/pub/exec_sqoop_export_delete.sh 10.9.28.135:3320/app wdDBUser '2Gb(tv+-n' app_sdk_search_style '/bi/app_ga/app_sdk_search_style/dt=' "delete from app_sdk_search_style where stat_dt=" 2019-05-01

sh /data/source/data_warehouse/ga/script/pub/exec_sparksql2.sh app_sdk_search 4 10 30 /data/source/data_warehouse/ga/script/app/search/ app_sdk_search 2019-04-30
sh /data/source/data_warehouse/ga/script/pub/exec_sqoop_export_delete.sh 10.9.28.135:3320/app wdDBUser '2Gb(tv+-n' app_sdk_search_style '/bi/app_ga/app_sdk_search_style/dt=' "delete from app_sdk_search_style where stat_dt=" 2019-04-30

sh /data/source/data_warehouse/ga/script/pub/exec_sparksql2.sh app_sdk_search 4 10 30 /data/source/data_warehouse/ga/script/app/search/ app_sdk_search 2019-04-29
sh /data/source/data_warehouse/ga/script/pub/exec_sqoop_export_delete.sh 10.9.28.135:3320/app wdDBUser '2Gb(tv+-n' app_sdk_search_style '/bi/app_ga/app_sdk_search_style/dt=' "delete from app_sdk_search_style where stat_dt=" 2019-04-29

sh /data/source/data_warehouse/ga/script/pub/exec_sparksql2.sh app_sdk_search 4 10 30 /data/source/data_warehouse/ga/script/app/search/ app_sdk_search 2019-04-28
sh /data/source/data_warehouse/ga/script/pub/exec_sqoop_export_delete.sh 10.9.28.135:3320/app wdDBUser '2Gb(tv+-n' app_sdk_search_style '/bi/app_ga/app_sdk_search_style/dt=' "delete from app_sdk_search_style where stat_dt=" 2019-04-28
