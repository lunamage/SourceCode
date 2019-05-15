select printf("%.6f",0.004833);

HIVE Skewed Table

stack
translate
trunc
xpath
variance
approx_count_distinct
bin
bround
ceil
floor
coalesce
conv
format_number
inline
instr
isnan
isnotnull
isnull
levenshtein

GROUPING SETS
with cube
with rollup

CUME_DIST 小于等于当前值的行数/分组内总行数
PERCENT_RANK 分组内当前行的RANK值-1/分组内总行数-1

elt（n，str1，str2，...） - 返回第n个字符串

hive中对多个数值进行取值,即sql中的对行取最大值或者最小值函数,
greatest(2,3,4),
least(1,2,3)

Skewed Tables
该特性为了优化表中一列或几列有数据倾斜的值。

CREATE TABLE list_bucket_single (key STRING, value STRING)
  SKEWED BY (key) ON (1,5,6) [STORED AS DIRECTORIES];

CREATE TABLE list_bucket_multiple (col1 STRING, col2 int, col3 STRING)
  SKEWED BY (col1, col2) ON (('s1',1), ('s3',3), ('s13',13), ('s78',78)) [STORED AS DIRECTORIES];



analyze table bi_dw_ga.dim_query_base64 partition(dt='2019-03-01') compute statistics;

analyze table stg.db_wiki_product_b2c partition(dt='2019-03-31') compute statistics;

shell 求出现次数


CREATE TABLE `t_article_data` (
  `id` BIGINT(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
  `itemId` BIGINT(20) DEFAULT NULL COMMENT '文章ID',
  `channel` SMALLINT(5) DEFAULT NULL COMMENT '文章频道',
  `channelId` SMALLINT(5) DEFAULT NULL COMMENT '文章详细子频道(如好价的1,2,5,21)',
  `scene` VARCHAR(50) DEFAULT NULL COMMENT '分发场景',
  `enterTime` DATETIME DEFAULT NULL COMMENT '进入池子时间',
  `quitTime` DATETIME DEFAULT NULL COMMENT '退出池子时间',
  PRIMARY KEY (`id`)
) ENGINE=INNODB DEFAULT CHARSET=utf8;


CREATE TABLE `t_stream_data_statics` (
  `id` BIGINT(20) NOT NULL AUTO_INCREMENT COMMENT '主键id',
  `stream` VARCHAR(5) CHARACTER SET latin1 DEFAULT NULL COMMENT '流量',
  `num` BIGINT(11) DEFAULT NULL COMMENT '数量',
  `staticsTime` DATETIME DEFAULT NULL COMMENT '统计时间',
  PRIMARY KEY (`id`)
) ENGINE=INNODB DEFAULT CHARSET=utf8;

select count(*) from t_article_data where greatest(enterTime,'2019-04-03 00:00:00')<least(quitTime,'2019-04-03 23:59:59');

{"imei":"123","aid":"com.smzdm.client.ios","lng":"117.1219331955941","sid":"15535788371060","uid":"234891237","tid":"UA-1000000-1","an":"smzdm","ca":"0","sr":"1125x2436","uuid":"39BEE7FF-AC4E-4048-9CDD-124676B0B73E","ecp":"{\"17\":\"a\",\"13\":\"a\",\"21\":\"首页_推荐_feed流_a_首页强制置顶\",\"20\":\"3\",\"11\":\"youhui\"}","idfa":"1MgqjeT41eINeS+IAsBZE+kJt5H9fZBdkvcqjYaDTdSfofPO6MzmDg==","type":"screenview","ift":"0","st":"iPhone","sn":"iPhone\/3\/12967582\/","v":"1.10","isnp":"1","ip":"117.50.12.220","sv":"1.1.0","dt":"1","ds":"app","ch":"AppStore","slt":1553579109,"av":"9.3.15","it":"1553579081445","cid":"VkF\/KKe4KaP\/oEDFDjcvOpRVnmK8FdfiyHmZNngjnTL1CSKIgp9s7g==.1552356749870","lat":"39.09510244436872","dm":"iPhone10,3","did":"VkF\/KKe4KaP\/oEDFDjcvOpRVnmK8FdfiyHmZNngjnTL1CSKIgp9s7g==","hnb":"54","os":"12.1","nt":"wifi","idfv":"eUJ9EhWsh\/SvKKbrkXrSYrHZhS0mpnkOgomb9kJfDgMkwUIWzBkUpg==","ec":"01"}


map
flatmap
filter
keyby
reduce
fold
aggregations
window
windowall
union
window join
split
select
project

transformations

hbase
solr

insert overwrite local directory '/data/tmp/zhaoyulong/data' row format delimited fields terminated by '\t'
select a.orderdate,a.mall,b.small,c.id,a.total_order,a.a_u_order
from(
select orderdate,mall,
sum(sales) total_order,
sum(if(a.suserid>'0' and a.sarticleid>'0',sales,0)) a_u_order
from bi_dw_gmv.dw_t_order a
where a.dt in('2019-02','2019-03') and orderdate between '2019-02-27' and '2019-03-08' and a.status=1 and a.flag=1
group by orderdate,mall) a
left join bi_stg_gmv.t_mall_map b on a.mall=b.mall
inner join (select id,name_cn from stg.db_smzdm_smzdm_mall where dt='2019-04-01' and is_deleted=0) c on nvl(b.small,a.mall)=c.name_cn;


insert overwrite local directory '/data/tmp/zhaoyulong/data' row format delimited fields terminated by '\t'
select a.dt,a.tv,b.p,count(*) sl,count(distinct a.did) sl2
from (select *
from bi_ods_ga.ods_pc_sdk_log where dt between '2019-04-01' and '2019-04-02' and ec='01' and ea='01') a
left join
(select * from bi_ods_ga.ods_pc_sdk_log_ecp where dt between '2019-04-01' and '2019-04-02') b on a.id=b.id and a.dt=b.dt
group by a.dt,a.tv,b.p order by dt,tv,p;




select * from bi_ods_ga.ods_pc_sdk_log where dt='2019-04-02' limit 20;



sh /data/source/data_warehouse/ga/script/pub/exec_sparksql2_special_1.sh ods_pc_sdk_log 4 10 30 /data/source/data_warehouse/ga/script/ods/ ods_pc_sdk_log ${tx_date}


CREATE EXTERNAL TABLE `bi_app_ga.app_ga_search_keyword_ctr`(
  `stat_dt` string,
  `rank` string COMMENT '排名',
  `rank_1` string COMMENT '名次变化',
  `query` string COMMENT '搜索词',
  `search_query_count` string COMMENT '检索量',
  `search_query_click` string COMMENT '点击量',
  `ctr` string,
  `bl` string COMMENT 'CTR变化率')
PARTITIONED BY ( `dt` string)
LOCATION  'hdfs://cluster/bi/app_ga/app_ga_search_keyword_ctr';

sh /data/source/data_warehouse/ga/script/pub/exec_sparksql2.sh app_ga_search_keyword_ctr 4 20 10 /data/source/data_warehouse/ga/script/app/search/ app_ga_search_keyword_ctr 2019-04-01
sh /data/source/data_warehouse/ga/script/pub/exec_sqoop_export_delete.sh 10.9.28.135:3320/app wdDBUser '2Gb(tv+-n' app_ga_search_keyword_ctr '/bi/app_ga/app_ga_search_keyword_ctr/dt=' "delete from app_ga_search_keyword_ctr where stat_dt=" 2019-04-01

ALTER TABLE bi_app_ga.app_ga_cms_feed_statistics DROP IF EXISTS PARTITION (dt='2019-04-02');
ALTER TABLE bi_app_ga.app_ga_cms_feed_article_statistics DROP IF EXISTS PARTITION (dt='2019-04-02');

e.duration,
e.complete,
f.cms_details_uv,

alter table bi_app_ga.app_ga_cms_feed_statistics add columns(duration bigint COMMENT '社区所有详情页阅读时长之和');
alter table bi_app_ga.app_ga_cms_feed_statistics add columns(complete decimal(18,4) COMMENT '社区所有有效阅读完成率之和');
alter table bi_app_ga.app_ga_cms_feed_statistics add columns(cms_details_uv bigint COMMENT '有过详情页的点击的UV');

alter table bi_app_ga.app_ga_cms_feed_article_statistics add columns(duration bigint COMMENT '社区所有详情页阅读时长之和');
alter table bi_app_ga.app_ga_cms_feed_article_statistics add columns(complete decimal(18,4) COMMENT '社区所有有效阅读完成率之和');
alter table bi_app_ga.app_ga_cms_feed_article_statistics add columns(cms_details_uv bigint COMMENT '有过详情页的点击的UV');


sh /data/source/data_warehouse/ga/script/pub/exec_sparksql2.sh app_ga_cms_feed_statistics 4 10 30 /data/source/data_warehouse/ga/script/app/recommend/ app_ga_cms_feed_statistics

dfs -rm -r /bi/app_ga/app_ga_cms_feed_statistics/dt=2019-03-30;
dfs -rm -r /bi/app_ga/app_ga_cms_feed_article_statistics/dt=2019-03-30;
ALTER TABLE bi_app_ga.app_ga_cms_feed_statistics DROP IF EXISTS PARTITION (dt='2019-03-30');
ALTER TABLE bi_app_ga.app_ga_cms_feed_article_statistics DROP IF EXISTS PARTITION (dt='2019-03-30');

ALTER TABLE app_ga_cms_feed_statistics ADD duration bigint(20) COMMENT '社区所有详情页阅读时长之和';
ALTER TABLE app_ga_cms_feed_statistics ADD complete decimal(18,4) COMMENT '社区所有有效阅读完成率之和';
ALTER TABLE app_ga_cms_feed_statistics ADD cms_details_uv bigint(20) COMMENT '有过详情页的点击的UV';

ALTER TABLE app_ga_cms_feed_article_statistics ADD duration bigint(20) COMMENT '此文章详情页阅读时长之和';
ALTER TABLE app_ga_cms_feed_article_statistics ADD complete decimal(18,4) COMMENT '此文章有效阅读完成率之和';
ALTER TABLE app_ga_cms_feed_article_statistics ADD cms_details_uv bigint(20) COMMENT '此文章UV';

alter table app_ga_cms_feed_statistics drop column duration;
alter table app_ga_cms_feed_statistics drop column complete;
alter table app_ga_cms_feed_statistics drop column cms_details_uv;

sqoop export -D mapred.job.queue.name=bi --connect "jdbc:mysql://10.9.28.135:3320/app?useUnicode=true&characterEncoding=utf-8" --username wdDBUser --password '2Gb(tv+-n' --table app_ga_cms_feed_statistics --fields-terminated-by '\001' --input-null-string '\\N' --input-null-non-string '\\N' --export-dir /bi/app_ga/app_ga_cms_feed_statistics/dt=2019-03*/*
