create table bi_test.zyl_tmp_190429_1 as
select dt,unix_timestamp(a.visitstarttime)+cast(a.hits_time/1000 as int)  visitstarttime,
case when a.hits_eventinfo_eventaction regexp '^结果点击_|快捷攻略点击' then regexp_extract(a.hits_eventinfo_eventlabel,'_([^_]+)',1)
when a.hits_eventinfo_eventaction regexp '聚簇结果点击_|更多结果点击_|特殊结果点击_' then regexp_extract(a.hits_eventinfo_eventlabel,'_([^_]+)_([^_]+)',2) else '无' end article_id
from bi_dw_ga.fact_ga_hits_data a
where a.dt in('2019-04-24','2019-04-20','2018-11-10') and a.hits_appinfo_appid in('com.smzdm.client.android','com.smzdm.client.ios') and a.hits_appinfo_appversion regexp '^8.7|^8.8|^8.9|^9|^4[0-9]{2}'
and a.hits_eventinfo_eventcategory='搜索'
and a.hits_eventinfo_eventaction regexp '结果点击_' and a.hits_eventinfo_eventaction not regexp '结果点击_无_|查看更多_无_|_分类$|_品牌$|_单品$' and a.hits_eventinfo_eventlabel<>'无_无'
and dim13 regexp 'youhui|haitao|faxian';

create table bi_test.zyl_tmp_190429_2 as
select a.dt,a.visitstarttime-unix_timestamp(b.pubdate) t_diff
from bi_test.zyl_tmp_190429_1 a
inner join (select id,pubdate from bi_dw_ga.dim_article_info where channel_id in('1','2','5','21')) b on a.article_id=b.id;


create table bi_test.zyl_tmp_190429_3 as
select dt,
count(*) sl,
sum(if(t_diff>0 and t_diff<=86400,1,0)) t_0_1,
sum(if(t_diff>86400 and t_diff<=259200,1,0)) t_1_3,
sum(if(t_diff>259200 and t_diff<=604800,1,0)) t_3_7,
sum(if(t_diff>604800 and t_diff<=2592000,1,0)) t_7_30,
sum(if(t_diff>2592000 and t_diff<=15552000,1,0)) t_30_180,
sum(if(t_diff>15552000 and t_diff<=31104000,1,0)) t_180_360,
sum(if(t_diff>31104000 and t_diff<=62208000,1,0)) t_360_720,
sum(if(t_diff>62208000,1,0)) t_720
from bi_test.zyl_tmp_190429_2
group by dt;

2019-04-24      947852  385281  208174  109654  118277  78027   16589   10196   7488
2019-04-20      1050545 398336  267811  116894  126470  91526   18494   11016   8021
2018-11-10      3544240 1920856 610649  265650  258234  143167  30332   21915   10234

dev_smzdm_base.t_dbzdm_comment_data

110000000



CREATE TABLE `t_dbzdm_comment_data` (
  `comment_id` bigint(20),
  `channel_id` int(11),
  `article_id` int(11),
  `user_id` int(11),
  `user_smzdm_id` bigint(20) ,
  `display_name` varchar(100),
  `status` tinyint(4),
  `parent_id` varchar(35),
  `parent_ids` text,
  `content` text CHARACTER SET utf8mb4,
  `receive_user_id` int(11) DEFAULT NULL COMMENT '被回复评论的作者ID',
  `ip` varchar(15) DEFAULT NULL COMMENT '简单IP',
  `remote_ip` varchar(15) DEFAULT NULL COMMENT '原始IP',
  `user_agent` varchar(255) DEFAULT NULL COMMENT '用户agent',
  `comment_from` varchar(100) DEFAULT NULL COMMENT '来源格式为“来源@版本”，例如“iphone@6.0”',
  `have_read` tinyint(4),
  `up_num` int(11),
  `down_num` int(11),
  `creation_date` datetime DEFAULT NULL COMMENT '提交时间',
  `modification_date` datetime DEFAULT NULL COMMENT '修改时间',
  `report_latest_time` datetime DEFAULT NULL COMMENT '最后一次被举报的时间',
  `report_count` int(11) DEFAULT '0' COMMENT '被举报的总次数',
  `report_count2` int(11) DEFAULT '0' COMMENT '通过审核后（从0开始计数）被举报次数',
  `sort` float DEFAULT '0' COMMENT '排序字段，越大越靠前显示。默认0',
  hash_value varchar(50),
  load_date datetime,
  KEY `status_comment_id` (comment_id) USING BTREE,
  KEY `status_channel_id` (`channel_id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

create table bi_test.zyl_tmp_190429_4 as
select a.abtest,a.sl,a.user_count,b.sl sl2
from(
select abtest,sl,count(*) user_count
from(
select abtest,smzdm_id,count(distinct category_level3) sl
from bi_dw_ga.fact_recsys_display_data_sdk
where dt='2019-04-29'
group by abtest,smzdm_id) a
group by abtest,sl) a
left join(
select abtest,count(distinct smzdm_id) sl
from bi_dw_ga.fact_recsys_display_data_sdk
where dt='2019-04-29'
group by abtest) b on a.abtest=b.abtest;

---------------------------------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE `bi_app_ga.app_ga_search_core_hour`(
  stat_dt string,
  stat_hour string,
splatform string,
search_user int COMMENT '检索人数',
search_count int COMMENT '检索量',
search_click int COMMENT '点击量',
search_details_click int COMMENT '搜索详情页点击量',
search_details_pv int COMMENT '详情页PV',
search_productAddsToCart int COMMENT '搜索电商点击量',
search_all_productAddsToCart int COMMENT '总电商点击量')
PARTITIONED BY (`dt` string)
LOCATION 'hdfs://cluster/bi/app_ga/app_ga_search_core_hour';



sh /data/source/data_warehouse/ga/script/pub/exec_sparksql2.sh app_ga_search_core_hour 4 10 30 /data/source/data_warehouse/ga/script/app/search/ app_ga_search_core_hour
sh /data/source/data_warehouse/ga/script/pub/exec_sqoop_export_delete.sh 10.9.28.135:3320/app wdDBUser '2Gb(tv+-n' app_ga_search_core_hour '/bi/app_ga/app_ga_search_core_hour/dt=' "delete from app_ga_search_core_hour where stat_dt=" ${tx_date}


CREATE TABLE `app_ga_search_core_hour` (
  stat_dt date,
  stat_hour datetime,
splatform varchar(20),
search_user int(10) COMMENT '检索人数',
search_count int(10) COMMENT '检索量',
search_click int(10) COMMENT '点击量',
search_details_click int(10) COMMENT '搜索详情页点击量',
search_details_pv int(10) COMMENT '详情页PV',
search_productAddsToCart int(10) COMMENT '搜索电商点击量',
search_all_productAddsToCart int(10) COMMENT '总电商点击量',
  KEY `idx_app_ga_search_core_hour01` (`stat_dt`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

--app
select
stat_hour,
search_user,
search_count,
search_click,
search_click/search_count ctr,
search_details_click/search_details_pv bl1,
search_productAddsToCart/search_all_productAddsToCart bl2,
search_productAddsToCart/search_details_click bl3,
search_details_click,
search_details_pv,
search_productAddsToCart,
search_all_productAddsToCart
from app.app_ga_search_core_hour
where splatform='app' and stat_dt='2019-04-29'
order by stat_hour;

--pc
select stat_hour,
search_user,
search_count,
search_click,
search_click/search_count ctr,
search_details_click/search_details_pv bl1,
search_productAddsToCart/search_all_productAddsToCart bl2
from app.app_ga_search_core_hour
where splatform='pc' and stat_dt='2019-04-29'
order by stat_hour;


-----------------------------------------------------------------------------
insert overwrite local directory '/data/tmp/zhaoyulong/data' row format delimited fields terminated by '\t'
SELECT SUBSTRING(a.publishtime, 1, 7) month,a.user_id,a.nickname,b.reg_date,COUNT(a.article_id) tot
FROM bi_dw.fact_haowu_changwen_base a left join fact_user_base b on a.user_id=b.user_id
WHERE a.status IN (1 , 0, 2)
        AND a.article_status IN (1 , 3, 8)
        AND SUBSTRING(a.publishtime, 1, 7) BETWEEN '2017-01' AND '2018-12'
        AND channel_id = 11
        AND a.user_id NOT IN (10595688 , 10525520,
        10497491,
        10239525,
        10188981,
        10162868,
        10160904,
        10158369,
        10006343,
        10006045,
        9945692,
        9945306,
        9945266,
        9934074,
        9934051,
        9934044,
        9043888,
        8994596,
        8556625,
        8425877,
        8392709,
        8327184,
        8144577,
        8110779,
        7999749,
        7998909,
        7998882,
        7998874,
        7998870,
        7994071,
        7977772,
        7977743,
        7977695,
        7977692,
        7977688,
        7977682,
        7977671,
        7977654,
        7894597,
        7695641,
        7624750,
        7606107,
        7592336,
        7296002,
        7197486,
        7192548,
        7076390,
        7076065,
        7022681,
        7016836,
        6836519,
        6308277,
        6032711,
        6032707,
        3403103,
        3340211,
        3267385,
        2842187,
        11408953,
        11162464,
        10826338,
        8313161,
        6670732,
        3340211)
GROUP BY SUBSTRING(a.publishtime, 1, 7),a.user_id,a.nickname,b.reg_date
ORDER BY month,tot desc;
-----------------------------------------------------------------------------------------------
--
create table bi_test.zyl_tmp_190430_1 as
select a.log_time,b.device_id,b.smzdm_id,b.s query
from bi_ods_ga.ods_search_log a
lateral view json_tuple(a.extra,'s','c','order','version','f','category_id','brand_id','mall_id','offset','min_price','max_price','device_id','smzdm_id') b as s,c,`order`,version,f,category_id,brand_id,mall_id,offset,min_price,max_price,device_id,smzdm_id
where a.dt='2019-04-29' and b.s is not null and b.c='new_home' and b.`order` in('score','time')
and b.version regexp '^8.7|^8.8|^8.9|^9.' and a.log_time between '2019-04-29 21:00:00' and '2019-04-29 23:59:59';

--
create table bi_test.zyl_tmp_190430_2 as
select b.device_id,b.smzdm_id,count(*) sl
from bi_ods_ga.ods_search_log a
lateral view json_tuple(a.extra,'s','c','order','version','f','category_id','brand_id','mall_id','offset','min_price','max_price','device_id','smzdm_id') b as s,c,`order`,version,f,category_id,brand_id,mall_id,offset,min_price,max_price,device_id,smzdm_id
where a.dt='2019-04-29' and b.s is not null and b.c='new_home' and b.`order` in('score','time')
and b.version regexp '^8.7|^8.8|^8.9|^9.' and a.log_time between '2019-04-29 21:00:00' and '2019-04-29 23:59:59'
group by b.device_id,b.smzdm_id;

create table bi_test.zyl_tmp_190430_3 as
select b.s query,count(*) sl
from bi_ods_ga.ods_search_log a
lateral view json_tuple(a.extra,'s','c','order','version','f','category_id','brand_id','mall_id','offset','min_price','max_price','device_id','smzdm_id') b as s,c,`order`,version,f,category_id,brand_id,mall_id,offset,min_price,max_price,device_id,smzdm_id
where a.dt='2019-04-29' and b.s is not null and b.c='new_home' and b.`order` in('score','time')
and b.version regexp '^8.7|^8.8|^8.9|^9.' and a.log_time between '2019-04-29 21:00:00' and '2019-04-29 23:59:59'
group by b.s;


insert overwrite local directory '/data/tmp/zhaoyulong/data' row format delimited fields terminated by '\t'
select * from bi_test.zyl_tmp_190430_1 order by log_time;

insert overwrite local directory '/data/tmp/zhaoyulong/data' row format delimited fields terminated by '\t'
select * from bi_test.zyl_tmp_190430_2 order by sl desc;

insert overwrite local directory '/data/tmp/zhaoyulong/data' row format delimited fields terminated by '\t'
select * from bi_test.zyl_tmp_190430_3 order by sl desc;
----------------------------------------------------------------------------------------------------------------------
--总表
select stat_dt,abtest,exposure,click,event,click/exposure ctr,event/click bl,event/exposure bl2
from(
select date_format(stat_dt,'%Y-%m-%d') stat_dt,
abtest,
sum(if(type='exposure',sl,0)) exposure,
sum(if(type='click',sl,0)) click,
sum(if(type='event',sl,0)) event
from app.app_rec_realtime
where stat_dt between CURRENT_DATE() and concat(CURRENT_DATE(),' 23:59:59')
and abtest not in ('无','smzdm','')
group by date_format(stat_dt,'%Y-%m-%d'),abtest) a
order by bl2;

--具体流量明细表
select stat_dt,abtest,exposure,click,event,click/exposure ctr,event/click bl
from(
select stat_dt,
abtest,
sum(if(type='exposure',sl,0)) exposure,
sum(if(type='click',sl,0)) click,
sum(if(type='event',sl,0)) event
from app.app_rec_realtime
where stat_dt between CURRENT_DATE() and concat(CURRENT_DATE(),' 23:59:59') and abtest='a'
group by stat_dt,abtest) a
order by stat_dt;

select count(*),max(load_date) from app.app_rec_realtime;

select avg(sl)
from(
select date_format(load_date,'%Y-%m-%d %H:%i'),count(*) sl
from app.app_rec_realtime group by date_format(load_date,'%Y-%m-%d %H:%i')) a;

select * from app.app_rec_realtime  order by load_date desc limit 100;


select count(*)
from bi_ods_ga.ods_app_sdk_log a
lateral view json_tuple(a.ecp,'a','c','tv','p','sp') b as a,c,tv,p,sp
where a.dt='2019-04-30' and a.ec='01' and a.ea='01' and b.sp='0' and a.it between '2019-04-30 11:00:00' and '2019-04-30 11:59:59' and b.tv='a';


CREATE TABLE `app_rec_realtime_bak` (
  `stat_dt` datetime DEFAULT NULL,
  `abtest` varchar(50) NOT NULL DEFAULT '',
  `exposure` int(10) DEFAULT NULL,
  `click` int(10) DEFAULT NULL,
  `event` int(10) DEFAULT NULL,
  `load_date` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY `idx_stat_type` (`stat_dt`,`abtest`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

insert into app_rec_realtime_bak(stat_dt,abtest,exposure,click,event)
select stat_dt,
abtest,
sum(if(type='exposure',sl,0)) exposure,
sum(if(type='click',sl,0)) click,
sum(if(type='event',sl,0)) event
from app.app_rec_realtime
where stat_dt between DATE_SUB(curdate(),INTERVAL 1 DAY) and concat(DATE_SUB(curdate(),INTERVAL 1 DAY),' 23:59:59')
group by stat_dt,abtest;
-------------------------------------------------------------------------------------------

delete from t_app_dingyue_lj_report where date between ${T1} and ${T2};

insert into t_app_dingyue_lj_report(date,type,add_count,cancel_count,lj_gz_count,lj_gz_fss)
select a.date,a.type, a.add_count, a.cancel_count,b.lj_gz_count, b.lj_gz_fss
from

(select date_format(creation_time,'%Y-%m-%d') date,type,
sum(case when action_type=1 then 1 else 0 end) add_count,
sum(case when action_type<>1 then 1 else 0 end) cancel_count
from dev_smzdm_base.t_dingyuedb_user_follow_log
where creation_time between ${T1} and concat(${T2},' 23:59:59')
and user_id not in(339483,2948655,2908329) and type<>'user'
group by date_format(creation_time,'%Y-%m-%d'),type
union all
select date_format(creation_date,'%Y-%m-%d') date,'user' type,
sum(case when type=1 then 1 else 0 end) add_count,
sum(case when type<>1 then 1 else 0 end) cancel_count
from dev_smzdm_base.t_UserLogDB_follow_log
where creation_date between ${T1} and concat(${T2},' 23:59:59')
and user_id not in(339483,2948655,2908329)
group by date_format(creation_date,'%Y-%m-%d')) a

left join

(select c.date,b.type,count(*) lj_gz_count,count(distinct a.user_id) lj_gz_fss
from dim_time c
left join dev_smzdm_base.t_dingyuedb_rule_base_user a on concat(c.date,' 23:59:59')>a.creation_time
inner join dev_smzdm_base.t_dingyuedb_rule_base b on a.rule_base_id=b.id
where c.date between ${T1} and ${T2}
and a.user_id not in(339483,2948655,2908329)
group by c.date,b.type
union all
select c.date,'user' type,count(*) lj_gz_count,count(distinct a.user_id) lj_gz_fss
from dim_time c
left join dev_smzdm_base.t_dingyuedb_follow_relate a on concat(c.date,' 23:59:59')>a.creation_date
where c.date between ${T1} and ${T2}
and a.user_id not in(339483,2948655,2908329)
group by c.date
union all
select c.date,case when wiki_hash_id is null then 'url' else 'baike' end type,count(*) lj_gz_count,count(distinct a.user_id) lj_gz_fss
from dim_time c
left join dev_smzdm_base.t_dingyuedb_dingyue_baike a on concat(c.date,' 23:59:59')>a.add_time
where c.date between ${T1} and ${T2}
and a.user_id not in(339483,2948655,2908329)
group by c.date,case when wiki_hash_id is null then 'url' else 'baike' end) b


on a.date=b.date and a.type=b.type;

dt=2019-04-27/dim=ar/v=gz_add
dt=2019-04-27/dim=ar/v=gz_cancel


select dim2,
max(if(v='gz_add',value,0)) add_count,
max(if(v='gz_cancel',value,0)) cancel_count
from bi_dw.fact_base_indicators
where dt='2019-04-10' and dim1='2019-04-10' and v in('gz_add','gz_cancel')
group by dim2



bi_app_ga.app_dingyue_type partition(dt)
select '${tx_date}' stat_dt,a.dim2 type,a.add_count,a.cancel_count,b.lj_gz_count,b.lj_gz_fss,c.all_lj_gz_count,c.all_lj_gz_fss,'${tx_date}' dt


CREATE EXTERNAL TABLE `app_dingyue_type`(
   `stat_dt` date COMMENT '日期',
  `type` string COMMENT '类型',
  `add_count` bigint COMMENT '加关注',
  `cancel_count` bigint COMMENT '取消关注',
  `lj_gz_count` bigint COMMENT '累计关注',
  `lj_gz_fss` bigint COMMENT '累计粉丝',
  `all_lj_gz_count` bigint COMMENT '总累计关注',
  `all_lj_gz_fss` bigint COMMENT '总累计粉丝')
PARTITIONED BY ( `dt` string)
LOCATION 'hdfs://cluster/bi/app_ga/app_dingyue_type';

CREATE TABLE `app_dingyue_type` (
  `stat_dt` date DEFAULT NULL COMMENT '日期',
  `type` varchar(50) DEFAULT NULL COMMENT 'type',
   `add_count` bigint(20) COMMENT '加关注',
  `cancel_count` bigint(20) COMMENT '取消关注',
  `lj_gz_count` bigint(20) COMMENT '累计关注',
  `lj_gz_fss` bigint(20) COMMENT '累计粉丝',
  `all_lj_gz_count` bigint(20) COMMENT '总累计关注',
  `all_lj_gz_fss` bigint(20) COMMENT '总累计粉丝',
  KEY `idx_stat_dt` (`stat_dt`,`type`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

sh /data/source/data_warehouse/ga/script/pub/exec_sparksql2.sh app_dingyue_type 4 10 30 /data/source/data_warehouse/ga/script/app/dingyue/ app_dingyue_type 2019-04-26
sh /data/source/data_warehouse/ga/script/pub/exec_sqoop_export_delete.sh 10.9.28.135:3320/app wdDBUser '2Gb(tv+-n' app_dingyue_type '/bi/app_ga/app_dingyue_type/dt=' "delete from app_dingyue_type where stat_dt=" 2019-04-26



--好价发布时间为3月11-3月17和4月15-4月21两个时间段；
--统计项：发布时间，好价文章ID，频道（发现/海淘/优惠），全部好价下曝光，全部好价下曝光>=500的时刻（yyyy-mm-dd hh:mm:ss），全部好价下曝光>=1000的时刻（yyyy-mm-dd hh:mm:ss）
create table bi_test.zyl_tmp_190428_1 as
select a.it,b.a id
from bi_ods_ga.ods_app_sdk_log a
lateral view json_tuple(a.ecp,'a') b as a
where (a.dt between '2019-03-11' and '2019-03-18' or a.dt between '2019-04-15' and '2019-04-22')
and a.ec='06' and a.ea='36' and a.av regexp '^9';

create table bi_test.zyl_tmp_190428_2 as
select pubdate,id,case when channel='1' then '优惠' when channel='2' then '发现'  else '海淘' end channel
from stg.db_youhui_youhui
where dt='2019-04-22'
and (pubdate between '2019-03-11' and '2019-03-17 23:59:59' or pubdate between '2019-04-15' and '2019-04-21 23:59:59')
and channel in('1','2','5');

create table bi_test.zyl_tmp_190428_3 as
select row_number()over(partition by b.id order by b.it) r,b.it,b.id
from bi_test.zyl_tmp_190428_2 a inner join bi_test.zyl_tmp_190428_1 b on a.id=b.id;

create table bi_test.zyl_tmp_190428_4 as
select a.pubdate,a.id,a.channel,nvl(b.sl,0) sl,nvl(c.it,'') d_500,nvl(d.it,'') d_1000
from bi_test.zyl_tmp_190428_2 a
left join(select id,count(*) sl from bi_test.zyl_tmp_190428_3 group by id) b on a.id=b.id
left join(select id,it from bi_test.zyl_tmp_190428_3 where r=500) c on a.id=c.id
left join(select id,it from bi_test.zyl_tmp_190428_3 where r=1000) d on a.id=d.id
order by pubdate;

12971191

select * from bi_test.zyl_tmp_190428_3 where id='12971191' order by r;


"


10.19.146.157 bi_test_user / Z16woifm82jzT4s

mysql -h10.19.146.157 -P3306 -ubi_test_user -p'Z16woifm82jzT4s'

create table app_rec_realtime(
stat_dt date,
type varchar(50),
abtest varchar(50),
sl int(10),
load_date timestamp,
primary key(stat_dt,type,abtest));



public static String getProperties_3(String filePath, String keyWord){
        Properties prop = new Properties();
        String value = null;
        try {
            InputStream inputStream = TestProperties.class.getResourceAsStream(filePath);
            prop.load(inputStream);
            value = prop.getProperty(keyWord);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return value;
    }

-----------------------------------------------------------------------------------------------------
cache table temp_ga as
select nvl(a.abtest,'首页全部') abtest,
nvl(b.channel_id,'all') channel_id,
sum(if(a.type='click',1,0)) click_count,
sum(if(a.type='event',1,0)) event_count
from(select if(hits_eventinfo_eventaction='首页站内文章点击','click','event') type,
nvl(if(hits_eventinfo_eventaction='首页站内文章点击',dim49,regexp_extract(dim64,'首页_推荐_feed流_([^_]+)',1)),'其他') abtest,
if(hits_eventinfo_eventaction='首页站内文章点击',regexp_extract(hits_eventinfo_eventlabel,'推荐_([^_]+)_([^_]+)',2),hits_product_productsku) article_id
from bi_dw_ga.fact_ga_hits_data
where dt ='2019-04-22' and hits_appinfo_appid in('com.smzdm.client.android','com.smzdm.client.ios') and hits_appinfo_appversion regexp '^8.(7|8|9)|^9.'
and (hits_eventinfo_eventcategory ='首页' and hits_eventinfo_eventaction='首页站内文章点击' and hits_eventinfo_eventlabel regexp '^推荐'
or hits_ecommerceaction_action_type='3' and hits_eventinfo_eventaction='添加到购物车' and dim64 regexp '首页_推荐_feed流')





select a.id,a.sl
from(
select regexp_extract(hits_eventinfo_eventlabel,'推荐_([^_]+)_([^_]+)',2) id,count(*) sl
from bi_dw_ga.fact_ga_hits_data
where dt ='2019-04-22' and hits_appinfo_appid in('com.smzdm.client.android','com.smzdm.client.ios') and hits_appinfo_appversion regexp '^8.(7|8|9)|^9.'
and hits_eventinfo_eventcategory ='首页' and hits_eventinfo_eventaction='首页站内文章点击' and hits_eventinfo_eventlabel regexp '^推荐' and dim49='c'
group by regexp_extract(hits_eventinfo_eventlabel,'推荐_([^_]+)_([^_]+)',2)) a
inner join tmp_article b on a.id=b.id
where b.channel_id=2;

13506066        24
13540456        4
13507112        19
13536129        1
13532188        14
13509338        20
13461923        1


select a.id,a.sl
from(
select hits_product_productsku id,count(*) sl
from bi_dw_ga.fact_ga_hits_data
where dt ='2019-04-22' and hits_appinfo_appid in('com.smzdm.client.android','com.smzdm.client.ios') and hits_appinfo_appversion regexp '^8.(7|8|9)|^9.'
and hits_ecommerceaction_action_type='3' and hits_eventinfo_eventaction='添加到购物车' and dim64 regexp '首页_推荐_feed流' and regexp_extract(dim64,'首页_推荐_feed流_([^_]+)',1)='c'
group by hits_product_productsku) a
inner join tmp_article b on a.id=b.id
where b.channel_id=2;



CREATE TABLE `t_stream_data_info` (
  `id` bigint(11) NOT NULL AUTO_INCREMENT COMMENT '主键',
  `itemId` bigint(20) DEFAULT NULL COMMENT '文章id',
  `channel` smallint(5) DEFAULT NULL COMMENT '文章频道',
  `channelId` smallint(5) DEFAULT NULL COMMENT '文章详细子频道(如好价的1,2,5,21)',
  `level1s` varchar(2000) DEFAULT NULL COMMENT '一级分类 json数组',
  `level2s` varchar(2000) DEFAULT NULL COMMENT '二级分类 json数组',
  `level3s` varchar(2000) DEFAULT NULL COMMENT '三级分类 json数组',
  `malls` varchar(2000) DEFAULT NULL COMMENT '商城 json数组',
  `stream` varchar(5) DEFAULT NULL COMMENT '流量',
  `cTime` datetime DEFAULT NULL COMMENT '创建时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB  DEFAULT CHARSET=utf8;

select log_time,pubdate from bi_ctr.search_exposure_ori where dt='2019-04-20' and article_id='11865629' limit 10;
