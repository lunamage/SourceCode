--Ecommerce	交易
--mall_id	商城id	字符串
--mall	商城名称	字符串
--order_id	订单号	字符串
--order_time	订单时间	时间
--status	订单状态	字符串
--amount	GMV	数值
--CPS	CPS	数值

--1.上传smzdmid，否则上传9999999999；
--2.京东+淘系+拼多多+苏宁；
--3.每天上传每个商城上传3000条订单，10.1-10.5

{
    "distinct_id": "123456",
    "time": 1434556935000,
    "type": "track",
    "event": "Ecommerce",
    "project": "default",
    "time_free": true,
    "properties": {
        "mall":"淘系",
        "order_id":"12345",
        "status":"1",
        "amount":14.0,
        "cps":14.0
    }
}

create table bi_test.zyl_tmp_191218_1 as
select concat('{\"distinct_id\":\"',suserid,'\",',
'\"time\":',ordertime,',',
'\"type\":\"track\",',
'\"event\":\"Ecommerce\",',
'\"project\":\"default\",',
'\"time_free\":true,',
'\"properties\":{',
'\"mall\":\"',mall,'\",',
'\"order_id\":\"',orderid,'\",',
'\"status\":\"',status,'\",',
'\"amount\":',orderamount,',',
'\"cps\":',commission,'}}') json
from(
select suserid,concat(unix_timestamp(ordertime),'000') ordertime,mall,nvl(orderid,'000') orderid,status,nvl(orderamount,0) orderamount,nvl(commission,0) commission,
row_number()over(partition by to_date(ordertime),mall order by ordertime) r
from bi_dw_gmv.dw_t_order
where dt='2019-10' and mall in('苏宁','京东','淘系','拼多多') and suserid regexp '^[0-9]{10}$'
and to_date(ordertime) between '2019-10-01' and '2019-10-05') a
where r<=3000;

insert overwrite local directory '/data/tmp/zhaoyulong/data' row format delimited fields terminated by '\t'
select * from bi_test.zyl_tmp_191218_1 order by json;


sudo su - sa_cluster
cd /home/sa_cluster/sa/tools/batch_importer
bin/sa-importer --path /data/tmp/zhaoyulong
bin/sa-importer --path /data/tmp/zhaoyulong --import --session new



--ZA-show	ZA-曝光
--article_id	文章id	字符串 a
--channel_id	频道ID	字符串 c
--position	列表页中的位置	字符串  p
--advertisement	坑位类型	字符串  ad
--66	tab名称	字符串  66

--抽样100个did
--ecp里截取
--每天抽20个did，一共5天

{
    "distinct_id": "123456",
    "time": 1434556935000,
    "type": "track",
    "event": "ZAshow",
    "project": "default",
    "time_free": true,
    "properties": {
        "channel_id":"1",
        "article_id":"12345",
        "position":"1",
        "advertisement":"1",
        "cd66":"tab名称"
    }
}

create table bi_test.zyl_tmp_191218_2 as
select concat('{\"distinct_id\":\"',uid,'\",',
'\"time\":',logtime,',',
'\"type\":\"track\",',
'\"event\":\"ZAshow\",',
'\"project\":\"default\",',
'\"time_free\":true,',
'\"properties\":{',
'\"channel_id\":\"',c,'\",',
'\"article_id\":\"',id,'\",',
'\"position\":\"',p,'\",',
'\"advertisement\":\"',ad,'\",',
'\"cd66\":\"',cd66,'\"}}') json
from(
select a.uid,concat(unix_timestamp(a.it),'000') logtime,nvl(b.a,'其他') id,nvl(b.c,'其他') c,nvl(b.p,'其他') p,nvl(b.ad,'其他') ad,nvl(b.cd66,'其他') cd66
from bi_ods_ga.ods_app_sdk_log a
lateral view json_tuple(a.ecp,'a','c','p','ad','66') b as a,c,p,ad,cd66
where a.dt between '2019-12-15' and '2019-12-16' and a.ec='01' and a.ea='01'
and a.av regexp '^9.' and a.uid regexp '^[0-9]{10}$' order by rand() limit 20000) a;

--ZA-event	ZA-点击
--ec	event category	字符串
--ea	event action	字符串
--el	event label	字符串
--mall	商城名称	字符串 1
--abtest_tuijian	推荐ab流	字符串  13
--article_id	文章id	字符串 4
--channel_id	频道ID	字符串 28
--channel	频道	字符串  11
--scenario	来源场景	字符串 21
{
    "distinct_id": "123456",
    "time": 1434556935000,
    "type": "track",
    "event": "ZAevent",
    "project": "default",
    "time_free": true,
    "properties": {
        "ec":"1",
        "ea":"1",
        "el":"1",
        "mall":"1",
        "abtest_tuijian":"a",
        "article_id":"a",
        "channel_id":"a",
        "channel":"a",
        "scenario":"a"
    }
}

create table bi_test.zyl_tmp_191218_3 as
select concat('{\"distinct_id\":\"',uid,'\",',
'\"time\":',logtime,',',
'\"type\":\"track\",',
'\"event\":\"ZAevent\",',
'\"project\":\"default\",',
'\"time_free\":true,',
'\"properties\":{',
'\"ec\":\"',ec,'\",',
'\"ea\":\"',ea,'\",',
'\"el\":\"',el,'\",',
'\"mall\":\"',cd1,'\",',
'\"abtest_tuijian\":\"',cd13,'\",',
'\"article_id\":\"',cd4,'\",',
'\"channel_id\":\"',cd28,'\",',
'\"channel\":\"',cd11,'\",',
'\"scenario\":\"',cd21,'\"}}') json
from(
select a.uid,concat(unix_timestamp(a.it),'000') logtime,nvl(a.ec,'其他') ec,nvl(a.ea,'其他') ea,nvl(a.el,'其他') el,
nvl(b.cd1,'其他') cd1,nvl(b.cd13,'其他') cd13,nvl(b.cd4,'其他') cd4,nvl(b.cd28,'其他') cd28,nvl(b.cd11,'其他') cd11,nvl(b.cd21,'其他') cd21
from bi_ods_ga.ods_app_sdk_log a
lateral view json_tuple(a.ecp,'1','13','4','28','11','21') b as cd1,cd13,cd4,cd28,cd11,cd21
where a.dt between '2019-12-15' and '2019-12-16' and a.ec='首页' and a.ea='首页站内文章点击'
and a.av regexp '^9.' and a.uid regexp '^[0-9]{10}$' order by rand() limit 20000) a;

--ZA-pageview	ZA-浏览
--screenname	屏幕名称	字符串
--mall	商城名称	字符串	1
--abtest_tuijian	推荐ab流	字符串	13
--channel_id	频道ID	字符串	28
--channel	频道	字符串	11
--scenario	来源场景	字符串	21
--article_id	文章id	字符串	4
{
    "distinct_id": "123456",
    "time": 1434556935000,
    "type": "track",
    "event": "ZApageview",
    "project": "default",
    "time_free": true,
    "properties": {
        "mall":"1",
        "abtest_tuijian":"1",
        "channel_id":"1",
        "channel":"1",
        "scenario":"a",
        "article_id":"a"
    }
}

create table bi_test.zyl_tmp_191218_4 as
select concat('{\"distinct_id\":\"',uid,'\",',
'\"time\":',logtime,',',
'\"type\":\"track\",',
'\"event\":\"ZApageview\",',
'\"project\":\"default\",',
'\"time_free\":true,',
'\"properties\":{',
'\"screenname\":\"',sn,'\",',
'\"mall\":\"',cd1,'\",',
'\"abtest_tuijian\":\"',cd13,'\",',
'\"article_id\":\"',cd4,'\",',
'\"channel_id\":\"',cd28,'\",',
'\"channel\":\"',cd11,'\",',
'\"scenario\":\"',cd21,'\"}}') json
from(
select a.uid,concat(unix_timestamp(a.it),'000') logtime,nvl(a.sn,'其他') sn,nvl(a.ea,'其他') ea,nvl(a.el,'其他') el,
nvl(b.cd1,'其他') cd1,nvl(b.cd13,'其他') cd13,nvl(b.cd4,'其他') cd4,nvl(b.cd28,'其他') cd28,nvl(b.cd11,'其他') cd11,nvl(b.cd21,'其他') cd21
from bi_ods_ga.ods_app_sdk_log a
lateral view json_tuple(a.ecp,'1','13','4','28','11','21') b as cd1,cd13,cd4,cd28,cd11,cd21
where a.dt between '2019-12-15' and '2019-12-16' and a.t='screenview'
and a.av regexp '^9.' and a.uid regexp '^[0-9]{10}$' order by rand() limit 20000) a;




insert overwrite local directory '/data/tmp/zhaoyulong/data' row format delimited fields terminated by '\t'
select * from bi_test.zyl_tmp_191218_2 order by json;

insert overwrite local directory '/data/tmp/zhaoyulong/data' row format delimited fields terminated by '\t'
select * from bi_test.zyl_tmp_191218_3 order by json;

insert overwrite local directory '/data/tmp/zhaoyulong/data' row format delimited fields terminated by '\t'
select * from bi_test.zyl_tmp_191218_4 order by json;


sudo su - sa_cluster
cd /home/sa_cluster/sa/tools/batch_importer
bin/sa-importer --path /data/tmp/zhaoyulong/imp1
bin/sa-importer --path /data/tmp/zhaoyulong/imp1 --import --session new
