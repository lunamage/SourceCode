/*new_alldata_key_tj3_expireResult 有效期
new_alldata_key_tj3_youhui_result_24
new_alldata_key_tj3_youhui_result_48
new_alldata_key_tj3_priceHeadNewResult (发现补量  头部数据)
new_alldata_key_tj3_newResult 自然结果
new_alldata_key_tj3_mustResult 强制分发
new_alldata_key_tj3_advanceResult 广告*/

CREATE TEMPORARY VIEW t_article_data
USING org.apache.spark.sql.jdbc
OPTIONS (
url  "jdbc:mysql://10.9.28.135:3353/recommendDB?user=recommendUser&password=pVhXTntx9ZG",
dbtable "t_article_data",
fetchSize "100000");

cache table t_article_data;

cache table t_article_data_clean as
select a.channel,
a.itemId id,
case when a.channelId in(6,7,8,11,31,66) then '非好价'
when a.scene in('new_alldata_key_tj3_expireResult','new_alldata_key_tj3_advanceResult') then '有效期'
when a.scene='new_alldata_key_tj3_priceHeadNewResult' then '发现补量'
when a.scene='new_alldata_key_tj3_youhui_result_24' then '24H'
when a.scene='new_alldata_key_tj3_youhui_result_48' then '48H' end scene,
if(a.scene='new_alldata_key_tj3_youhui_result_48',from_unixtime(unix_timestamp(a.enterTime)+86400,'yyyy-MM-dd HH:mm:ss'),a.enterTime) enterTime,
a.quitTime,
a.malls
from t_article_data a
where a.scene in('new_alldata_key_tj3_expireResult','new_alldata_key_tj3_youhui_result_24','new_alldata_key_tj3_youhui_result_48','new_alldata_key_tj3_priceHeadNewResult','new_alldata_key_tj3_advanceResult') or a.channelId in(6,7,8,11,31,66);

create table bi_test.zyl_tmp_190531_2 as
select scene,if(malls is null or malls='[]','其他商城',regexp_extract(malls,'val\":\"([^\"]+)',1)) mall,count(*) sl
from t_article_data_clean a
where enterTime<='2019-06-01 11:00:00' and (quitTime>='2019-06-01 11:00:00' or quitTime is null)
group by scene,if(malls is null or malls='[]','其他商城',regexp_extract(malls,'val\":\"([^\"]+)',1));

insert overwrite local directory '/data/tmp/zhaoyulong/data' row format delimited fields terminated by '\t'
select * from bi_test.zyl_tmp_190531_2;
