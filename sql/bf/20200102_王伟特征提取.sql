create table bi_test.zyl_tmp_200102_1 as
select
a.dt,
a.user_id,
a.article_id,
b.channelid,
a.ab_test,
b.price,
b.level1,
b.level2,
b.level3,
b.level4,
a.f,
a.calscore,
a.conversionpercent
from bi_log.feature_log_extract a
lateral view json_tuple(a.featuremap,'channelid','price','level1','level2','level3','level4') b
as channelid,price,level1,level2,level3,level4
where a.dt between '2019-12-31' and '2020-01-01';
