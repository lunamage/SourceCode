create table bi_test.zyl_tmp_190516_1 as
select a.dt,a.sid,a.did,b.cate_level3,count(*) sl1,count(distinct a.id) sl2
from(
select a.dt,a.sid,a.did,b.a id
from bi_ods_ga.ods_app_sdk_log a
lateral view json_tuple(a.ecp,'a','c','tv','p','sp') b as a,c,tv,p,sp
where a.dt between '2019-05-09' and '2019-05-09'
and a.ec='01' and a.ea='01' and b.sp='0') a
inner join (select id,cate_level3 from bi_dw_ga.dim_article_info where channel_id in('1','2','5','21','6','8','11','31','66')) b on a.id=b.id
group by a.dt,a.sid,a.did,b.cate_level3;

create table bi_test.zyl_tmp_190516_2 as
select a.dt,a.sid,a.did,b.cate_level3,count(*) sl3
from(
select a.dt,a.sid,a.did,regexp_extract(a.el,'_([^_]+)_([^_]+)',2) id
from bi_ods_ga.ods_app_sdk_log a
lateral view json_tuple(a.ecp,'a','c','tv','p','sp') b as a,c,tv,p,sp
where a.dt between '2019-05-09' and '2019-05-09'
and a.ec='首页' and a.ea='首页站内文章点击' and a.el regexp '^推荐_') a
inner join (select id,cate_level3 from bi_dw_ga.dim_article_info where channel_id in('1','2','5','21','6','8','11','31','66')) b on a.id=b.id
group by a.dt,a.sid,a.did,b.cate_level3;

--日期 会话id 用户id 三级品类 曝光 有效曝光 点击
insert overwrite local directory '/data/tmp/zhaoyulong/data' row format delimited fields terminated by '\t'
select a.dt,a.sid,a.did,a.cate_level3,a.sl1,a.sl2,b.sl3
from (select * from bi_test.zyl_tmp_190516_1 where cate_level3<>'' and isnotnull(cate_level3) and sid is not null) a
left join (select * from bi_test.zyl_tmp_190516_2 where cate_level3<>'' and isnotnull(cate_level3) and sid is not null) b on a.sid=b.sid and a.cate_level3=b.cate_level3 and a.did=b.did
where isnotnull(b.cate_level3)
order by sid limit 50000;
