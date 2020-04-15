--用户筛选：
--2月24-2月28日 新用户（无画像）， 第二天留存 标记为新留存用户
--2月24-2月28日 老用户（画像超过2天）， 第二天留存 标记为老留存用户（随机选取，数量等下当天新留存用户的数量）

--分别导出以上用户访问的文章
--用户id、用户昵称、画像时间、文章ID、文章频道、价格、一级分类、二级分类、三级分类、四级分类、文章标题、浏览时间、是否在这篇文章上有电商点击



create table bi_test.zyl_tmp_200304_1 as
select a.dt,
a.uid,
case when a.ec='首页' then 'click' else 'event' end type,
if(a.ec='01',if(b.c='87','87',b.atp),if(b.cd11='ku_day_know','87',b.cd20)) channel,
case when a.ec='首页' then regexp_extract(a.el,'_([^_]+)_([^_]+)',2) else b.cd4 end id,
max(isnp) isnp
from bi_ods_ga.ods_app_sdk_log a
lateral view json_tuple(a.ecp,'a','c','tv','p','sp','atp','20','4','21','11') b as a,c,tv,p,sp,atp,cd20,cd4,cd21,cd11
where a.dt between '2020-02-24' and '2020-02-29'
and (a.ec='首页' and a.ea='首页站内文章点击' and b.cd20 in('3','76','7')
or a.ec='增强型电子商务' and a.ea='添加到购物车' and b.cd21 regexp '^首页_推荐_feed流_' and b.cd20 in('3','76','7'))
and a.uid regexp '[0-9]{10}'
group by a.dt,
a.uid,
case when a.ec='首页' then 'click' else 'event' end,
if(a.ec='01',if(b.c='87','87',b.atp),if(b.cd11='ku_day_know','87',b.cd20)),
case when a.ec='首页' then regexp_extract(a.el,'_([^_]+)_([^_]+)',2) else b.cd4 end;

--新用户
create table bi_test.zyl_tmp_200304_2 as
select a.dt,a.uid
from (select distinct dt,uid from bi_test.zyl_tmp_200304_1 where isnp='1' and type='click') a
inner join (select distinct dt,uid from bi_test.zyl_tmp_200304_1 where type='click') b on a.dt=date_sub(b.dt,1) and a.uid=b.uid;

--老用户
create table bi_test.zyl_tmp_200304_3 as
select a.dt,a.uid
from (select distinct dt,uid from bi_test.zyl_tmp_200304_1 where isnp='0' and type='click') a
inner join (select distinct dt,uid from bi_test.zyl_tmp_200304_1 where type='click') b on a.dt=date_sub(b.dt,1) and a.uid=b.uid
inner join (select distinct user_proxy_key from bi_dw.dw_cp_user_detail where dt='2020-02-22' and sys='180d' and tag_type_id='200') c on a.uid=c.user_proxy_key;


--sl =
--2020-02-24      3270
--2020-02-25      3965
--2020-02-26      4984
--2020-02-27      4616
--2020-02-28      4788
select dt,count(*) from bi_test.zyl_tmp_200304_2 group by dt order by dt;

create table bi_test.zyl_tmp_200304_4 as
select dt,uid
from(
select dt,uid,row_number()over(partition by dt order by rand()) r
from bi_test.zyl_tmp_200304_3) a
where dt='2020-02-24' and r<=3270
or dt='2020-02-25' and r<=3965
or dt='2020-02-26' and r<=4984
or dt='2020-02-27' and r<=4616
or dt='2020-02-28' and r<=4788;


--分别导出以上用户访问的文章
--用户id、用户昵称、文章ID、文章频道、价格、一级分类、二级分类、三级分类、四级分类、文章标题、浏览时间、是否在这篇文章上有电商点击
create table bi_test.zyl_tmp_200304_5 as
select if(channel_id in('6','8','11','31','66'),hash_id,id) id,
case when channel_id in('1','2','5','21') then '3'
when channel_id in('6','8','11','31','66') then '76' else '7' end channel,
price,
cate_level1,cate_level2,cate_level3,cate_level4,
title
from bi_dw_ga.dim_article_info
where channel_id in('1','2','5','21','6','8','11','31','66','7');


create table bi_test.zyl_tmp_200304_6 as
select a.dt,
a.uid,
b.id,
case when b.channel='3' then '好价' when b.channel='76' then '社区' else '众测' end channel,
c.price,
c.cate_level1,c.cate_level2,c.cate_level3,c.cate_level4,
c.title,
if(isnull(d.id),'无电商点击','有电商点击') bz
from bi_test.zyl_tmp_200304_2 a
left join (select * from bi_test.zyl_tmp_200304_1 where type='click') b on a.dt=b.dt and a.uid=b.uid
left join bi_test.zyl_tmp_200304_5 c on b.channel=c.channel and b.id=c.id
left join (select * from bi_test.zyl_tmp_200304_1 where type='event') d on a.dt=d.dt and a.uid=d.uid and b.id=d.id;

create table bi_test.zyl_tmp_200304_7 as
select a.dt,
a.uid,
b.id,
case when b.channel='3' then '好价' when b.channel='76' then '社区' else '众测' end channel,
c.price,
c.cate_level1,c.cate_level2,c.cate_level3,c.cate_level4,
c.title,
if(isnull(d.id),'无电商点击','有电商点击') bz
from bi_test.zyl_tmp_200304_4 a
left join (select * from bi_test.zyl_tmp_200304_1 where type='click') b on a.dt=b.dt and a.uid=b.uid
left join bi_test.zyl_tmp_200304_5 c on b.channel=c.channel and b.id=c.id
left join (select * from bi_test.zyl_tmp_200304_1 where type='event') d on a.dt=d.dt and a.uid=d.uid and b.id=d.id;


insert overwrite local directory '/data/tmp/zhaoyulong/data'
select * from bi_test.zyl_tmp_200304_6 order by dt;

insert overwrite local directory '/data/tmp/zhaoyulong/data'
select * from bi_test.zyl_tmp_200304_7 order by dt;
