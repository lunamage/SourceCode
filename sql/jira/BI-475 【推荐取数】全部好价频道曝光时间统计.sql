/*
需求详细：

时间范围：好价发布时间为5月20-6月5 时间段；

数据范围：好价类型文章

统计项：发布时间（yyyy-mm-dd hh:mm:ss），好价文章ID，频道（发现/海淘/优惠），好价feed流累计曝光量，全部好价下曝光>=500的时刻（yyyy-mm-dd hh:mm:ss），全部好价下曝光>=1000的时刻（yyyy-mm-dd hh:mm:ss）

t=show 且 ec=06 且 ea=36 且ecp中c为1,2,5
时间使用it字段
文章id取ecp中a
app版本限制为9.0及以上 */

create table bi_test.zyl_tmp_190606_1 as
select a.it,b.a id
from bi_ods_ga.ods_app_sdk_log a
lateral view json_tuple(a.ecp,'a','c') b as a,c
where a.dt between '2019-05-20' and '2019-06-05' and a.ec='06' and a.ea='36' and b.c in('1','2','5') and a.av regexp '^9.';

create table bi_test.zyl_tmp_190606_2 as
select id,count(*) sl
from bi_test.zyl_tmp_190606_1
group by id; 

create table bi_test.zyl_tmp_190606_3 as
select id,
max(if(r=500,it,'')) it_500,
max(if(r=1000,it,'')) it_1000
from(
select id,it,row_number()over(partition by id order by it) r
from bi_test.zyl_tmp_190606_1) a
where r in(500,1000)
group by id;

insert overwrite local directory '/data/tmp/zhaoyulong/data' row format delimited fields terminated by '\t'
select b.pubdate,a.id,b.channel,a.sl,d.it_500,d.it_1000
from bi_test.zyl_tmp_190606_2 a
inner join(
select id,channel,pubdate
from bi_dw_ga.dim_article_info
where channel_id in('1','2','5') and pubdate between '2019-05-20' and '2019-06-05 23:59:59') b on a.id=b.id
inner join bi_test.zyl_tmp_190606_3 d on a.id=d.id order by pubdate;
