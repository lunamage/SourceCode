/*需求背景：

为分析实时特征刷屏情况，需要根据3级分类数维度，统计用户数。



需求说明：

1、时间范围：0428-0 508

2、用户范围：a流、b流、q流、z7流

3、位置范围：只统计位置为20以上的曝光事件

4、导出字段：日期   流量   3级分类数    用户数*/

create table bi_test.zyl_tmp_190509_1 as
select a.dt,b.tv,a.did,b.a id
from bi_ods_ga.ods_app_sdk_log a
lateral view json_tuple(a.ecp,'a','c','tv','p','sp') b as a,c,tv,p,sp
where a.dt between '2019-04-28' and '2019-05-08'
and a.ec='01' and a.ea='01' and b.sp='0' and b.tv in('a','b','q','z7')
and cast(b.p as int)>20;


insert overwrite local directory '/data/tmp/zhaoyulong/data' row format delimited fields terminated by '\t'
select dt,tv,cate_count,count(distinct did) uv
from(
select a.dt,a.tv,a.did,count(distinct b.cate_level3_id) cate_count
from bi_test.zyl_tmp_190509_1 a
inner join(
select id,cate_level3_id from bi_dw_ga.dim_article_info where channel_id in('1','2','5','21','6','8','11','31','66')) b on a.id=b.id
group by a.dt,a.tv,a.did) a
group by dt,tv,cate_count;
