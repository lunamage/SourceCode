--1、导出coupon表中，createtime晚于等于20190619的文章ID（coupon表中的ID字段）
create table bi_test.zyl_tmp_190712_1 as
select id
from stg.db_exchange_coupon
where dt='2019-07-11' and create_time >='2019-06-19';

--2、导出券频道同步到好价频道的文章
--统计周期：coupon表中，create_time为20190619-20190709的券文章对应的数据
--导出字段：券ID(coupon表中取id）,对应好价文章ID（根据券ID，从coupon_to_youhui_log日志表中取对应的youhui_id）、youhui发布日期、优惠所属频道（优惠、海淘、发现）
create table bi_test.zyl_tmp_190712_2 as
select b.coupon_id,b.youhui_id,c.pubdate,case when channel_id='1' then '优惠' when channel_id='2' then '发现' else '海淘' end channel
from(select id
from stg.db_exchange_coupon
where dt='2019-07-11' and create_time between '2019-06-19' and '2019-07-09 23:59:59') a
inner join stg.db_exchange_coupon_to_youhui_log b on a.id=b.coupon_id
inner join (select * from bi_dw_ga.dim_article_info where channel_id in(1,2,5)) c on b.youhui_id=c.id;

--3、coupon表中的文章在关注频道的曝光、点击（曝光和点击均用SDK记录的数据）
--统计周期：20190619-20190709
--统计数据范围：1中导出的id
--导出字段：日期、文章ID、曝光数（口径如下）、点击数（口径如下）
/*曝光数据规则

t='show' 且 ec='02' 且 ea in ('01','02')

取ecp中的a做为文章id用文章id

取av作为版本

点击数据规则

t='event' 且 ec=关注

ea=首页关注_点击

用el字段作为文章id

 用av作为版本

用取到的文章id去匹配业务库的好价文章id和优惠券id */
create table bi_test.zyl_tmp_190712_3 as
