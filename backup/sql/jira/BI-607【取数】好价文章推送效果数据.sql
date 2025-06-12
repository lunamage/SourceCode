--日期（推送的年月日）
--文章名称
--文章ID或文章链接
--文章总PV
--一级分类（如有）
--推送时间（推送的时分秒）
--推送规则（如商城、分类、标签、品牌等）
--推送标题
--推送副标题
--送达数
--打开数
--打开率：打开数/送达数
--来自推送的电商点击（自定义维度69：G3）
--二跳率：来自推送的电商点击/打开数

create table bi_test.zyl_tmp_190625_1 as
select a.dt,
a.hits_product_productsku id,
count(*) event
from bi_dw_ga.fact_ga_hits_data a
where a.dt BETWEEN '2019-05-01' AND '2019-06-18' and a.hits_ecommerceaction_action_type=3
and (a.hits_eventinfo_eventcategory like '%电商点击%' or (a.hits_eventinfo_eventaction like '%添加到购物车%' and (hits_appinfo_appid='com.smzdm.client.android' or hits_appinfo_appid='com.smzdm.client.ios')))
and dim69 regexp 'G3'
group by dt,hits_product_productsku;



insert overwrite local directory '/data/tmp/zhaoyulong/data' row format delimited fields terminated by '\t'
select to_date(a.msg_time) msg_d,
b.title,
a.id,
c.pv,
b.cate_level1,
substring(a.msg_time,12,8) msg_h,
a.msg_ext1,
a.msg_full_title,
a.msg_content,
a.send_count,
a.open_count,
a.open_count/a.send_count bl1,
d.event,
d.event/a.open_count bl2
from(
select a.msg_time,
a.msg_id id,
sum(b.send_count) send_count,
sum(b.open_count) open_count,
a.msg_full_title,
a.msg_content,
a.msg_ext1
from (select * from stg.db_dbzdm_push_mps_msg where dt=date_sub(CURRENT_DATE(),1) and msg_time BETWEEN '2019-05-01' AND concat('2019-06-18',' 23:59:59') and msg_type in('youhui','faxian','haitao') and msg_source='sub') a
inner join (select * from stg.db_dbzdm_push_mps_msg_batch where dt=date_sub(CURRENT_DATE(),1)) b ON a.global_id = b.global_id
group by a.msg_time,
a.msg_id,
a.msg_full_title,
a.msg_content,
a.msg_ext1) a
left join (select * from bi_dw_ga.dim_article_info where channel_id in(1,2,5,21)) b on a.id=b.id
left join (select article_id,pc_pv+wap_pv+app_pv pv from bi_dw_ga.fact_ga_article_flow_summary where dt='2019-06-24' and dim='youhui') c on a.id=c.article_id
left join (select id,sum(event) event from bi_test.zyl_tmp_190625_1 group by id) d on a.id=d.id
order by msg_d,id;
