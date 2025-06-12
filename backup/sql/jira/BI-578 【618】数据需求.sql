create table bi_test.zyl_tmp_190528_1 as
select a.dt,a.mall,a.exposure,nvl(b.event,0) event
from(
select a.dt,a.mall,count(*) exposure
from bi_dw_ga.fact_recsys_display_data a
where a.dt in('2018-06-01','2018-06-17','2018-06-18','2018-11-01','2018-11-10','2018-11-11')
group by a.dt,a.mall) a
left join(
select a.dt,a.dim12 mall,count(*) event
from bi_dw_ga.fact_ga_hits_data a
where a.dt in('2018-06-01','2018-06-17','2018-06-18','2018-11-01','2018-11-10','2018-11-11')
and (hits_appinfo_appid='com.smzdm.client.android' or hits_appinfo_appid='com.smzdm.client.ios')
and a.hits_ecommerceaction_action_type='3'
and a.hits_eventinfo_eventaction='添加到购物车' and a.dim64 regexp '首页_推荐_feed流' and a.hits_appinfo_appversion regexp '^8.(7|8|9)|^9.'
group by a.dt,a.dim12) b on a.dt=b.dt and a.mall=b.mall;



create table bi_test.zyl_tmp_190528_2 as
select a.dt,b.level1,b.level2,a.exposure,a.event
from(
select dt,dim3 cate2,
sum(if(metric='cate2_exposure_count',`values`,0)) exposure,
sum(if(metric='cate2_rec_event',`values`,0)) event
from bi_app_ga.app_ga_recommend_statistics
where dt in('2018-06-01','2018-06-17','2018-06-18','2018-11-01','2018-11-10','2018-11-11')
and metric in('cate2_rec_event','cate2_exposure_count')
group by dt,dim3) a
inner join (select max(level1) level1,level2 from bi_dw_ga.dim_category group by level2) b on a.cate2=b.level2;


create table bi_test.zyl_tmp_190528_3 as
select a.dt,a.exposure,b.event
from(
select a.dt,count(*) exposure
from bi_dw_ga.fact_recsys_display_data a
where a.dt in('2018-06-01','2018-06-17','2018-06-18','2018-11-01','2018-11-10','2018-11-11')
and a.tags like '%白菜%'
group by a.dt) a
left join(
select a.dt,sum(a.event) event
from(
select dt,article_id,event
from bi_dw_ga.fact_ga_article_flow
where dt in('2018-06-01','2018-06-17','2018-06-18','2018-11-01','2018-11-10','2018-11-11')
and dim='youhui' and splatform='app' and event<>0) a
inner join bi_dw_ga.dim_article_info b on a.article_id=b.id and b.channel_id in(1,2,5,21)
where b.tags_name like '%白菜%'
group by a.dt) b on a.dt=b.dt;
