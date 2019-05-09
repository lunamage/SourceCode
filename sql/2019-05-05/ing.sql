insert into app_rec_realtime_bak(stat_dt,abtest,exposure,click,event)
select stat_dt,
abtest,
sum(if(type='exposure',sl,0)) exposure,
sum(if(type='click',sl,0)) click,
sum(if(type='event',sl,0)) event
from app.app_rec_realtime
where stat_dt between DATE_SUB(curdate(),INTERVAL 1 DAY) and concat(DATE_SUB(curdate(),INTERVAL 1 DAY),' 23:59:59')
group by stat_dt,abtest;



--总表
select stat_dt,abtest,exposure,click,event,click/exposure ctr,event/click bl,event/exposure bl2
from(
select date_format(stat_dt,'%Y-%m-%d') stat_dt,
abtest,
sum(if(type='exposure',sl,0)) exposure,
sum(if(type='click',sl,0)) click,
sum(if(type='event',sl,0)) event
from app.app_rec_realtime
where stat_dt between CURRENT_DATE() and concat(CURRENT_DATE(),' 23:59:59')
and abtest not in ('无','smzdm','')
group by date_format(stat_dt,'%Y-%m-%d'),abtest) a
order by bl2;

--具体流量明细表
select stat_dt,abtest,exposure,click,event,click/exposure ctr,event/click bl
from(
select stat_dt,
abtest,
sum(if(type='exposure',sl,0)) exposure,
sum(if(type='click',sl,0)) click,
sum(if(type='event',sl,0)) event
from app.app_rec_realtime
where stat_dt between CURRENT_DATE() and concat(CURRENT_DATE(),' 23:59:59') and abtest='a'
group by stat_dt,abtest) a
order by stat_dt;

select count(*),max(load_date) from app.app_rec_realtime;

select date_format(stat_dt,'%Y-%m-%d') d,count(*) from app.app_rec_realtime group by date_format(stat_dt,'%Y-%m-%d');

select avg(sl)
from(
select date_format(load_date,'%Y-%m-%d %H:%i'),count(*) sl
from app.app_rec_realtime group by date_format(load_date,'%Y-%m-%d %H:%i')) a;

select * from app.app_rec_realtime  order by load_date desc limit 100;
--------------------------------------------------------------------------------------------------------------


select a.sn,a.ecp
from bi_ods_ga.ods_app_sdk_log a
lateral view json_tuple(a.ecp,'tv','ctp','sp','a','c') b as tv,ctp,sp,a,c
where a.dt between '2019-05-04' and '2019-05-04' and a.sn regexp '好价/P/'
and a.av regexp '^9.4' limit 200;

------------------------------------------------------------------------------------------------

SELECT
    bl_date,
    sum(bl_count)bl_count,
    sum(bl_sendgoodprice_count)bl_sendgoodprice_count,
    sum(bl_youhui_count)bl_youhui_count,
    sum(bl_haitao_count)bl_haitao_count,
    sum(bl_Synchronoushome_count)bl_Synchronoushome_count
FROM
    t_app_baoliao_1total
WHERE
    type in(1,7)
    and bl_date between '2019-04-24' and DATE_SUB(current_date,INTERVAL 1 DAY)
    group by bl_date

SELECT
    bl_date,
    bl_count,
    bl_sendgoodprice_count,
    bl_youhui_count,
    bl_haitao_count,
    bl_Synchronoushome_count,
    concat(round((select count(distinct a.id) from
          dev_smzdm_base.t_zdmdb_baoliao_baoliao a left join dev_smzdm_base.t_zdmdb_baoliao_baoliao_extend b on a.id = b.bl_id
          where b.source in('用户android端爆料','用户iOS端爆料','用户winphone端爆料','用户ipad端爆料','用户win8端爆料') and a.bl_from = 1
          and a.bl_add_time between DATE_SUB(current_date,INTERVAL 1 DAY) and DATE_SUB(current_date,INTERVAL 0 DAY))*100 / bl_count,2),'%') khdzb,
    concat(round((select max(yh_yfbl_count)*100 from t_app_baoliao_8newmodel where bl_date=DATE_SUB(current_date,INTERVAL 1 DAY))/bl_count,2),'%') ybblzb
FROM
    t_app_baoliao_1total
WHERE
    type =1
    and bl_date = DATE_SUB(current_date,INTERVAL 1 DAY);


    select a.pubdate,
    sum(case when a.channel in(1,2,5) then 1 else 0 end) fx_count,
    sum(case when a.source_from=9 and a.channel in(1,2,5) then 1 else 0 end) zj_pl_fx_count,
    sum(case when a.source_from=10 and a.channel in(1,2,5) then 1 else 0 end) zj_sw_fx_count,
    sum(case when a.channel = 1 then 1 else 0 end) gn_count,
    sum(case when a.channel = 5 then 1 else 0 end) ht_count,
    sum(case when a.sync_home<>0 then 1 else 0 end) home_count,
    sum(case when b.bl_from=1 and a.channel in(1,5) then 1 else 0 end) user_jx_count,
    sum(case when a.source_from=9 and a.channel in(1,5) then 1 else 0 end) zj_pl_count,
    sum(case when b.bl_from=7 and a.channel in(1,5)  then 1 else 0 end) sj_jx_count,
    sum(case when a.source_from=10 and a.channel in(1,5) then 1 else 0 end) zj_sw_count
    from t_dim_article_youhui a left join t_fact_baoliao_associate b on a.id=b.article_id
    where a.pubdate =DATE_SUB(current_date,INTERVAL 1 DAY)
    group by a.pubdate;


    SELECT
        a.bl_date,
        fbbls,
        concat(round(fbbls * 100/user_bl_count,2),'%') bl,
        concat(round(fbbls_fb * 100/fbs,2),'%') bl2,
        concat(round(fbbls_fb * 100/fbbls,2),'%') bl3,
        concat(round(fbbls_fb*100/(select count(*) from t_dim_article_youhui a left join t_fact_baoliao_associate b on a.id=b.article_id
                  where a.pubdate =DATE_SUB(current_date,INTERVAL 1 DAY) and a.channel in(1,2,5)),2),'%') bl4
    FROM
        t_app_baoliao_8newmodel a
    WHERE
        a.bl_date = DATE_SUB(current_date,INTERVAL 1 DAY);



select hits_appinfo_screenname,article_id
from bi_dw_ga.fact_ga_hits_data a
where a.dt ='2019-05-01'
and hits_type='APPVIEW' and a.hits_appinfo_appid in('com.smzdm.client.android','com.smzdm.client.ios')
and hits_appinfo_screenname regexp '好价/P/13655818|好价/过期/P/13655818';

select count(*)
from bi_dw_ga.fact_ga_hits_data a
where a.dt ='2019-05-01'
and hits_type='APPVIEW' and a.hits_appinfo_appid in('com.smzdm.client.android','com.smzdm.client.ios')
and article_id='13655818';
