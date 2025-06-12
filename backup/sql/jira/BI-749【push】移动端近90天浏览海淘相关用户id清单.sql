insert overwrite local directory '/data/tmp/zhaoyulong/data' row format delimited fields terminated by '\t'
select distinct c.user_id
from(
select article_id,dim16
from bi_dw_ga.fact_ga_hits_data
where article_id<>''  and length(dim16)=10
and dt between '2019-04-15' and '2019-07-15' and hits_type='APPVIEW' and hits_appinfo_appid in('com.smzdm.client.android','com.smzdm.client.ios') and hits_appinfo_screenname regexp '好价') a
inner join(select id from bi_dw_ga.dim_article_info where channel_id='5') b on a.article_id=b.id
inner join(select * from stg.db_udc_account where dt='2019-07-15') c on a.dim16=c.smzdm_id;
;
