insert overwrite local directory '/data/tmp/zhaoyulong/data' row format delimited fields terminated by '\t'
select a.pubdate,a.clean_link,a.digital_price,a.id,a.brand,a.worthy,a.unworthy,a.comment_count,a.collection_count,b.pc_event,b.wap_event,b.app_event,c.amount,c.sales
from(
select pubdate,clean_link,digital_price,id,brand,worthy,unworthy,comment_count,collection_count
from stg.db_youhui_youhui
where dt='2019-06-01' and channel in(1,5) and mall_id in('183','3639','3641','4579','5005','5123','5199','8547','9019') and pubdate>='2019-01-01' and pubdate<='2019-06-01 15:00:00') a
left join(
select article_id,pc_event,wap_event,app_event from fact_ga_article_flow_summary where dt='2019-05-31') b on a.id=b.article_id
left join (select level2 article_id,
sum(if(totalname='day_gmv_queren_art_amount',totalvalue,0)) amount,
sum(if(totalname='day_gmv_queren_art_sales',totalvalue,0)) sales
from bi_app_gmv.t_day_statistics
where dt>='2018-01' and totalname in('day_gmv_queren_art_amount','day_gmv_queren_art_sales') and level2 regexp '[0-9]+'
group by level2) c on a.id=c.article_id;
