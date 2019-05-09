explain
select query,concat('cl_',channel,'_',article_id,'_1d') type,sum(click) v
from(
select a.query,a.channel,a.article_id,a.click
from bi_dw_ga.dim_sq_query_click a
where a.dt='2019-05-08') a
group by query,concat('cl_',channel,'_',article_id,'_1d')
union all
select query,concat('cl_',channel,'_',article_id,'_3d') type,sum(click) v
from (select a.query,a.channel,a.article_id,a.click
from (select query,channel,article_id,sum(click) click from bi_dw_ga.dim_sq_query_click where dt between date_sub('2019-05-08',2) and '2019-05-08' group by query,channel,article_id) a) a
group by query,concat('cl_',channel,'_',article_id,'_3d')
union all
select query,concat('cl_',channel,'_',article_id,'_7d') type,sum(click) v
from (select a.query,a.channel,a.article_id,a.click
from (select query,channel,article_id,sum(click) click from bi_dw_ga.dim_sq_query_click where dt between date_sub('2019-05-08',6) and '2019-05-08' group by query,channel,article_id) a) a
group by query,concat('cl_',channel,'_',article_id,'_7d')
union all
select query,concat('cl_',channel,'_',article_id,'_time') type,max(v) v
from(
select a.query,a.channel,a.article_id,a.v
from (select query,channel,article_id,max(max_time) v from bi_dw_ga.dim_sq_query_click where dt between date_sub('2019-05-08',6) and '2019-05-08' group by query,channel,article_id) a) a
group by query,concat('cl_',channel,'_',article_id,'_time');
