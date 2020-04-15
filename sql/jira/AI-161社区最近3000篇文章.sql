insert overwrite local directory '/data/tmp/zhaoyulong/data'
select a.id,a.channel_id,a.channel,a.title,b.content2
from (select * from bi_dw_ga.dim_article_info where channel_id in(6,8,11,31,66) and pubdate<='2020-03-10' order by pubdate desc limit 5000) a
left join(select * from stg.db_smzdm_article_article_base where dt='2020-03-09') b on a.id=b.article_id
order by id;
