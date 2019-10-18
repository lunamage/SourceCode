insert overwrite local directory '/data/tmp/zhaoyulong/data' row format delimited fields terminated by '\t'
select a.hits_eventinfo_eventlabel query,count(*) search_query_count
from bi_dw_ga.fact_ga_hits_data a
where a.dt between '2019-10-10' and '2019-10-17'
and a.hits_eventinfo_eventcategory='搜索'
and a.hits_eventinfo_eventaction regexp '搜索_综合'
and a.hits_appinfo_appversion regexp '^8.7|^8.8|^8.9|^9.|^4[0-9]{2}'
and a.hits_eventinfo_eventlabel <>'无'
and a.hits_eventinfo_eventaction not regexp '_分类$|_品牌$|_单品$'
group by a.hits_eventinfo_eventlabel order by search_query_count desc limit 50000;
