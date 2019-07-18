

insert overwrite local directory '/data/tmp/zhaoyulong/data' row format delimited fields terminated by '\t'
select * from bi_test.zyl_tmp_190624_2 order by sl desc;






create table bi_test.zyl_tmp_190624_1 as
select regexp_extract(hits_eventinfo_eventaction,'结果点击_([^_]+)',1) query,
count(*) sl,
sum(if(dim13 in('youhui','haitao'),1,0)) jx_sl,
sum(if(dim13 in('faxian'),1,0)) fx_sl,
sum(if(dim13 in('youhui','haitao'),1,0))/count(*) jx_bl,
sum(if(dim13 in('faxian'),1,0))/count(*) fx_bl
from bi_dw_ga.fact_ga_hits_data
where dt between '2019-03-24' and '2019-06-23' and hits_appinfo_appversion regexp '^8.(7|8|9)|^9.' and hits_eventinfo_eventcategory='搜索' and dim13 in('youhui','faxian','haitao')
and hits_eventinfo_eventaction regexp '结果点击_' and dim14='1'
group by regexp_extract(hits_eventinfo_eventaction,'结果点击_([^_]+)',1)
order by sl desc;


create table bi_test.zyl_tmp_190624_2 as
select regexp_extract(hits_eventinfo_eventaction,'结果点击_([^_]+)',1) query,
count(*) sl,
sum(if(dim13 in('youhui','haitao'),1,0)) jx_sl,
sum(if(dim13 in('faxian'),1,0)) fx_sl,
sum(if(dim13 in('youhui','haitao'),1,0))/count(*) jx_bl,
sum(if(dim13 in('faxian'),1,0))/count(*) fx_bl
from bi_dw_ga.fact_ga_hits_data
where dt between '2019-01-01' and '2019-06-23' and hits_appinfo_appversion regexp '^8.(7|8|9)|^9.' and hits_eventinfo_eventcategory='搜索' and dim13 in('youhui','faxian','haitao')
and hits_eventinfo_eventaction regexp '结果点击_' and dim14='1'
group by regexp_extract(hits_eventinfo_eventaction,'结果点击_([^_]+)',1)
order by sl desc;

create table bi_test.zyl_tmp_190624_1 as
select regexp_extract(hits_eventinfo_eventaction,'结果点击_([^_]+)',1) query,
count(*) sl,
sum(if(dim13 in('youhui','haitao'),1,0)) jx_sl,
sum(if(dim13 in('faxian'),1,0)) fx_sl,
sum(if(dim13 in('youhui','haitao'),1,0))/count(*) jx_bl,
sum(if(dim13 in('faxian'),1,0))/count(*) fx_bl
from bi_dw_ga.fact_ga_hits_data a
where a.dt between '2019-05-25' and '2019-06-23'
and a.hits_eventinfo_eventcategory='搜索'
and a.hits_eventinfo_eventaction regexp '结果点击_'
and a.hits_eventinfo_eventaction regexp '_综合$|_综合_'
and dim14='1'
and dim13 in('youhui','faxian','haitao')
and a.hits_appinfo_appversion regexp '^8.(7|8|9)|^9.'
group by regexp_extract(hits_eventinfo_eventaction,'结果点击_([^_]+)',1)
order by sl desc;


create table bi_test.zyl_tmp_190624_2 as
select regexp_extract(hits_eventinfo_eventaction,'结果点击_([^_]+)',1) query,
count(*) sl,
sum(if(dim13 in('youhui','haitao'),1,0)) jx_sl,
sum(if(dim13 in('faxian'),1,0)) fx_sl,
sum(if(dim13 in('youhui','haitao'),1,0))/count(*) jx_bl,
sum(if(dim13 in('faxian'),1,0))/count(*) fx_bl
from bi_dw_ga.fact_ga_hits_data a
where a.dt between '2019-03-25' and '2019-06-23'
and a.hits_eventinfo_eventcategory='搜索'
and a.hits_eventinfo_eventaction regexp '结果点击_'
and a.hits_eventinfo_eventaction regexp '_综合$|_综合_'
and dim14='1'
and dim13 in('youhui','faxian','haitao')
and a.hits_appinfo_appversion regexp '^8.(7|8|9)|^9.'
group by regexp_extract(hits_eventinfo_eventaction,'结果点击_([^_]+)',1)
order by sl desc;
