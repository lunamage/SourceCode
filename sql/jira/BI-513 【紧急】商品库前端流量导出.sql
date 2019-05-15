307A: 主机名 regexp 'wiki' and 主机名 not regexp 'zhi.smzdm.com|go.smzdm.com'
102:主机名 not regexp '(\.m.smzdm)|^(m|h5)' and 主机名 not regexp 'zhi.smzdm.com|go.smzdm.com' and a.hits_page_pagepath not regexp '/gourl/|/url|/business'
103:主机名 regexp '(\.m.smzdm)|^(m|h5)'

--百科PC-UV
select a.dt,count(*) uv
from(
select distinct dt,fullvisitorid
from bi_dw_ga.fact_ga_hits_data a
where a.dt between '2017-01-01' and '2017-03-31'
and a.hits_page_hostname regexp 'wiki' and a.hits_type='PAGE' and a.hits_page_hostname not regexp 'zhi.smzdm.com|go.smzdm.com'
and a.hits_page_hostname not regexp '(\.m.smzdm)|^(m|h5)') a
inner join (select * from bi_dw_ga.fact_ga_fullvisitorid_flow where dt between '2017-01-01' and '2017-03-31' and splatform='pc') b on a.dt=b.dt and a.fullvisitorid=b.fullvisitorid
group by a.dt;

--百科WAP-UV
select a.dt,count(*) uv
from(
select distinct dt,fullvisitorid
from bi_dw_ga.fact_ga_hits_data a
where a.dt between '2017-01-01' and '2017-03-31'
and a.hits_page_hostname regexp 'wiki' and a.hits_type='PAGE' and a.hits_page_hostname not regexp 'zhi.smzdm.com|go.smzdm.com'
and a.hits_page_hostname regexp '(\.m.smzdm)|^(m|h5)') a
inner join (select * from bi_dw_ga.fact_ga_fullvisitorid_flow where dt between '2017-01-01' and '2017-03-31' and splatform='wap') b on a.dt=b.dt and a.fullvisitorid=b.fullvisitorid
group by a.dt;

--品牌库PC-UV
select a.dt,count(*) uv
from(
select distinct dt,fullvisitorid
from bi_dw_ga.fact_ga_hits_data a
where a.dt between '2017-01-01' and '2017-03-31'
and a.hits_page_hostname regexp 'pinpai.smzdm.com' and a.hits_type='PAGE' and a.hits_page_hostname not regexp 'zhi.smzdm.com|go.smzdm.com' and a.hits_page_pagepath not regexp '/gourl/|/url|/business'
and a.hits_page_hostname not regexp '(\.m.smzdm)|^(m|h5)') a
inner join (select * from bi_dw_ga.fact_ga_fullvisitorid_flow where dt between '2017-01-01' and '2017-03-31' and splatform='pc') b on a.dt=b.dt and a.fullvisitorid=b.fullvisitorid
group by a.dt;

--品牌库WAP-UV
select a.dt,count(distinct a.fullvisitorid) uv
from bi_ods_ga.ods_ga_hits_data a
left join (select * from bi_ods_ga.ods_ga_hits_customdimensions_data where dt between '2017-01-01' and '2017-03-31' and index='8') c on a.id=c.id and a.dt=c.dt and a.hits_hitnumber=c.hits_hitnumber
inner join (select * from bi_dw_ga.fact_ga_fullvisitorid_flow where dt between '2017-01-01' and '2017-03-31' and splatform='wap') b on a.fullvisitorid=b.fullvisitorid and a.dt=b.dt
where a.dt between '2017-01-01' and '2017-03-31' and a.hits_page_hostname!='go.smzdm.com' and a.hits_type='PAGE' and a.hits_page_hostname regexp '(\.m.smzdm)|^(m|h5)'
and a.hits_page_hostname regexp 'pinpai.m.smzdm.com' and a.hits_page_pagepath not regexp '/gourl/|/url|/business|go.smzdm.com'
and (c.value not regexp '(?i)smzdmapp' or c.value is null)
group by a.dt;

--商城PC-UV
select a.dt,count(*) uv
from(
select distinct dt,fullvisitorid
from bi_dw_ga.fact_ga_hits_data a
where a.dt between '2017-01-01' and '2017-03-31'
and a.hits_page_pagepath regexp 'smzdm.com/mall' and a.hits_type='PAGE' and a.hits_page_hostname not regexp 'zhi.smzdm.com|go.smzdm.com' and a.hits_page_pagepath not regexp '/gourl/|/url|/business'
and a.hits_page_hostname not regexp '(\.m.smzdm)|^(m|h5)') a
inner join (select * from bi_dw_ga.fact_ga_fullvisitorid_flow where dt between '2017-01-01' and '2017-03-31' and splatform='pc') b on a.dt=b.dt and a.fullvisitorid=b.fullvisitorid
group by a.dt;

--商城WAP-UV
select a.dt,count(distinct a.fullvisitorid) uv
from bi_ods_ga.ods_ga_hits_data a
left join (select * from bi_ods_ga.ods_ga_hits_customdimensions_data where dt between '2017-01-01' and '2017-03-31' and index='8') c on a.id=c.id and a.dt=c.dt and a.hits_hitnumber=c.hits_hitnumber
inner join (select * from bi_dw_ga.fact_ga_fullvisitorid_flow where dt between '2017-01-01' and '2017-03-31' and splatform='wap') b on a.fullvisitorid=b.fullvisitorid and a.dt=b.dt
where a.dt between '2017-01-01' and '2017-03-31' and a.hits_page_hostname!='go.smzdm.com' and a.hits_type='PAGE' and a.hits_page_hostname regexp '(\.m.smzdm)|^(m|h5)'
and a.hits_page_pagepath regexp 'smzdm.com/mall' and a.hits_page_pagepath not regexp '/gourl/|/url|/business|go.smzdm.com'
and (c.value not regexp '(?i)smzdmapp' or c.value is null)
group by a.dt;


create table bi_test.zyl_tmp_190512_4 as
select dt,uv
from(
select dt,uv from bi_test.zyl_tmp_190511_4
union all
select dt,uv from bi_test.zyl_tmp_190511_40
union all
select dt,uv from bi_test.zyl_tmp_190511_400
union all
select dt,uv from bi_test.zyl_tmp_190511_4000
union all
select dt,uv from bi_test.zyl_tmp_190511_40000
union all
select dt,uv from bi_test.zyl_tmp_190511_400000) a
order by dt;

create table bi_test.zyl_tmp_190512_6 as
select dt,uv
from(
select dt,uv from bi_test.zyl_tmp_190511_6
union all
select dt,uv from bi_test.zyl_tmp_190511_60
union all
select dt,uv from bi_test.zyl_tmp_190511_600
union all
select dt,uv from bi_test.zyl_tmp_190511_6000
union all
select dt,uv from bi_test.zyl_tmp_190511_60000
union all
select dt,uv from bi_test.zyl_tmp_190511_600000) a
order by dt;

insert overwrite local directory '/data/tmp/zhaoyulong/data' row format delimited fields terminated by '\t'
select * from(
select '百科PC-UV' bz,dt,uv from bi_test.zyl_tmp_190512_1
union all
select '百科WAP-UV' bz,dt,uv from bi_test.zyl_tmp_190512_2
union all
select '品牌库PC-UV' bz,dt,uv from bi_test.zyl_tmp_190512_3
union all
select '品牌库WAP-UV' bz,dt,uv from bi_test.zyl_tmp_190512_4
union all
select '商城PC-UV' bz,dt,uv from bi_test.zyl_tmp_190512_5
union all
select '商城WAP-UV' bz,dt,uv from bi_test.zyl_tmp_190512_6) a
order by bz,dt;
