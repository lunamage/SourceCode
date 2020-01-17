--1
insert overwrite local directory '/data/tmp/zhaoyulong/data'
select a.pro_id,a.pro_name,b.top_category_id
from (select pro_id,pro_name from stg.db_wiki_product_index where dt='2019-12-16') a
left join (select pro_id,max(top_category_id) top_category_id from stg.db_wiki_product_category_v2 where dt='2019-12-16' group by pro_id) b on a.pro_id=b.pro_id;

--2 bi_test.zyl_tmp_191213_1
create table bi_test.zyl_tmp_200109_1 as
SELECT dt,
id,
dim16,
article_id,
cast(case when next_page_time<>0 then (next_page_time-hits_time)/1000 else 0 end as bigint) timeonscreen
FROM (
SELECT dt,id,hits_hitnumber,hits_time,LEAD(hits_time,1,0) OVER (PARTITION BY dt,id ORDER BY cast(hits_hitnumber as int)) AS next_page_time,article_id,
case when hits_page_hostname regexp '(www|faxian|haitao|(^m))' and hits_page_pagepath regexp '(m.)?((www.|faxian.|haitao.)?(m.)?smzdm.com)|(/p/)' and hits_page_pagepath not regexp '/gourl/|/url|/business' and hits_page_pagepath not regexp 'm.smzdm.com/list|list_preview/[0-9]+' or hits_appinfo_screenname regexp '好价' and hits_appinfo_screenname not regexp '好价\/闲值' then 'youhui'
when hits_page_hostname regexp '(shaiwu|post)' or hits_appinfo_screenname regexp '原创' then 'yuanchuang'
when hits_page_hostname regexp 'news' or hits_appinfo_screenname regexp '资讯' then 'news'
when hits_page_hostname regexp 'test' and hits_page_pagepath regexp '/pingce/p/' or hits_appinfo_screenname regexp '评测' then 'pingce'
when hits_page_hostname regexp 'test' and hits_page_pagepath not regexp 'pingce' and hits_page_pagepath regexp '/p/' or hits_appinfo_screenname regexp '众测/产品/' then 'zhongce'
when hits_page_hostname regexp 'brand' and hits_page_pagepath regexp 'topic' or hits_appinfo_screenname regexp '新锐品牌' then 'newbrand_topic'
when hits_page_hostname regexp 'list' or hits_appinfo_screenname regexp '好物榜单' then 'haowubang'
when hits_page_hostname regexp 'wiki' and hits_page_pagepath regexp '/p/' or hits_appinfo_screenname regexp '百科' then 'wiki'
when hits_appinfo_screenname regexp '长图文|值友说' then 'haowen' end dim,
dim16
FROM (
select dt,id,hits_hitnumber,hits_time,hits_appinfo_screenname,hits_page_hostname,hits_page_pagepath,article_id,hits_type,dim16
from bi_dw_ga.fact_ga_hits_data WHERE hits_type in('APPVIEW') and dt between '2019-07-01' and '2019-12-16'
union all
select dt,id,hits_hitnumber,hits_time,'' hits_appinfo_screenname,hits_page_hostname,hits_page_pagepath,article_id,hits_type,dim16
from(SELECT row_number() over(partition by dt,id order by cast(hits_hitnumber as int) desc) r,dt,id,hits_hitnumber,hits_time,hits_page_hostname,hits_page_pagepath,article_id,hits_type,dim16
FROM bi_dw_ga.fact_ga_hits_data
WHERE dt between '2019-07-01' and '2019-12-16' and hits_type in('EVENT','APPVIEW')) a
where r=1) a) a
where dim='youhui' and article_id<>'';

create table bi_test.zyl_tmp_200109_2 as
select a.dt,a.id,a.dim16,i.wiki_id,a.timeonscreen
from bi_test.zyl_tmp_200109_1 a
inner join (select article_id,min(meta_value) wiki_id from stg.db_youhui_youhui_meta where dt=date_sub(current_date,1) and meta_key='wiki_id' group by article_id) i on a.article_id=i.article_id
where cast(i.wiki_id as int)>0 ;

create table bi_test.zyl_tmp_200109_3 as
select a.id,a.dim16,a.wiki_id,a.timeonscreen
from bi_test.zyl_tmp_200109_2 a
inner join(
select id
from bi_test.zyl_tmp_200109_2
group by id
having(count(*)>5)) b on a.id=b.id;

create table bi_test.zyl_tmp_200109_4 as
SELECT concat('{',max(dim16),':{',concat_ws(',',collect_set(concat(wiki_id,':',timeonscreen))),'}}') json
from bi_test.zyl_tmp_200109_3
group by id;
