create table bi_test.zyl_tmp_190819_3 as
select dt,
sum(if(b.cd21 not regexp '优惠券',1,0)) sl1,
count(distinct if(b.cd21 not regexp '优惠券',a.did,null)) uv1,
sum(if(b.cd21='优惠券',1,0)) sl2,
count(distinct if(b.cd21='优惠券',a.did,null)) uv2,
count(*) sl3,
count(distinct a.did) uv3
from bi_ods_ga.ods_app_sdk_log a
lateral view json_tuple(a.ecp,'1','21') b as cd1,cd21
where a.dt between '2019-07-01' and '2019-08-18'
and a.ec='增强型电子商务' and a.ea='添加到购物车' and av regexp'^9.[4-9]|^10.|^11.'
and b.cd1='京东'
group by dt order by dt;


create table bi_test.zyl_tmp_190819_1 as
select a.dt,
sum(if(a.hits_appinfo_appid='com.smzdm.client.android' or a.hits_appinfo_appid='com.smzdm.client.ios',1,0)) app_sl,
count(distinct if(a.hits_appinfo_appid='com.smzdm.client.android' or a.hits_appinfo_appid='com.smzdm.client.ios',fullvisitorid,null)) app_uv,
sum(if(hits_eventinfo_eventcategory like '%电商点击%' and a.hits_page_hostname not regexp '(\.m.smzdm)|^(m|h5)',1,0)) pc_sl,
count(distinct if(hits_eventinfo_eventcategory like '%电商点击%' and a.hits_page_hostname not regexp '(\.m.smzdm)|^(m|h5)',fullvisitorid,null)) pc_uv,
sum(if(a.hits_page_hostname regexp '(\.m.smzdm)|^(m|h5)',1,0)) wap_sl,
count(distinct if(a.hits_page_hostname regexp '(\.m.smzdm)|^(m|h5)',fullvisitorid,null)) wap_uv,
sum(if(a.hits_page_hostname regexp '(\.m.smzdm)|^(m|h5)' and (dim8 not regexp '(?i)smzdmapp' or dim8 is null) ,1,0)) wap_sl2,
count(distinct if(a.hits_page_hostname regexp '(\.m.smzdm)|^(m|h5)' and (dim8 not regexp '(?i)smzdmapp' or dim8 is null) ,fullvisitorid,null)) wap_uv2
from bi_dw_ga.fact_ga_hits_data a
where a.dt between '2019-07-01' and '2019-08-18'  and a.hits_ecommerceaction_action_type=3
and (a.hits_eventinfo_eventcategory like '%电商点击%' or (a.hits_eventinfo_eventaction like '%添加到购物车%' and (hits_appinfo_appid='com.smzdm.client.android' or hits_appinfo_appid='com.smzdm.client.ios')))
and dim12 in('京东','京东全球购','京东全球购eBay精选','京东金融','京东到家','京东众筹','京东酒店','京东国际','海囤全球')
group by dt order by dt;



create table bi_test.zyl_tmp_190819_2 as
select a.dt,
count(*) pc_sl,
count(DISTINCT fullvisitorid) pc_uv
from bi_dw_ga.fact_ga_hits_data a
where a.dt between '2019-07-01' and '2019-08-18'  and a.hits_ecommerceaction_action_type=3
and a.hits_eventinfo_eventcategory like '%电商点击%' and a.hits_page_hostname not regexp '(\.m.smzdm)|^(m|h5)'
and dim12 in('京东','京东全球购','京东全球购eBay精选','京东金融','京东到家','京东众筹','京东酒店','京东国际','海囤全球')
group by dt order by dt;
