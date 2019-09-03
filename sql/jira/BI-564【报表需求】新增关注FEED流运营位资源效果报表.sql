select dt,
'横滑多规则' type,
if(ea regexp '首页猜你喜欢',regexp_extract(el,'_([^_]+)_([^_]+)',2),regexp_extract(el,'_([^_]+)',1)) name,
'' type2,
sum(if(ea='横滑多规则',1,0)) sl1,
sum(if(ea regexp '首页关注_横滑推荐模块|首页猜你喜欢',1,0)) sl2,
sum(if(ea regexp '首页关注_横滑推荐模块|首页猜你喜欢' and el regexp '加关注|添加关注',1,0)) sl3
from bi_ods_ga.ods_app_sdk_log a
where a.dt='2019-08-25'
and (a.ec='关注' and a.ea='横滑多规则'
or a.ec='关注' and a.ea regexp '首页关注_横滑推荐模块|首页猜你喜欢')
group by dt,if(ea regexp '首页猜你喜欢',regexp_extract(el,'_([^_]+)_([^_]+)',2),regexp_extract(el,'_([^_]+)',1));


--
select dt,
'人工配置单一规则' type,
if(ea='横滑单一规则',regexp_extract(el,'_([^_]+)',1),regexp_extract(el,'_([^_]+)_([^_]+)',2)) name,
max(if(ea='横滑单一规则',regexp_extract(el,'_([^_]+)_([^_]+)',2),null)) type2,
sum(if(ea='横滑单一规则',1,0)) sl1,
sum(if(ea regexp '首页关注_单规则横滑动推荐模块|首页关注_单规则横滑推荐模块',1,0)) sl2,
sum(if(ea regexp '首页关注_单规则横滑动推荐模块|首页关注_单规则横滑推荐模块' and el regexp '加关注|添加关注',1,0)) sl3
from bi_ods_ga.ods_app_sdk_log a
where a.dt='2019-08-25'
and (a.ec='关注' and a.ea='横滑单一规则'
or a.ec='关注' and a.ea regexp '首页关注_单规则横滑动推荐模块|首页关注_单规则横滑推荐模块')
group by dt,if(ea='横滑单一规则',regexp_extract(el,'_([^_]+)',1),regexp_extract(el,'_([^_]+)_([^_]+)',2));
--
select a.dt,
'个性化单一规则' type,
a.name,
b.type2,
a.sl1,
b.sl2,
b.sl3
from(
select a.dt,
b.rn name,
count(*) sl1
from bi_ods_ga.ods_app_sdk_log a
lateral view json_tuple(a.ecp,'srtp','rn') b as srtp,rn
where a.dt='2019-08-25' and a.ec='02' and a.ea regexp '^01|02$'
and b.srtp regexp '个性化'
group by a.dt,b.rn) a
left join (
select a.dt,
regexp_extract(a.hits_eventinfo_eventlabel,'_([^_]+)_([^_]+)',2) name,
max(regexp_extract(a.hits_eventinfo_eventlabel,'_([^_]+)_([^_]+)_([^_]+)',3)) type2,
count(*) sl2,
sum(if(a.hits_eventinfo_eventlabel regexp '加关注',1,0)) sl3
from bi_dw_ga.fact_ga_hits_data a
where a.dt ='2019-08-25'
and (hits_appinfo_appid='com.smzdm.client.android' or hits_appinfo_appid='com.smzdm.client.ios')
and a.hits_eventinfo_eventcategory='关注' and a.hits_eventinfo_eventaction='首页单一规则个性化点击'
group by a.dt,regexp_extract(a.hits_eventinfo_eventlabel,'_([^_]+)_([^_]+)',2)) b on a.dt=b.dt and a.name=b.name;
--
select dt,
'无更新推荐关注模块' type,
'' name,
'' type2,
sum(if(ea='无更新模块加载屏幕数',1,0)) sl1,
sum(if(ea regexp '首页关注_无更新用户顶部推荐模块点击|无更新用户顶部推荐模块换一批点击',1,0)) sl2,
sum(if(ea regexp '首页关注_无更新用户顶部推荐模块点击|无更新用户顶部推荐模块换一批点击' and el regexp '加关注|添加关注',1,0)) sl3
from bi_ods_ga.ods_app_sdk_log a
where a.dt='2019-08-25'
and (a.ec='关注' and a.ea='无更新模块加载屏幕数'
or a.ec='关注' and a.ea regexp '首页关注_无更新用户顶部推荐模块点击|无更新用户顶部推荐模块换一批点击')
group by dt;
--
select dt,
'用户标签竖列' type,
'' name,
'' type2,
sum(if(ea='用户标签竖列',1,0)) sl1,
sum(if(ea='首页关注_竖排推荐关注模块',1,0)) sl2,
sum(if(ea='首页关注_竖排推荐关注模块' and el regexp '加关注|添加关注',1,0)) sl3
from bi_ods_ga.ods_app_sdk_log a
where a.dt='2019-08-25'
and (a.ec='关注' and a.ea='用户标签竖列' or a.ec='关注' and a.ea='首页关注_竖排推荐关注模块')
group by dt;
