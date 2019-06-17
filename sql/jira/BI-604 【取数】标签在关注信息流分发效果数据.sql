cache table tmp_tags as
select '白菜党' name union all
select '神价格' name union all
select '绝对值' name union all
select '历史低价' name union all
select '必领神券' name union all
select '手慢无' name union all
select '每日白菜' name union all
select '预告' name union all
select '日用品囤货' name union all
select '值友专享' name union all
select '京东PLUS会员' name union all
select '吃货' name union all
select '特价机票' name union all
select '必看活动' name union all
select '免费得' name union all
select '促销活动' name union all
select '限时秒杀' name union all
select '京东全品类券' name union all
select '618预售' name union all
select '每天刷什么' name union all
select '中亚Prime会员' name union all
select '羊毛党' name union all
select '排行榜商品' name union all
select 'App限免' name union all
select '育儿经' name union all
select '618精华汇总' name union all
select 'iPhone价格动态' name union all
select '优惠券码' name union all
select '半价特惠' name union all
select '奇葩物' name union all
select '攒机爱好者' name union all
select '米面粮油' name union all
select '自由行' name union all
select '京东羊毛福利' name union all
select '轻奢女包' name union all
select '高端秀' name union all
select '京东数码' name union all
select '户外党' name union all
select '京东促销' name union all
select '9块9包邮' name union all
select '拼单' name union all
select '海淘排行榜' name union all
select '支付宝福利' name union all
select '日常出行优惠' name union all
select '养车囤货' name union all
select '家装囤货活动' name union all
select '好价汇总一览' name union all
select '白菜汇总' name union all
select '重返游戏' name union all
select 'COSME人气单品' name union all
select '移动专享' name union all
select '囤书活动' name union all
select '租房家居指南' name union all
select '凑单品' name union all
select '车榜单' name union all
select '618预告' name union all
select '招商银行' name union all
select '京东食品购' name union all
select '夏日清凉特饮' name union all
select '苏宁数码' name union all
select '新品发售' name union all
select '微信支付福利' name union all
select '线下快餐福利' name union all
select 'EDC' name union all
select '拼购价' name union all
select '临期品' name union all
select '每日精选榜' name union all
select '旅游BUG价' name union all
select '话费流量优惠' name union all
select '多重优惠叠加' name union all
select '信用卡剁手指南' name union all
select 'switch游戏粉' name union all
select '视频网站VIP折扣' name union all
select '值酒馆' name union all
select 'IKEA宜家俱乐部' name union all
select '关注有奖' name union all
select '办公室饿了来点零食' name union all
select '薅羊毛' name union all
select '天猫促销' name union all
select '健身党' name union all
select '每日一景点' name union all
select '食材集' name union all
select '每日精选车品' name union all
select '母婴囤货活动' name union all
select '游戏限免' name union all
select 'Prime会员' name union all
select '数码潮人' name union all
select '小编精选' name union all
select '超级爆料团' name union all
select '养娃更轻松' name union all
select '好看清单' name union all
select '618必看' name union all
select 'Home+' name union all
select '遇书坊' name union all
select '食客' name union all
select '燃' name union all
select '家电研究所' name;

create table bi_test.zyl_tmp_190611_1 as
select a.dt,a.keyword,a.ljgz,a.yx_count
from (select dt,keyword,ljgz,yx_count from bi_app_ga.app_dingyue_keyword_summary where dt between '2019-05-04' and '2019-05-10' and type='tag'
union all
select dt,keyword,ljgz,yx_count from bi_app_ga.app_dingyue_keyword_summary where dt between '2019-05-11' and '2019-05-18' and type='tag'
union all
select dt,keyword,ljgz,yx_count from bi_app_ga.app_dingyue_keyword_summary where dt between '2019-05-19' and '2019-05-24' and type='tag'
union all
select dt,keyword,ljgz,yx_count from bi_app_ga.app_dingyue_keyword_summary where dt between '2019-05-25' and '2019-06-01' and type='tag'
union all
select dt,keyword,ljgz,yx_count from bi_app_ga.app_dingyue_keyword_summary where dt between '2019-06-02' and '2019-06-05' and type='tag'
) a
inner join tmp_tags b on a.keyword=b.name
order by dt,keyword;



create table bi_test.zyl_tmp_190611_2 as
select a.dt,c.name keyword,count(*) sl,sum(d.pv) pv,sum(d.event) event
from (select id,to_date(pubdate) dt from stg.db_youhui_youhui where dt='2019-06-05' and pubdate between '2019-05-04' and concat('2019-06-05',' 23:59:59') and channel in(1,2,5) and yh_status=1) a
inner join (select article_id,tag_id from stg.db_youhui_youhui_tag_type_item where dt='2019-06-05') b on a.id=b.article_id
inner join (select id,name from stg.db_smzdm_smzdm_tag_type where dt='2019-06-05') c on b.tag_id=c.id
left join (select article_id,sum(pc_pv+wap_pv+app_pv) pv,sum(pc_event+wap_event+app_event) event from bi_dw_ga.fact_ga_article_flow_summary where dt='2019-06-05' and dim='youhui' group by article_id) d on a.id=d.article_id
inner join tmp_tags e on c.name=e.name
group by a.dt,c.name;


--5.曝光规则:    ec='02' 且 ea in (‘01’，‘02’）取ecp中rn作为本需求中的标签。取ecp中atp='3',取ecp中a作为文章id,app版本限制在9.3.20及以上
create table bi_test.zyl_tmp_190611_3 as
select a.dt,a.rn keyword,a.sl bgl,a.asl
from(
select a.dt,b.rn,count(*) sl,count(distinct b.a) asl
from bi_ods_ga.ods_app_sdk_log a
lateral view json_tuple(a.ecp,'rn','atp','a') b as rn,atp,a
where a.dt between '2019-05-04' and '2019-06-05'
and a.ec='02' and a.ea in('01','02') and a.av regexp '^9.3.2[0-9]|^9.3.[3-9]|^9.[4-9]' and b.atp='3'
group by a.dt,b.rn) a
inner join tmp_tags b on a.rn=b.name;

--3.关注信息流点击数：GA埋点规则
--事件类别：关注
--事件操作：首页关注_站内文章点击
--事件标签： 来源类型:标签
create table bi_test.zyl_tmp_190611_4 as
select a.dt,regexp_extract(a.hits_eventinfo_eventlabel,'_([^_]+)',1) keyword,count(*) click
from bi_dw_ga.fact_ga_hits_data a
inner join tmp_tags b on regexp_extract(a.hits_eventinfo_eventlabel,'_([^_]+)',1)=b.name
where a.dt between '2019-05-04' and '2019-06-05' and a.hits_appinfo_appversion regexp '^9.3.2[0-9]|^9.3.[3-9]|^9.[4-9]'
and a.hits_eventinfo_eventcategory='关注' and a.hits_eventinfo_eventaction='首页关注_站内文章点击' and regexp_extract(a.hits_eventinfo_eventlabel,'([^_]+)_',1)='话题'
group by a.dt,regexp_extract(a.hits_eventinfo_eventlabel,'_([^_]+)',1);

--4.关注-好价详情电商点击：GA埋点规则
--事件操作：添加到购物车
--自定义维度：购物车：中台拆分=G1 包含tag=“标签名”
create table bi_test.zyl_tmp_190611_5 as
select a.dt,regexp_extract(a.dim69,'tag=([^_]+)',1) keyword,count(*) event
from bi_dw_ga.fact_ga_hits_data a
inner join tmp_tags b on regexp_extract(a.dim69,'tag=([^_]+)',1)=b.name
where a.dt between '2019-05-04' and '2019-06-05' and a.hits_appinfo_appversion regexp '^9.3.2[0-9]|^9.3.[3-9]|^9.[4-9]'
and a.hits_eventinfo_eventaction='添加到购物车' and dim69 regexp '(?i)G1'
group by a.dt,regexp_extract(a.dim69,'tag=([^_]+)',1);



insert overwrite local directory '/data/tmp/zhaoyulong/data' row format delimited fields terminated by '\t'
select a.dt,a.keyword,a.ljgz,a.yx_count,
b.sl,b.pv,b.event,
c.bgl,c.asl,d.click,e.event
from bi_test.zyl_tmp_190611_1 a
left join bi_test.zyl_tmp_190611_2 b on a.keyword=b.keyword and a.dt=b.dt
left join bi_test.zyl_tmp_190611_3 c on a.keyword=c.keyword and a.dt=c.dt
left join bi_test.zyl_tmp_190611_4 d on a.keyword=d.keyword and a.dt=d.dt
left join bi_test.zyl_tmp_190611_5 e on a.keyword=e.keyword and a.dt=e.dt
order by keyword,dt;
