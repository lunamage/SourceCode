select dt,mall_type,cate_level1,sum(exposure) exposure,sum(click) click,sum(event) event
from bi_app_ga.app_recommend_mall_cate1_s_h
where dt between '2019-05-08' and '2019-05-14'
group by dt,mall_type,cate_level1 order by dt,mall_type,cate_level1;
