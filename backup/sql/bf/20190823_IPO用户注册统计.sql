select dt,count(*) event
from bi_dw_ga.fact_ga_hits_data a
where a.dt between '2019-01-01' and '2019-06-30' and a.hits_ecommerceaction_action_type=3
and (a.hits_eventinfo_eventcategory like '%电商点击%' or (a.hits_eventinfo_eventaction like '%添加到购物车%' and (hits_appinfo_appid='com.smzdm.client.android' or hits_appinfo_appid='com.smzdm.client.ios')))
group by dt order by dt;


注册用户除了总数，还得给我下封禁用户数

SELECT dt,COUNT(*) tot_reg
FROM stg.db_userlog_register_log
WHERE dt BETWEEN '2018-01-01' AND '2019-06-30'
AND register_from <> 'job_yuanchuang'
GROUP BY dt
ORDER BY dt;


SELECT a.dt,COUNT(*) tot_reg
FROM stg.db_userlog_register_log a
inner  JOIN bi_dw.fact_user_base b ON a.user_id = b.user_id AND b.is_forbidden = 0
WHERE a.dt BETWEEN '2018-01-01' AND '2019-06-30' AND a.register_from <> 'job_yuanchuang'
and b.forbid_days = '2038'
GROUP BY a.dt
ORDER BY dt;
