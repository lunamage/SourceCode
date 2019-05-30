select b.ct,count(*) sl
from bi_ods_ga.ods_app_sdk_log a
lateral view json_tuple(a.ecp,'tv','ct','sp','a','c') b as tv,ct,sp,a,c
where a.dt between '2019-05-25' and '2019-05-25' and a.ec='01' and a.ea='01' and b.sp='0'
and a.av regexp '^9.[4-9]'
group by b.ct order by sl desc limit 50;
