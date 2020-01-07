select * from(
select cd11,cd20,cd30,a.el,
row_number()over(partition by cd11 order by rand()) r
from bi_ods_ga.ods_app_sdk_log a
lateral view json_tuple(a.ecp,'11','20','30') b as cd11,cd20,cd30
where a.dt='2020-01-02' and a.ec='详情页' and a.ea='详情页阅读' and cd20 in('3','76')) a
where r<3 order by cd20;
