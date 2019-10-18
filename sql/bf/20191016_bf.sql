select regexp_extract('规则类型_规则名称_文章频道_文章id_用户id_文章点击时间_文章类型ID','.*\\_([^_]+)_([^_]+)_([^_]+)_([^_]+)',1);
select regexp_extract('规则类型_规则名称_文章频道_文章id_用户id_文章点击时间_文章类型ID','.*\\_([^_]+)_([^_]+)_([^_]+)_([^_]+)',2);


mysql -h10.9.180.92 -P3306 -udw_olap_etl -pfn40TPAbTt21nz
select table_name,
concat(round(data_length/1024/1024,2),'MB') as data_length_MB,
concat(round(index_length/1024/1024,2),'MB') as index_length_MB
from information_schema.tables
where  table_schema='dwa_olap'
order by data_length desc;
