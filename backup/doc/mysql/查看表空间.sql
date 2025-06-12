mysql -h10.9.180.92 -P3306 -udw_olap_etl -pfn40TPAbTt21nz
select table_name,
concat(round(data_length/1024/1024,2),'MB') as data_length_MB,
concat(round(index_length/1024/1024,2),'MB') as index_length_MB
from information_schema.tables
where  table_schema='user_offline_data_interface'
order by data_length desc;
