#
clickhouse-client -h ch01
clickhouse-client -h ch02
10.45.1.125 ch01
10.45.1.198 ch02

#ddl
CREATE TABLE bi_test.app_olap_rec_user(`stat_dt` Nullable(String) COMMENT '日期',`abtest` Nullable(String) COMMENT 'abtest',`uid` Nullable(String) COMMENT '用户id',`did` Nullable(String) COMMENT '设备id md5',`isnp` Nullable(String) COMMENT '是否画像新用户',`device_type` Nullable(String) COMMENT '设备类型',`av` Nullable(String) COMMENT '版本',`exposure` Nullable(Int64) COMMENT '曝光',`click` Nullable(Int64) COMMENT '详情点击',`event` Nullable(Int64) COMMENT '电商点击',`regdate` Nullable(Int64) COMMENT '注册日距今天数',`status` Nullable(String) COMMENT '用户状态',`is_gold_bl` Nullable(String) COMMENT '是否金牌爆料团',`is_official` Nullable(String) COMMENT '是否官方账号',`shenghuojia_type` Nullable(String) COMMENT '是否生活家',`is_media` Nullable(String) COMMENT '是否媒体号',`checkin_num` Nullable(Int64) COMMENT '连续签到天数',`user_level` Nullable(Int64) COMMENT '用户等级',`sex` Nullable(String) COMMENT '性别',`age` Nullable(Int64) COMMENT '年龄',`city` Nullable(String) COMMENT '城市',`dt` Date) ENGINE = MergeTree PARTITION BY dt ORDER BY tuple() SETTINGS index_granularity = 8192;

CREATE TABLE app_sdk_indicators.app_sdk_indicators (`dim1` Nullable(String) COMMENT '维度1', `dim2` Nullable(String) COMMENT '维度2', `dim3` Nullable(String) COMMENT '维度3', `dim4` Nullable(String) COMMENT '维度4', `dim5` Nullable(String) COMMENT '维度5', `dim6` Nullable(String) COMMENT '维度6', `dim7` Nullable(String) COMMENT '维度7', `dim8` Nullable(String) COMMENT '维度8', `dim9` Nullable(String) COMMENT '维度9', `dim10` Nullable(String) COMMENT '维度10', `value` Nullable(Int64) COMMENT '指标值', `remark` Nullable(String) COMMENT '备注', `load_date` Nullable(String) COMMENT '时间戳', `dim` String COMMENT '维度编码，与文档对应', `v` String COMMENT '指标代码', `dt` Date)  ENGINE = MergeTree PARTITION BY dt ORDER BY (`dim`,`v`) SETTINGS index_granularity = 8192;
CREATE TABLE app_sdk_indicators.app_sdk_indicators (`dim1` Nullable(String) COMMENT '维度1', `dim2` Nullable(String) COMMENT '维度2', `dim3` Nullable(String) COMMENT '维度3', `dim4` Nullable(String) COMMENT '维度4', `dim5` Nullable(String) COMMENT '维度5', `dim6` Nullable(String) COMMENT '维度6', `dim7` Nullable(String) COMMENT '维度7', `dim8` Nullable(String) COMMENT '维度8', `dim9` Nullable(String) COMMENT '维度9', `dim10` Nullable(String) COMMENT '维度10', `value` Nullable(Int64) COMMENT '指标值', `remark` Nullable(String) COMMENT '备注', `load_date` Nullable(String) COMMENT '时间戳', `dim` String COMMENT '维度编码，与文档对应', `v` String COMMENT '指标代码', `dt` Date)  ENGINE = MergeTree(dt, (dim, v), 8192);


ALTER TABLE app_sdk_indicators.app_sdk_indicators DROP PARTITION '2019-12-24';

#语法
select toUnixTimestamp('2018-11-25 00:00:02');
select toDateTime(1543075202);

#同步
sh /data/source/data_warehouse/ga/script/pub/hdfs_to_clickhouse.sh "hdfs://hadoop001:8020/bi/app_ga/app_sdk_indicators" "10.45.1.125" "app_sdk_indicators" "app_sdk_indicators" "dim1,dim2,dim3,dim4,dim5,dim6,dim7,dim8,dim9,dim10,value,remark,load_date,dim,v,dt" "String,String,String,String,String,String,String,String,String,String,Int64,String,String,String,String,Date" "2019-12-25"


flink run -q -m yarn-cluster -c hdfsToClickhouse.HdfsToClickhouse -yqu bitmp -ynm dfsToClickhouse -yjm 6096 -p 12 -yn 6 -ys 2 -ytm 10240 -yD env.java.opts="-Dfile.encoding=UTF-8 -Dsun.jnu.encoding=UTF-8" /data/source/data_warehouse/ga/jar/batch-1.0.jar \
--hdfsPath "hdfs://hadoop001:8020/bi/app_ga/app_sdk_indicators/dt=2019-12-24" --hdfsPartition "2019-12-24" --schemaName "bi_test" --tableName "app_sdk_indicators" --clickhouseip "10.45.1.125" \
--columnName "dim1,dim2,dim3,dim4,dim5,dim6,dim7,dim8,dim9,dim10,value,remark,load_date,dim,v,dt" \
--columnType "String,String,String,String,String,String,String,String,String,String,Int64,String,String,String,String,Date"

flink run -q -m yarn-cluster -c hdfsToClickhouse.HdfsToClickhouse -yqu bitmp -ynm HdfsToClickhouse -yjm 6096 -p 12 -yn 6 -ys 2 -ytm 10240 -yD env.java.opts="-Dfile.encoding=UTF-8 -Dsun.jnu.encoding=UTF-8" /data/source/data_warehouse/ga/jar/batch-1.0.jar \
--hdfsPath "hdfs://hadoop001:8020/bi/app_ga/app_olap_rec_user/dt=2019-12-18" --hdfsPartition "2019-12-18" --schemaName "bi_test" --tableName "app_olap_rec_user" --clickhouseip "10.45.1.125" \
--columnName "stat_dt,abtest,uid,did,isnp,device_type,av,exposure,click,event,regdate,status,is_gold_bl,is_official,shenghuojia_type,is_media,checkin_num,user_level,sex,age,city,dt" \
--columnType "String,String,String,String,String,String,String,Int64,Int64,Int64,Int64,String,String,String,String,String,Int64,Int64,String,Int64,String,Date"


yarn logs -applicationId application_1568719445207_450415>log.txt
