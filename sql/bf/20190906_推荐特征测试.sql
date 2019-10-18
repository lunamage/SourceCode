

cache table tmp as
select '点击' t1,'001' user_id,'00:00:01' time,'电脑数码' cate1
union all
select '曝光' t1,'001' user_id,'00:00:02' time,'' cate1
union all
select '曝光' t1,'001' user_id,'00:00:03' time,'' cate1
union all
select '点击' t1,'001' user_id,'00:00:04' time,'日用百货' cate1
union all
select '曝光' t1,'001' user_id,'00:00:05' time,'' cate1;

select t1,user_id,time,
if(rank<>0,first_value(cate1)over(partition by user_id,rank order by time),'') cate1_new
from(
select sum(if(t1='点击',1,0))over(partition by user_id order by time) rank,
t1,user_id,time,cate1
from tmp) a;



/usr/hdp/2.6.3.0-235/kafka/bin/kafka-console-consumer.sh  --zookeeper 10.42.62.211:2181,10.42.2.192:2181,10.42.170.247:2181  --topic search_feature_mid_log
flink run -m yarn-cluster -c search.query.QueryRealtimeToKafka -yqu bi -ynm QueryToKafka -p 4 -yn 4 -ys 4 -ytm 40480 stream-1.0.jar &


sudo systemctl enable docker
sudo systemctl start docker
docker-compose up -d
docker-compose exec sql-client ./sql-client.sh

docker images

docker run -t fhueske/flink-sql-client-training-1.7.2 /bin/bash


docker-compose exec sql-client ./sql-client.sh
docker-compose exec sql-client cat ./sql-client.sh

/usr/local/service/flink-1.9.0/bin/start-cluster.sh
/usr/local/service/flink-1.9.0/bin/sql-client.sh embedded






spark2-sql --jars /opt/app/hdp/2.6.3.0-235/hive2/lib/mysql-connector-java-commercial-5.1.40-bin.jar --driver-class-path /opt/app/jars/mysql-connector-java-commercial-5.1.40-bin.jar --name 'home_exposure_summary_playback_v5' --driver-memory 4g --executor-cores 4 --master yarn --executor-memory 10g --num-executors 40 --conf spark.default.parallelism=400 --conf spark.sql.shuffle.partitions=400 --conf spark.driver.maxResultSize=3g --queue bi -d tx_date=2019-09-04 -d begin_date=2019-09-05 -d end_date=2019-09-05 -v -f /data/source/data_warehouse/ga/script/app/recommend/ctr/home_exposure_playback_v4/home_exposure_summary_playback_v5.sql




search.query.QueryRealtimeToKafka
search.query.QueryRealtimeToRedis


/usr/hdp/2.6.3.0-235/kafka/bin/kafka-console-consumer.sh  --zookeeper hadoop001:2181,hadoop002:2181,hadoop003:2181  --topic app-sdk-log-simplify-repeat|less


redis-cli -h 10.45.1.32 -p 6379



redis-cli -h 10.19.99.78 -p 6379


flink run -m yarn-cluster -c search.query.QueryRealtimeToKafka -yqu bi -ynm QueryToKafka -p 4 -yn 4 -ys 4 -ytm 320480 stream-1.0.jar &

flink run -m yarn-cluster -c search.query.QueryRealtimeToRedis -yqu bi -ynm QueryRealtimeToRedis -p 4 -yn 4 -ys 4 -ytm 40480 stream-1.0.jar &

flink run   -m yarn-cluster -c search.query.QueryRealtimeToKafka -yqu bi -ynm QueryToKafka -p 3 -yn 4 -ys 4 -ytm 205929 stream-1.0.jar &

flink run   -m yarn-cluster -c search.query.QueryRealtimeToKafka -yqu bi -ynm QueryToKafka -p 3 -yn 4 -ys 4 -ytm 105929 stream-1.0.jar &


–conf spark.executor.extraJavaOptions="-Dfile.encoding=UTF-8 -Dsun.jnu.encoding=UTF-8"



flink run -m yarn-cluster -c search.query.QueryRealtime -yqu bi -ynm QueryRealtime -p 4 -yn 4 -ys 4 -ytm 40480 stream-1.0.jar &

-p4 -yn 4 -ys 1 -ytm 40480


Cannot instantiate file system for URI: hdfs://HDFS80727/bi/flink/checkpoint



uclicklast_uid	cate1_id	updatetime
uclicklast_uid	cate2_id	updatetime
uclicklast_uid	cate3_id	updatetime
uclicklast_uid	cate4_id	updatetime
uclicklast_uid	brand_id	updatetime
uclicklast_uid	mall_id	updatetime

uclicklast_1234567890	cate1_121	时间戳


artread_文章id	avgtime	33
artread_文章id	avgfinish	0.66


artread_文章id	avgtime	33
artread_文章id	avgfinish	0.66

if(mall == null || !mall.matches("^[0-9]+$") || !Utils.isContainsMall(mall)) {return;}



/usr/hdp/2.6.3.0-235/flink-1.9.0/bin/flink run -m yarn-cluster -c recommend.uClickLast.uClickLastFeature -ynm uClickLastFeature -p 1 stream-1.0.jar &
/usr/hdp/2.6.3.0-235/flink-1.9.0/bin/flink run -m yarn-cluster -c recmmend.artRead.ArtReadFeature -ynm ArtReadFeature stream-1.0.jar &


redis-cli -h 10.9.31.154 -p 6379
keys uclicklast*
keys artread*

yarn logs -applicationId application_1565767801119_28396>log.txt


yarn logs -applicationId application_1559641511680_28097|grep 15701312

redis-cli -h 10.9.15.111 -p 6379
keys ib_15701307_3


/usr/hdp/2.6.3.0-235/kafka/bin/kafka-console-consumer.sh  --zookeeper hadoop001:2181,hadoop002:2181,hadoop003:2181  --topic app-sdk-log-simplify-repeat|grep jIl6FtU2LCYdjZoopb5yxLrC8xUuVzdEK1D0egM1wDzep
/usr/hdp/2.6.3.0-235/kafka/bin/kafka-console-consumer.sh  --zookeeper hadoop001:2181,hadoop002:2181,hadoop003:2181  --topic analytics-zcollect|grep 详情页阅读


10.9.31.154

665078


Pre-bundled Hadoop 2.8.3








select regexp_extract(a.el,'([^_]+)',1) id,count(*) sl
from bi_ods_ga.ods_app_sdk_log a
lateral view json_tuple(a.ecp,'11') b as cd11
where a.dt='2019-09-06'
and a.ec='详情页' and a.ea='详情页阅读' and cd11 in('youhui','faxian','haitao')
group by regexp_extract(a.el,'([^_]+)',1) order by sl desc;



"ib_%s_%s"

String.format("ib_%s_%s", "123456789", 3)


ib_123456789_3

jedis.pool.feature.m1=10.9.188.114
jedis.pool.feature.m2=10.9.5.117


redis-cli -h 10.9.188.114 -p 6379

EXISTS ib_15969601_3
type ib_15969601_3

1567659020
hget "rqac_7234" 637234_3h


/usr/hdp/2.6.3.0-235/kafka/bin/kafka-console-consumer.sh  --zookeeper 10.42.62.211:2181,10.42.2.192:2181,10.42.170.247:2181  --topic search_feature_mid_log|grep 51119_3h

redis-cli -h 10.19.169.22 -p 6379
redis-cli -h 10.19.82.107 -p 6379

10.19.82.107
hgetall uclicklast_8266856703


flink run -s hdfs://HDFS80727/bi/flink/savepoint/savepoint-b09236-aa917f41a359 -m yarn-cluster -c search.query.QueryRealtime -yqu bi -ynm QueryRealtime -p 3 -yn 3 -ys 1 -ytm 155929 -yD env.java.opts="-Dfile.encoding=UTF-8 -Dsun.jnu.encoding=UTF-8" stream-1.0.jar &



recommend.uClickLast.uClickLastFeature
recmmend.artRead.ArtReadFeature

search.query.QueryRealtimeToKafka
search.query.QueryRealtimeToRedis

flink run -s hdfs://cluster/bi/flink_checkpoint/savepoint-1a2c9c-4ef69109531b -q -m yarn-cluster -c search.query.QueryRealtime -yqu bi -ynm queryRealtime -p 4 -yn 4 -ys 1 -ytm 40480 datastream-1.1.jar &

http://hadoop001:8088/cluster/scheduler
http://10.45.0.51:5004/cluster/scheduler


2019-08-07





搜索特征redis（10.19.131.185）之前由于负载较高，取消了持久化；现在已将部分特征迁移，申请增加持久化，请批准。

/usr/hdp/2.6.3.0-235/flink-1.9.0/bin/flink run -m yarn-cluster -c recommend.totalClickEvent.TotalClickEventFeature -ynm TotalClickEventFeature stream-1.0.jar &


周二 周四 周会

flink分享
