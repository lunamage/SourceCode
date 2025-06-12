#kafka
/data/local/kafka_2.10-0.10.1.1/bin/kafka-topics.sh --zookeeper hadoop001:2181,hadoop002:2181,hadoop003:2181 --list
/data/local/kafka_2.10-0.10.1.1/bin/kafka-topics.sh --zookeeper hadoop001:2181,hadoop002:2181,hadoop003:2181 --describe --topic app-sdk-log
/data/local/kafka_2.10-0.10.1.1/bin/kafka-topics.sh --zookeeper hadoop001:2181,hadoop002:2181,hadoop003:2181 --create --topic zyltest --partitions 1  --replication-factor 1
/data/local/kafka_2.10-0.10.1.1/bin/kafka-console-producer.sh --broker-list hadoop001:6667,hadoop002:6667,hadoop003:6667 --topic zyltest
/data/local/kafka_2.10-0.10.1.1/bin/kafka-console-consumer.sh  --zookeeper hadoop001:2181,hadoop002:2181,hadoop003:2181  --topic analytics-zcollect --from-beginning|grep 推送点击|grep 70062968
/data/local/kafka_2.10-0.10.1.1/bin/kafka-console-consumer.sh  --zookeeper hadoop001:2181,hadoop002:2181,hadoop003:2181  --topic analytics-zcollect --from-beginning|grep 5786557172|grep 搜索|grep 点击
/data/local/kafka_2.10-0.10.1.1/bin/kafka-console-consumer.sh  --zookeeper hadoop001:2181,hadoop002:2181,hadoop003:2181  --topic analytics-zcollect --max-messages 10
/data/local/kafka_2.10-0.10.1.1/bin/kafka-run-class.sh kafka.tools.ConsumerOffsetChecker

/data/local/kafka_2.10-0.10.1.1/bin/kafka-console-consumer.sh --bootstrap-server 10.45.1.179:9092 --topic gmv_olap_order --max-messages 10


/data/local/kafka_2.10-0.10.1.1/bin/kafka-console-producer.sh --broker-list hadoop001:6667,hadoop002:6667,hadoop003:6667 --topic analytics-zcollect --max-messages 10
/data/local/kafka_2.10-0.10.1.1/bin/kafka-console-consumer.sh --bootstrap-server 10.45.4.146:9092 --topic youhui --from-beginning |grep youhui_meta |grep 26515904 > log.txt


/data/local/kafka_2.10-0.10.1.1/bin/kafka-topics.sh --zookeeper 10.10.49.125:2181 --list
/data/local/kafka_2.10-0.10.1.1/bin/kafka-topics.sh --zookeeper 10.10.49.125:2181 --create --topic olap_rec_article_sdk_gmv_t55 --partitions 1  --replication-factor 1


#test
/usr/local/service/flink-1.9.0/bin/start-cluster.sh
/usr/local/service/flink-1.9.0/bin/sql-client.sh embedded

/usr/hdp/2.6.3.0-235/flink-1.9.0/bin/flink run -m yarn-cluster -c recommend.uClickLast.uClickLastFeature -ynm uClickLastFeature -p 1 stream-1.0.jar &
/usr/hdp/2.6.3.0-235/flink-1.9.0/bin/flink run -m yarn-cluster -c recommend.artRead.ArtReadFeatureNew -ynm ArtReadFeatureNew stream-1.0.jar --istest 1 &
/usr/hdp/2.6.3.0-235/flink-1.9.0/bin/flink run -m yarn-cluster -c recommend.jdEventMaxDate.JDEventMaxDate -ynm JDEventMaxDate stream-1.0.jar --istest 1 &
/usr/hdp/2.6.3.0-235/flink-1.9.0/bin/flink run -m yarn-cluster -c sdk.smzdm.pushOpen.PushOpen -ynm PushOpen stream-1.0.jar --istest 1 &
/usr/hdp/2.6.3.0-235/flink-1.9.0/bin/flink run -m yarn-cluster -c recommend.artKind.ArtKindFeature -ynm ArtKindFeature stream-1.0.jar --istest 1 &
/usr/hdp/2.6.3.0-235/flink-1.9.0/bin/flink run -m yarn-cluster -c recommend.article.cmsTop.CmsTopFeature -ynm CmsTopFeature stream-1.0.jar --istest 1 &
/data/local/kafka_2.10-0.10.1.1/bin/kafka-console-consumer.sh --bootstrap-server 10.10.49.125:9092,10.10.31.23:9092,10.10.182.32:9092 --topic olap_rec_article_sdk_gmv_t55 --max-messages 10



yarn logs -applicationId application_1588492312649_783578>log.txt
yarn logs -applicationId application_1588492312649_977819|less


redis-cli -h 10.45.3.110 -p 6379

flink savepoint 48cb321a7a74eb57a772d165c2704fbc hdfs://HDFS80727/bi/flink/savepoint -yid application_1579511008386_754935
flink run -s hdfs://HDFS80727/bi/flink/savepoint/savepoint-5adb05-c531f0b08a24 -m yarn-cluster -c search.query.QueryRealtime -yqu bi -ynm QueryRealtime -p 8  -yn 4 -ys 2 -ytm 162400 -yD env.java.opts="-Dfile.encoding=UTF-8 -Dsun.jnu.encoding=UTF-8" /data/tmp/zhaoyulong/stream-1.0.jar &

/data/local/flink-1.11.1/bin/flink savepoint b2f1fc410e6b78b164a3e38a219a55b8 hdfs://HDFS80727/bi/flink/savepoint -yid application_1588492312649_743211





/usr/hdp/2.6.3.0-235/flink-1.9.0/bin/flink run -m yarn-cluster -c sdk.smzdm.userEvent.UserEvent -ynm UserEvent stream-1.0.jar &
------------------
redis-cli -h 10.45.3.110 -p 6379
auth wazhHcz52cchC1IlUF

redis-cli -h 10.19.99.78 -p 6379

LLEN sendmsg_data

yarn logs -applicationId application_1588492312649_323631>log.txt

/usr/hdp/2.6.3.0-235/flink-1.9.0/bin/flink run -m yarn-cluster -c sdk.smzdm.userEvent.UserEvent -ynm UserEvent stream-1.0.jar &

flink run -m yarn-cluster -c sdk.smzdm.userEvent.UserEvent -yqu bi -ynm UserEvent -p 4 -yn 4 -ys 1 -ytm 20480 -yD env.java.opts="-Dfile.encoding=UTF-8 -Dsun.jnu.encoding=UTF-8" stream-1.0.jar &


/data/local/kafka_2.10-0.10.1.1/bin/kafka-console-consumer.sh  --zookeeper hadoop001:2181,hadoop002:2181,hadoop003:2181  --topic analytics-zcollect --from-beginning|grep 7048145441|grep 添加到购物车

flink savepoint 848d9088bf7c4354dc7fba200b900f1f hdfs://HDFS80727/bi/flink/savepoint -yid application_1588492312649_284291
flink run -s hdfs://HDFS80727/bi/flink/savepoint/savepoint-f47c32-84e7206db400 -m yarn-cluster -c recommend.report.hourMalltype.HourMalltype -ynm HourMalltype -p 4  -yn 4 -ys 1 -ytm 20400 -yD env.java.opts="-Dfile.encoding=UTF-8 -Dsun.jnu.encoding=UTF-8" /data/tmp/zhaoyulong/stream-1.0.jar &


flink run -m yarn-cluster -c recommend.report.hourMalltype.HourMalltype -ynm HourMalltype -p 16  -yn 4 -ys 4 -ytm 20400 -yD env.java.opts="-Dfile.encoding=UTF-8 -Dsun.jnu.encoding=UTF-8" /data/tmp/zhaoyulong/stream-1.0.jar &



/usr/hdp/2.6.3.0-235/flink-1.9.0/bin/flink run -m yarn-cluster -c recommend.userBuyCate4.UserBuyCate4 -ynm UserBuyCate4 stream-1.0.jar &

/data/local/kafka_2.10-0.10.1.1/bin/kafka-console-consumer.sh  --zookeeper hadoop001:2181,hadoop002:2181,hadoop003:2181  --topic zyltest --from-beginning

yarn logs -applicationId application_1579511008386_361723>log.txt


/data/local/kafka_2.10-0.10.1.1/bin/kafka-console-consumer.sh  --bootstrap-server 10.45.1.179:9092  --topic user_buy_cate

k8s
MallFeature
uClickLastFeature
ArtReadFeature


/data/local/flink-1.10.0/bin/sql-client.sh embedded -e ./conf/zyl.yaml

CREATE TABLE sdk_log (
  type STRING,
  event STRING,

) WITH (
  'connector.type' = 'kafka',
  'connector.version' = '0.10',
--  'connector.topic' = 'app-sdk-log',
  'connector.topic' = 'app-sdk-log-simplify-repeat',
  'connector.startup-mode' = 'latest-offset',
  'connector.properties.zookeeper.connect' = '10.42.35.191:2181,10.42.6.198:2181,10.42.76.238:2181,10.42.190.100:2181,10.42.70.233:2181,10.42.112.26:2181',
  'connector.properties.bootstrap.servers' = '10.42.35.191:9092,10.42.6.198:9092,10.42.76.238:9092,10.42.190.100:9092,10.42.70.233:9092,10.42.112.26:9092',
  'format.type' = 'json',
  'format.fail-on-missing-field' = 'false',
  'format.json-schema' =
    '{
      "type": "object",
      "properties": {
        "ec": {
          "type": "string"
        },
        "ea": {
          "type": "string"
        }
      }
    }'
);

/data/local/flink-1.10.0/bin/flink run -m yarn-cluster -yqu bitmp -ynm sqltest -p 2 -ys 2 -ytm 12288 -yD env.java.opts="-Dfile.encoding=UTF-8 -Dsun.jnu.encoding=UTF-8" /data/tmp/zhaoyulong/test/flinksql-1.1.jar --f /data/tmp/zhaoyulong/test/a1.sql &

redis-cli -h 10.42.168.37 -p 6379
redis-cli -h 10.42.168.37 keys "*" | xargs redis-cli -h 10.42.168.37 del
redis-cli -h 10.42.126.216 -a wazhHcz52cchC1IlUF  keys "tmp*" | xargs redis-cli -h 10.42.126.216 -a wazhHcz52cchC1IlUF del

redis-cli -h 10.45.4.60 scan 0 count 10000 | xargs redis-cli -h 10.45.4.60 del



/data/local/flink-1.11.1/bin/sql-client.sh embedded -l /data/tmp/zhaoyulong/test2/

/data/local/flink-1.11.0/bin/stop-cluster.sh
/data/local/flink-1.10.0/bin/stop-cluster.sh
/data/local/flink-1.9.0/bin/stop-cluster.sh
/data/local/flink-1.11.1/bin/start-cluster.sh


scan 0 match ta* count 10
