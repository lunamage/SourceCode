#kafka
/usr/hdp/2.6.3.0-235/kafka/bin/kafka-topics.sh --zookeeper zk01:2181,zk02:2181,zk03:2181 --list
/usr/hdp/2.6.3.0-235/kafka/bin/kafka-topics.sh --zookeeper hadoop001:2181,hadoop002:2181,hadoop003:2181 --describe --topic app-sdk-log
/usr/hdp/2.6.3.0-235/kafka/bin/kafka-topics.sh --zookeeper hadoop001:2181,hadoop002:2181,hadoop003:2181 --create --topic zyltest --partitions 1  --replication-factor 1
/usr/hdp/2.6.3.0-235/kafka/bin/kafka-console-producer.sh --broker-list hadoop001:6667,hadoop002:6667,hadoop003:6667 --topic zyltest
/usr/hdp/2.6.3.0-235/kafka/bin/kafka-console-consumer.sh  --zookeeper hadoop001:2181,hadoop002:2181,hadoop003:2181  --topic analytics-zcollect |grep 详情页阅读
/usr/hdp/2.6.3.0-235/kafka/bin/kafka-console-consumer.sh  --zookeeper hadoop001:2181,hadoop002:2181,hadoop003:2181  --topic analytics-zcollect --from-beginning|grep 5786557172|grep 搜索|grep 点击
/usr/hdp/2.6.3.0-235/kafka/bin/kafka-console-consumer.sh  --zookeeper hadoop001:2181,hadoop002:2181,hadoop003:2181  --topic analytics-zcollect --max-messages 10
/usr/hdp/2.6.3.0-235/kafka/bin/kafka-run-class.sh kafka.tools.ConsumerOffsetChecker

/usr/hdp/2.6.3.0-235/kafka/bin/kafka-console-consumer.sh  --zookeeper hadoop001:2181,hadoop002:2181,hadoop003:2181  --topic app-sdk-log-simplify-repeat --max-messages 10


/usr/hdp/2.6.3.0-235/kafka/bin/kafka-console-producer.sh --broker-list hadoop001:6667,hadoop002:6667,hadoop003:6667 --topic analytics-zcollect
#test
/usr/local/service/flink-1.9.0/bin/start-cluster.sh
/usr/local/service/flink-1.9.0/bin/sql-client.sh embedded

/usr/hdp/2.6.3.0-235/flink-1.9.0/bin/flink run -m yarn-cluster -c recommend.uClickLast.uClickLastFeature -ynm uClickLastFeature -p 1 stream-1.0.jar &
/usr/hdp/2.6.3.0-235/flink-1.9.0/bin/flink run -m yarn-cluster -c recommend.artRead.ArtReadFeatureNew -ynm ArtReadFeatureNew stream-1.0.jar --istest 1 &
/usr/hdp/2.6.3.0-235/flink-1.9.0/bin/flink run -m yarn-cluster -c recommend.jdEventMaxDate.JDEventMaxDate -ynm JDEventMaxDate stream-1.0.jar --istest 1 &

yarn logs -applicationId application_1568719445207_588611>log.txt

redis-cli -h 10.19.99.78 -p 6379

flink savepoint 5adb05ceb527b20f2c4138ae53596745 hdfs://HDFS80727/bi/flink/savepoint -yid application_1568719445207_149209
flink run -s hdfs://HDFS80727/bi/flink/savepoint/savepoint-5adb05-c531f0b08a24 -m yarn-cluster -c search.query.QueryRealtime -yqu bi -ynm QueryRealtime -p 8  -yn 4 -ys 2 -ytm 162400 -yD env.java.opts="-Dfile.encoding=UTF-8 -Dsun.jnu.encoding=UTF-8" /data/tmp/zhaoyulong/stream-1.0.jar &




/usr/hdp/2.6.3.0-235/flink-1.9.0/bin/flink run -m yarn-cluster -c sdk.smzdm.userEvent.UserEvent -ynm UserEvent stream-1.0.jar &
------------------
redis-cli -h 10.42.168.37 -p 6379
auth wazhHcz52cchC1IlUF

redis-cli -h 10.19.99.78 -p 6379

LLEN sendmsg_data

yarn logs -applicationId application_1577083835688_5214>log.txt

/usr/hdp/2.6.3.0-235/flink-1.9.0/bin/flink run -m yarn-cluster -c sdk.smzdm.userEvent.UserEvent -ynm UserEvent stream-1.0.jar &

flink run -m yarn-cluster -c sdk.smzdm.userEvent.UserEvent -yqu bi -ynm UserEvent -p 4 -yn 4 -ys 1 -ytm 20480 -yD env.java.opts="-Dfile.encoding=UTF-8 -Dsun.jnu.encoding=UTF-8" stream-1.0.jar &


/usr/hdp/2.6.3.0-235/kafka/bin/kafka-console-consumer.sh  --zookeeper hadoop001:2181,hadoop002:2181,hadoop003:2181  --topic analytics-zcollect --from-beginning|grep 7048145441|grep 添加到购物车

flink savepoint f47c3244f80468c9ab8e300a0bd87eed hdfs://HDFS80727/bi/flink/savepoint -yid application_1568719445207_150038
flink run -s hdfs://HDFS80727/bi/flink/savepoint/savepoint-f47c32-84e7206db400 -m yarn-cluster -c recommend.report.hourMalltype.HourMalltype -ynm HourMalltype -p 4  -yn 4 -ys 1 -ytm 20400 -yD env.java.opts="-Dfile.encoding=UTF-8 -Dsun.jnu.encoding=UTF-8" /data/tmp/zhaoyulong/stream-1.0.jar &


flink run -m yarn-cluster -c recommend.report.hourMalltype.HourMalltype -ynm HourMalltype -p 16  -yn 4 -ys 4 -ytm 20400 -yD env.java.opts="-Dfile.encoding=UTF-8 -Dsun.jnu.encoding=UTF-8" /data/tmp/zhaoyulong/stream-1.0.jar &



/usr/hdp/2.6.3.0-235/flink-1.9.0/bin/flink run -m yarn-cluster -c recommend.userBuyCate4.UserBuyCate4 -ynm UserBuyCate4 stream-1.0.jar &

/usr/hdp/2.6.3.0-235/kafka/bin/kafka-console-consumer.sh  --zookeeper hadoop001:2181,hadoop002:2181,hadoop003:2181  --topic zyltest --from-beginning

yarn logs -applicationId application_1568719445207_563701>log.txt


/usr/hdp/2.6.3.0-235/kafka/bin/kafka-console-consumer.sh  --bootstrap-server 10.45.1.179:9092  --topic user_buy_cate

k8s
MallFeature
uClickLastFeature
ArtReadFeature
