#kafka
/usr/hdp/2.6.3.0-235/kafka/bin/kafka-topics.sh --zookeeper zk01:2181,zk02:2181,zk03:2181 --list
/usr/hdp/2.6.3.0-235/kafka/bin/kafka-topics.sh --zookeeper hadoop001:2181,hadoop002:2181,hadoop003:2181 --describe --topic app-sdk-log
/usr/hdp/2.6.3.0-235/kafka/bin/kafka-topics.sh --zookeeper hadoop001:2181,hadoop002:2181,hadoop003:2181 --create --topic zyltest --partitions 1  --replication-factor 1
/usr/hdp/2.6.3.0-235/kafka/bin/kafka-console-producer.sh --broker-list hadoop001:6667,hadoop002:6667,hadoop003:6667 --topic zyltest
/usr/hdp/2.6.3.0-235/kafka/bin/kafka-console-consumer.sh  --zookeeper hadoop001:2181,hadoop002:2181,hadoop003:2181  --topic analytics-zcollect --from-beginning
/usr/hdp/2.6.3.0-235/kafka/bin/kafka-console-consumer.sh  --zookeeper hadoop001:2181,hadoop002:2181,hadoop003:2181  --topic analytics-zcollect --from-beginning|grep 5786557172|grep 搜索|grep 点击
/usr/hdp/2.6.3.0-235/kafka/bin/kafka-console-consumer.sh  --zookeeper hadoop001:2181,hadoop002:2181,hadoop003:2181  --topic analytics-zcollect --max-messages 10
/usr/hdp/2.6.3.0-235/kafka/bin/kafka-run-class.sh kafka.tools.ConsumerOffsetChecker

/usr/hdp/2.6.3.0-235/kafka/bin/kafka-console-consumer.sh  --zookeeper hadoop001:2181,hadoop002:2181,hadoop003:2181  --topic app-sdk-log-simplify-repeat --max-messages 10

#test
/usr/local/service/flink-1.9.0/bin/start-cluster.sh
/usr/local/service/flink-1.9.0/bin/sql-client.sh embedded

/usr/hdp/2.6.3.0-235/flink-1.9.0/bin/flink run -m yarn-cluster -c recommend.uClickLast.uClickLastFeature -ynm uClickLastFeature -p 1 stream-1.0.jar &
/usr/hdp/2.6.3.0-235/flink-1.9.0/bin/flink run -m yarn-cluster -c recmmend.artRead.ArtReadFeature -ynm ArtReadFeature stream-1.0.jar &
/usr/hdp/2.6.3.0-235/flink-1.9.0/bin/flink run -m yarn-cluster -c recommend.jdEventMaxDate.JDEventMaxDate -ynm JDEventMaxDate stream-1.0.jar &

yarn logs -applicationId application_1568719445207_95462>log.txt

redis-cli -h 10.9.31.154 -p 6379

flink savepoint 5adb05ceb527b20f2c4138ae53596745 hdfs://HDFS80727/bi/flink/savepoint -yid application_1568719445207_149209
flink run -s hdfs://HDFS80727/bi/flink/savepoint/savepoint-5adb05-c531f0b08a24 -m yarn-cluster -c search.query.QueryRealtime -yqu bi -ynm QueryRealtime -p 8  -yn 4 -ys 2 -ytm 162400 -yD env.java.opts="-Dfile.encoding=UTF-8 -Dsun.jnu.encoding=UTF-8" /data/tmp/zhaoyulong/stream-1.0.jar &
