#kafka
/usr/hdp/2.6.3.0-235/kafka/bin/kafka-topics.sh --zookeeper zk01:2181,zk02:2181,zk03:2181 --list
/usr/hdp/2.6.3.0-235/kafka/bin/kafka-topics.sh --zookeeper hadoop001:2181,hadoop002:2181,hadoop003:2181 --describe --topic app-sdk-log
/usr/hdp/2.6.3.0-235/kafka/bin/kafka-topics.sh --zookeeper hadoop001:2181,hadoop002:2181,hadoop003:2181 --create --topic zyltest --partitions 1  --replication-factor 1
/usr/hdp/2.6.3.0-235/kafka/bin/kafka-console-producer.sh --broker-list hadoop001:6667,hadoop002:6667,hadoop003:6667 --topic zyltest
/usr/hdp/2.6.3.0-235/kafka/bin/kafka-console-consumer.sh  --zookeeper hadoop001:2181,hadoop002:2181,hadoop003:2181  --topic analytics-zcollect --from-beginning
/usr/hdp/2.6.3.0-235/kafka/bin/kafka-console-consumer.sh  --zookeeper hadoop001:2181,hadoop002:2181,hadoop003:2181  --topic analytics-zcollect --from-beginning|grep 5786557172|grep 搜索|grep 点击
/usr/hdp/2.6.3.0-235/kafka/bin/kafka-console-consumer.sh  --zookeeper hadoop001:2181,hadoop002:2181,hadoop003:2181  --topic analytics-zcollect --max-messages 10
/usr/hdp/2.6.3.0-235/kafka/bin/kafka-run-class.sh kafka.tools.ConsumerOffsetChecker

#test
/usr/local/service/flink-1.9.0/bin/start-cluster.sh
/usr/local/service/flink-1.9.0/bin/sql-client.sh embedded

/usr/hdp/2.6.3.0-235/flink-1.9.0/bin/flink run -m yarn-cluster -c recommend.uClickLast.uClickLastFeature -ynm uClickLastFeature -p 1 stream-1.0.jar &
/usr/hdp/2.6.3.0-235/flink-1.9.0/bin/flink run -m yarn-cluster -c recmmend.artRead.ArtReadFeature -ynm ArtReadFeature stream-1.0.jar &

yarn logs -applicationId application_1565767801119_28396>log.txt
