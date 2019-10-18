/usr/local/service/flink-1.9.0/bin/start-cluster.sh
/usr/local/service/flink-1.9.0/bin/sql-client.sh embedded

/usr/hdp/2.6.3.0-235/flink-1.9.0/bin/flink run -m yarn-cluster -c recommend.uClickLast.uClickLastFeature -ynm uClickLastFeature -p 1 stream-1.0.jar &
/usr/hdp/2.6.3.0-235/flink-1.9.0/bin/flink run -m yarn-cluster -c recmmend.artRead.ArtReadFeature -ynm ArtReadFeature stream-1.0.jar &

yarn logs -applicationId application_1565767801119_28396>log.txt
