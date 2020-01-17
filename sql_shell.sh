hive --hiveconf hive.cli.print.current.db=true --hiveconf hive.cli.print.header=true --hiveconf hive.execution.engine=tez


spark2-sql --name 'zyl_test' --queue bi --driver-memory 4g --executor-cores 6 --master yarn --executor-memory 8g --num-executors 30 --conf spark.default.parallelism=800 --conf spark.sql.shuffle.partitions=800 --conf spark.scheduler.listenerbus.eventqueue.capacity=100000

spark2-sql --jars /data/source/data_warehouse/pub_jar/mysql-connector-java-commercial-5.1.40-bin.jar --driver-class-path /data/source/data_warehouse/pub_jar/mysql-connector-java-commercial-5.1.40-bin.jar --name zyl_test --driver-memory 3g --executor-cores 4 --master yarn --executor-memory 20g --num-executors 20 --conf spark.default.parallelism=600 --conf spark.sql.shuffle.partitions=600 --conf spark.driver.maxResultSize=3g --conf spark.scheduler.listenerbus.eventqueue.capacity=100000 --queue bi


spark2-sql --jars /data/source/data_warehouse/pub_jar/mysql-connector-java-commercial-5.1.40-bin.jar --driver-class-path /data/source/data_warehouse/pub_jar/mysql-connector-java-commercial-5.1.40-bin.jar --name zyl_test --driver-memory 3g --executor-cores 4 --master yarn --executor-memory 8g --num-executors 30 --conf spark.default.parallelism=600 --conf spark.sql.shuffle.partitions=600 --conf spark.driver.maxResultSize=3g --conf spark.scheduler.listenerbus.eventqueue.capacity=100000 --queue bitmp



spark2-sql --name 'zyl_test' --queue bi --driver-memory 4g --executor-cores 6 --master yarn --executor-memory 18g --num-executors 22 --conf spark.default.parallelism=800 --conf spark.sql.shuffle.partitions=800 --conf spark.scheduler.listenerbus.eventqueue.capacity=100000
