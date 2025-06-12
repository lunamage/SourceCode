set hive.exec.dynamic.partition=true; // 是否允许动态生成分区
set hive.exec.dynamic.partition.mode=nonstrict; // 是否容忍指定分区全部动态生成
set hive.exec.max.dynamic.partitions = 100; // 动态生成的最多分区数2.运行行为
set spark.sql.autoBroadcastJoinThreshold; // 大表 JOIN 小表，小表做广播的阈值
set spark.dynamicAllocation.enabled;  // 开启动态资源分配
set spark.dynamicAllocation.maxExecutors;  //开启动态资源分配后，最多可分配的Executor数
set spark.dynamicAllocation.minExecutors;  //开启动态资源分配后，最少可分配的Executor数
set spark.sql.shuffle.partitions;  // 需要shuffle是mapper端写出的partition个数
set spark.sql.adaptive.enabled;  // 是否开启调整partition功能，如果开启，
set spark.sql.shuffle.partitions; 设置的partition可能会被合并到一个reducer里运行
set spark.sql.adaptive.shuffle.targetPostShuffleInputSize;  //开启spark.sql.adaptive.enabled后，两个partition的和低于该阈值会合并到一个reducer
set spark.sql.adaptive.minNumPostShufflePartitions;  // 开启spark.sql.adaptive.enabled后，最小的分区数
set spark.hadoop.mapreduce.input.fileinputformat.split.maxsize;  //当几个stripe的大小大于该值时，会合并到一个task中处理3.executor能力
set spark.executor.memory;  // executor用于缓存数据、代码执行的堆内存以及JVM运行时需要的内存
set spark.yarn.executor.memoryOverhead;  //Spark运行还需要一些堆外内存，直接向系统申请，如数据传输时的netty等。
set spark.sql.windowExec.buffer.spill.threshold;  //当用户的SQL中包含窗口函数时，并不会把一个窗口中的所有数据全部读进内存，而是维护一个缓存池，当池中的数据条数大于该参数表示的阈值时，spark将数据写到磁盘
set spark.executor.cores;  //单个executor上可以同时运行的task数4.GC优化
-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps // 打开GC打印-Xmn // 提高eden区大小-XX:NewRatio // 设置老年代是新生代的多少倍

set spark.driver.maxResultSize;
