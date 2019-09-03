package test.uv;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction.Context;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction.ReadOnlyContext;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import recommend.deviceType.UserDeviceFeature;
import recommend.deviceType.UserDeviceFeatureRedisSink;
import recommend.deviceType.UserDeviceFeatureSplitter;
import search.query.JdbcReader;
import search.query.QueryRealtimeSplitterBak;
import utils.ReadConfig;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;


/**
 * @author: zyl
 * @date: 2019/6/25
 * @time: 3:14 PM
 * @Description: sdk文章数据聚合
 */
public class StreamTest {
	
	private static Logger log = LoggerFactory.getLogger(UserDeviceFeature.class);
	
	private final static String filter1 = "\"isnp\":\"1\"";
    
    public static void main(String[] args) throws Exception {
    	
    	 StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
         env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
         env.enableCheckpointing(30000L);
         //env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000L);
         //env.setStateBackend((StateBackend) new RocksDBStateBackend("hdfs://cluster/bi/flink_checkpoint/searchquery3h",true));
         
      
         
         DataStream<String> myConsumer = env.readTextFile ("C:/Users/zhaoyulong/Downloads/sdkjson.txt");
         
         DataStream<Tuple2<String, Long>> d1 = myConsumer.filter((FilterFunction<String>) log -> {
             return log.contains(filter1);
         }).flatMap(new Splitter());
         
         //d1.keyBy(0).countWindowAll(100, 2).sum(1).print();
        	
         d1.keyBy(0).flatMap(new StateSplitter()).print();
         
         /*countWindowAll(100,2).reduce(new ReduceFunction<Tuple3<String,Long, Long>>() { //来一条数据处理一下,增量处理
             private static final long serialVersionUID = 1L;
             @Override
             public Tuple3<String, Long, Long> reduce(Tuple3<String,Long, Long> value1, Tuple3<String,Long, Long> value2)
                     throws Exception {
            	 return new Tuple3<String, Long, Long>(value1.f0, value1.f1 + value2.f1,value1.f2);
             }

         })
         .print();*/
    
         env.execute();
    }
 }
        



