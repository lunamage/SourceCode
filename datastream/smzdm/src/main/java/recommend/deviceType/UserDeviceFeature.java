package recommend.deviceType;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
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
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.fs.bucketing.DateTimeBucketer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;

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
 * @date: 2019/7/25
 * @time: 3:14 PM
 * @Description: user device
 */
public class UserDeviceFeature {
	
	private static Logger log = LoggerFactory.getLogger(UserDeviceFeature.class);
	
	private final static String filter1 = "\"isnp\":\"1\"";
    
    public static void main(String[] args) throws Exception {
    	
    	 StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
         env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
         //env.enableCheckpointing(120000L);
         //env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000L);
         //env.setStateBackend((StateBackend) new RocksDBStateBackend("hdfs://cluster/bi/flink_checkpoint/searchquery3h",true));
         
         Properties properties = new Properties();
         properties.setProperty("bootstrap.servers",ReadConfig.getProperties("bootstrap.servers"));
         properties.setProperty("group.id", "UserDeviceFeature");
         FlinkKafkaConsumer<String> myConsumer = new FlinkKafkaConsumer<>(ReadConfig.getProperties("kafka.topic"), new SimpleStringSchema(), properties);
         myConsumer.setStartFromGroupOffsets();
         
         //DataStream<String> myConsumer = env.readTextFile ("C:/Users/zhaoyulong/Downloads/sdkjson.txt");
         
         DataStream<Tuple3<String, String, Long>> stream = env.addSource(myConsumer).filter((FilterFunction<String>) log -> {
             return log.contains(filter1);
         }).flatMap(new UserDeviceFeatureSplitter());
        		 
         //1.
         DataStream<Map<String, String>> streamMySql = env.addSource(new JdbcReader());
 		 //2、创建MapStateDescriptor规则，对广播的数据的数据类型的规则
         MapStateDescriptor <String,Map<String,String>> ruleStateDescriptor = new MapStateDescriptor <>("dimdevice",BasicTypeInfo.STRING_TYPE_INFO,new MapTypeInfo<>(String.class,String.class));
         //3、对conf进行broadcast返回BroadcastStream
         final BroadcastStream <Map<String, String>> confBroadcast = streamMySql.broadcast(ruleStateDescriptor);
         
         
         stream.connect(confBroadcast).process(
        		 new BroadcastProcessFunction <Tuple3<String, String, Long>, Map <String, String>, Tuple3<String, String, Long>>() {
                     /**
					 * 
					 */
					private static final long serialVersionUID = 346247603452183698L;
                    MapStateDescriptor <String,Map<String,String>> ruleStateDescriptor = new MapStateDescriptor <>("dimdevice",BasicTypeInfo.STRING_TYPE_INFO,new MapTypeInfo<>(String.class,String.class));

                     @Override
                     public void open(Configuration parameters) throws Exception {
                         super.open(parameters);
                     }
                     
                     /**
                      * 接收广播中的数据
                      * @param value
                      * @param ctx
                      * @param out
                      * @throws Exception
                      */
                     @Override
                     public void processBroadcastElement(Map <String, String> value, Context ctx, Collector <Tuple3<String, String, Long>> out) throws Exception {
                         log.info("zdm "+value.size());
                         ctx.getBroadcastState(ruleStateDescriptor).put("device", value);
                     }
                     
                     @Override
                     public void processElement(Tuple3 <String, String, Long> value, ReadOnlyContext ctx, Collector <Tuple3 <String, String, Long>> out) throws Exception {
 						Map<String, String> map= ctx.getBroadcastState(ruleStateDescriptor).get("device");
 						if(map != null) {
 							String result = map.get(value.f1);
 							if (result != null) {
 								out.collect(new Tuple3 <>(value.f0, result, value.f2));
 							}                    
 						}
 					}
                 }).addSink(new UserDeviceFeatureRedisSink());
         
         
    
         env.execute();
    }
}



