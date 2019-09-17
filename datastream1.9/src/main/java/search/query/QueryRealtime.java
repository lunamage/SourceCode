package search.query;

import org.apache.flink.api.common.ExecutionConfig;
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
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
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
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import search.query.QueryRealtimeToKafka.CalEntity;
import search.query.QueryRealtimeToKafka.ItemFeatureEntity;
import search.query.QueryRealtimeToKafka.WindowResultFunction;
import search.query.QueryRealtimeToKafka.WindowResultFunction2;
import search.query.QueryRealtimeToKafka.cal;
import search.query.QueryRealtimeToKafka.cal2;
import utils.ReadConfig;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
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
public class QueryRealtime {
	
	private static Logger log = LoggerFactory.getLogger(QueryRealtime.class);
	
	private final static String filter1 = "\"ec\":\"搜索\"";
    private final static String filter2 = "\"ea\":\"点击\"";
    private final static String filter3 = "\"ec\":\"04\"";
    private final static String filter4 = "\"ea\":\"03\"";
    

    public static void main(String[] args) throws Exception {
    	
   	 StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        //
        env.enableCheckpointing(240000L);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000L);
        env.getCheckpointConfig().setCheckpointTimeout(1800000L);
        
        env.setStateBackend((StateBackend) new FsStateBackend("hdfs://HDFS80727/bi/flink/checkpoint",true));
        ExecutionConfig executionConfig = new ExecutionConfig();
		executionConfig.setUseSnapshotCompression(true);
        
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers",ReadConfig.getProperties("bootstrap.servers"));
        properties.setProperty("group.id", "QueryRealtimeToKafka");

        FlinkKafkaConsumer010<String> myConsumer = new FlinkKafkaConsumer010<>(ReadConfig.getProperties("kafka.topic"), new SimpleStringSchema(), properties);
        
        myConsumer.setStartFromGroupOffsets();

        DataStream<Tuple6<String, String, Integer, Double, Integer, Long>> result = env.addSource(myConsumer).filter((FilterFunction<String>) log -> {
            return log.contains(filter1) && log.contains(filter2) || log.contains(filter3) && log.contains(filter4);
        }).flatMap(new QueryRealtimeSplitter()).keyBy(0).timeWindow(Time.minutes(4)).aggregate(new cal(), new WindowResultFunction());
        
        result.keyBy(0).timeWindow(Time.hours(3), Time.minutes(8)).aggregate(new cal2(), new WindowResultFunction2()).addSink(new QueryRealtimeRedisSink3h());
        result.keyBy(0).timeWindow(Time.hours(12), Time.minutes(8)).aggregate(new cal2(), new WindowResultFunction2()).addSink(new QueryRealtimeRedisSink12h());
        
        env.execute("QueryRealtime");
   }
   
   
   public static class CalEntity {
       public int sl = 0;
       public Double correctSl = 0.0;
       public int imp = 0;
   }

  public static class cal implements AggregateFunction<Tuple5<String, String, Double, String, Long>, HashMap<String, CalEntity>, HashMap<String, CalEntity>> {

       @Override
       public HashMap<String, CalEntity> createAccumulator() {
           return new HashMap<>();
       }

       @Override
       public HashMap<String, CalEntity> getResult(HashMap<String, CalEntity> o) {
           return o;
       }

       @Override
       public HashMap<String, CalEntity> merge(HashMap<String, CalEntity> entity1, HashMap<String, CalEntity> entity2) {
           return null;
       }

       @Override
       public HashMap<String, CalEntity> add(Tuple5<String, String, Double, String, Long> val, HashMap<String, CalEntity> entity) {
           if (!entity.containsKey(val.f1)) {
               entity.put(val.f1, new CalEntity());
           }
           if (Objects.equals(val.f3, "click")) {
           	entity.get(val.f1).sl += 1;
               entity.get(val.f1).correctSl += val.f2;
           }
           if (Objects.equals(val.f3, "imp")) {
           	entity.get(val.f1).imp += 1;
           }
           return entity;
       }
   }

   public static class WindowResultFunction implements WindowFunction<HashMap<String, CalEntity>, Tuple6<String, String, Integer, Double, Integer, Long>, Tuple, TimeWindow> {
       @Override
       public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<HashMap<String, CalEntity>> entitys, Collector<Tuple6<String, String, Integer, Double, Integer, Long>> collector) {
           String queryId = tuple.getField(0);
           Map<String, CalEntity> entity = entitys.iterator().next();
           Map<String, Object[]> val = new HashMap<>();

           for (Map.Entry<String, CalEntity> map : entity.entrySet()) {
           	String key = map.getKey();
           	Integer sl = map.getValue().sl;
               Double correctSl = map.getValue().correctSl;
               Integer imp = map.getValue().imp;
               collector.collect(new Tuple6<String, String, Integer, Double, Integer, Long>(queryId, key, sl, correctSl, imp, timeWindow.getEnd()));
           }
       }
   }
   
   
   
   public static class cal2 implements AggregateFunction<Tuple6<String, String, Integer, Double, Integer, Long>, HashMap<String, CalEntity>, HashMap<String, CalEntity>> {

       @Override
       public HashMap<String, CalEntity> createAccumulator() {
           return new HashMap<>();
       }

       @Override
       public HashMap<String, CalEntity> getResult(HashMap<String, CalEntity> o) {
           return o;
       }

       @Override
       public HashMap<String, CalEntity> merge(HashMap<String, CalEntity> entity1, HashMap<String, CalEntity> entity2) {
           return null;
       }

       @Override
       public HashMap<String, CalEntity> add(Tuple6<String, String, Integer, Double, Integer, Long> val, HashMap<String, CalEntity> entity) {
           if (!entity.containsKey(val.f1)) {
               entity.put(val.f1, new CalEntity());
           }
       	entity.get(val.f1).sl += val.f2;
           entity.get(val.f1).correctSl += val.f3;
       	entity.get(val.f1).imp +=val.f4;
           return entity;
       }
   }
   
   
   
   public static class ItemFeatureEntity {
       private String queryId;
       private Map<String, Object[]> val;
       private String windowEnd;

       public String getQueryId() {
           return queryId;
       }

       public Map<String, Object[]> getVal() {
           return val;
       }

       public String getWindowEnd() {
           return windowEnd;
       }

       public static ItemFeatureEntity getEntity(String queryId, Map<String, Object[]> val, String windowEnd) {
       	ItemFeatureEntity entity = new ItemFeatureEntity();
           entity.queryId = queryId;
           entity.val = val;
           entity.windowEnd = windowEnd;
           return entity;
       }

		@Override
		public String toString() {
			return "ItemFeatureEntity [queryId=" + queryId + ", val=" + val + ", windowEnd=" + windowEnd + "]";
		}
       
   }
   
   public static class WindowResultFunction2 implements WindowFunction<HashMap<String, CalEntity>, ItemFeatureEntity, Tuple, TimeWindow> {
       @Override
       public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<HashMap<String, CalEntity>> entitys, Collector<ItemFeatureEntity> collector) {
           String queryId = tuple.getField(0);
           Map<String, CalEntity> entity = entitys.iterator().next();
           Map<String, Object[]> val = new HashMap<>();

           for (Map.Entry<String, CalEntity> map : entity.entrySet()) {
               String sl = String.valueOf(map.getValue().sl);
               DecimalFormat df = new DecimalFormat("#.00");
               String correctSl = String.valueOf(df.format(map.getValue().correctSl));
               
               DecimalFormat df2 = new DecimalFormat("0.0000");
               
               int imp = map.getValue().imp;
               Double ctr = imp == 0 ? 0.0 : Double.valueOf(df2.format(Double.valueOf(sl) / imp));
               if(!sl.equals("0")) {
               	val.put(map.getKey(), new Object[]{sl,correctSl,ctr});
               }
           }
           if(val!=null) {
           	collector.collect(ItemFeatureEntity.getEntity(queryId, val, String.valueOf(timeWindow.getEnd())));
           }
       }
   }
}

