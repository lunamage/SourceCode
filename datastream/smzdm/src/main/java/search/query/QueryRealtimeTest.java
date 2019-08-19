package search.query;

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
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.fs.bucketing.DateTimeBucketer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class QueryRealtimeTest {
	
	private static Logger log = LoggerFactory.getLogger(QueryRealtimeTest.class);
	
	private final static String filter1 = "\"ec\":\"搜索\"";
    private final static String filter2 = "\"ea\":\"点击\"";
    

    public static void main(String[] args) throws Exception {
    	
    	 StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
         env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
         //
         env.enableCheckpointing(120000L);
         env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000L);
         env.getCheckpointConfig().setCheckpointTimeout(900000L);
         
         //env.setStateBackend((StateBackend) new FsStateBackend("hdfs://cluster/bi/flink_checkpoint/searchquery3h",true));
         env.setStateBackend((StateBackend) new RocksDBStateBackend("hdfs://cluster/bi/flink_checkpoint/searchquery3h",true));
         
         Properties properties = new Properties();
         properties.setProperty("bootstrap.servers",ReadConfig.getProperties("bootstrap.servers"));
         properties.setProperty("group.id", "queryRealtime");
 	
         FlinkKafkaConsumer<String> myConsumer = new FlinkKafkaConsumer<>(ReadConfig.getProperties("kafka.topic"), new SimpleStringSchema(), properties);
         myConsumer.setStartFromGroupOffsets();
 	
      /*   DataStream<Tuple4<String, String, String, Long>> stream = env.addSource(myConsumer).filter((FilterFunction<String>) log -> {
             return log.contains(filter1) && log.contains(filter2);
         }).flatMap(new QueryRealtimeSplitter());
         //.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple4<String, String, String, Long>>(Time.seconds(5)) {
         //@Override
         //public long extractTimestamp(Tuple4<String, String, String, Long> tuple4) {return tuple4.f3;}});
         
         //1.
         DataStream<Map<String, String>> streamMySql = env.addSource(new JdbcReader());
 		 //2、创建MapStateDescriptor规则，对广播的数据的数据类型的规则
         MapStateDescriptor <String,Map<String,String>> ruleStateDescriptor = new MapStateDescriptor <>("dimquery",BasicTypeInfo.STRING_TYPE_INFO,new MapTypeInfo<>(String.class,String.class));
         //3、对conf进行broadcast返回BroadcastStream
         final BroadcastStream <Map<String, String>> confBroadcast = streamMySql.broadcast(ruleStateDescriptor);
         
         KeyedStream<Tuple4<String, String, String, Long>, Tuple> keyedStream = stream.connect(confBroadcast).process(
        		 new BroadcastProcessFunction <Tuple4 <String, String, String, Long>, Map <String, String>, Tuple4 <String, String, String, Long>>() {
					private static final long serialVersionUID = 346247603452183698L;
					private Map<String,String> keyWords = new Hashtable<String, String>();
                     MapStateDescriptor <String,Map<String,String>> ruleStateDescriptor = new MapStateDescriptor <>("dimquery",BasicTypeInfo.STRING_TYPE_INFO,new MapTypeInfo<>(String.class,String.class));

                     @Override
                     public void open(Configuration parameters) throws Exception {
                         super.open(parameters);
                     }
                     
                     @Override
                     public void processBroadcastElement(Map <String, String> value, Context ctx, Collector <Tuple4<String, String, String, Long>> out) throws Exception {
                         log.info("zdm "+value.size());
                         ctx.getBroadcastState(ruleStateDescriptor).put("keyWords", value);
                     }
                     
                     @Override
                     public void processElement(Tuple4 <String, String, String, Long> value, ReadOnlyContext ctx, Collector <Tuple4 <String, String, String, Long>> out) throws Exception {
                        //Thread.sleep(30);
 						Map<String, String> map= ctx.getBroadcastState(ruleStateDescriptor).get("keyWords");
 						if(map != null) {
 							String result = map.get(value.f0);
 							if (result != null) {
 								out.collect(new Tuple4 <>(result, value.f1, value.f2, value.f3));
 							}                    
 						}
 					}
                 }).keyBy(0,1,2);

 		
        
         keyedStream.timeWindow(Time.hours(3), Time.minutes(5)).aggregate(new Cal(), new WindowResultFunction()).addSink(new QueryRealtimeRedisSink3h());
         keyedStream.timeWindow(Time.hours(12), Time.minutes(5)).aggregate(new Cal(), new WindowResultFunction()).addSink(new QueryRealtimeRedisSink12h());*/
         env.execute();
    }
    
    
	public static class CalEntity {
        public int sl = 0;
    }
	
	public static class ItemFeatureEntity {
		private String queryId;
        private String channelId;
        private String articleId;
        private int sl;
        private long windowEnd;
        
		public String getQueryId() {
			return queryId;
		}
		public String getChannelId() {
			return channelId;
		}
		public String getArticleId() {
			return articleId;
		}
		public int getSl() {
			return sl;
		}
		public long getWindowEnd() {
			return windowEnd;
		}
		public static ItemFeatureEntity getEntity(String queryId, String channelId, String articleId, int sl, long windowEnd) {
			ItemFeatureEntity entity = new ItemFeatureEntity();
            entity.queryId = queryId;
            entity.channelId = channelId;
            entity.articleId = articleId;
            entity.sl = sl;
            entity.windowEnd = windowEnd;
            return entity;
		}
		@Override
		public String toString() {
			return "ItemFeatureEntity [queryId=" + queryId + ", channelId=" + channelId + ", articleId=" + articleId
					+ ", sl=" + sl + ", windowEnd=" + windowEnd + "]";
		}
		
		
    }
	
	 public static class Cal implements AggregateFunction<Tuple4<String, String, String, Long>, CalEntity, CalEntity> {

		private static final long serialVersionUID = 1L;

			@Override
	        public CalEntity createAccumulator() {
	            return new CalEntity();
	        }

	        @Override
	        public CalEntity getResult(CalEntity o) {
	            return o;
	        }

	        @Override
	        public CalEntity merge(CalEntity entity1, CalEntity entity2) {
	            return null;
	        }

	        @Override
	        public CalEntity add(Tuple4<String, String, String, Long> val, CalEntity entity) {
	            entity.sl += 1;
	            return entity;
	        }
	    }

	    public static class WindowResultFunction implements WindowFunction<CalEntity, ItemFeatureEntity, Tuple, TimeWindow> {
			private static final long serialVersionUID = 1L;

			@Override
	        public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<CalEntity> entitys, Collector<ItemFeatureEntity> collector) throws Exception {
				String queryId = tuple.getField(0);
				String channelId = tuple.getField(1);
				String articleId = tuple.getField(2);
				int sl = entitys.iterator().next().sl;
	            collector.collect(ItemFeatureEntity.getEntity(queryId, channelId, articleId, sl, timeWindow.getEnd()));
	        }
	    }
 }
        



