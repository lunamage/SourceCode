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
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
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
public class QueryRealtimeBak {

	private static Logger log = LoggerFactory.getLogger(QueryRealtimeBak.class);

	private final static String filter1 = "\"ec\":\"搜索\"";
    private final static String filter2 = "\"ea\":\"点击\"";
    private final static String filter3 = "\"ec\":\"04\"";
    private final static String filter4 = "\"ea\":\"03\"";


    public static void main(String[] args) throws Exception {

    	 StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
         env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
         //
         env.enableCheckpointing(120000L);
         env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000L);
         env.getCheckpointConfig().setCheckpointTimeout(1800000L);

         env.setStateBackend((StateBackend) new FsStateBackend("hdfs://cluster/bi/flink_checkpoint/searchquery",true));
         //env.setStateBackend((StateBackend) new RocksDBStateBackend("hdfs://cluster/bi/flink_checkpoint/searchquery",true));

         Properties properties = new Properties();
         properties.setProperty("bootstrap.servers",ReadConfig.getProperties("bootstrap.servers"));
         properties.setProperty("group.id", "queryRealtime");

         FlinkKafkaConsumer<String> myConsumer = new FlinkKafkaConsumer<>(ReadConfig.getProperties("kafka.topic"), new SimpleStringSchema(), properties);
         myConsumer.setStartFromGroupOffsets();
         //myConsumer.setStartFromLatest();
         DataStream<Tuple5<String, String, Double, String, Long>> stream = env.addSource(myConsumer).filter((FilterFunction<String>) log -> {
             return log.contains(filter1) && log.contains(filter2) || log.contains(filter3) && log.contains(filter4);
         }).flatMap(new QueryRealtimeSplitterBak());

         //1.
         DataStream<Map<String, String>> streamMySql = env.addSource(new JdbcReader());
 		 //2、创建MapStateDescriptor规则，对广播的数据的数据类型的规则
         MapStateDescriptor <String,Map<String,String>> ruleStateDescriptor = new MapStateDescriptor <>("dimquery",BasicTypeInfo.STRING_TYPE_INFO,new MapTypeInfo<>(String.class,String.class));
         //3、对conf进行broadcast返回BroadcastStream
         final BroadcastStream <Map<String, String>> confBroadcast = streamMySql.broadcast(ruleStateDescriptor);

         KeyedStream<Tuple5<String, String, Double, String, Long>, Tuple> keyedStream = stream.connect(confBroadcast).process(
        		 new BroadcastProcessFunction <Tuple5<String, String, Double, String, Long>, Map <String, String>, Tuple5<String, String, Double, String, Long>>() {
					//private Map<String,String> keyWords = new Hashtable<String, String>();
                     MapStateDescriptor <String,Map<String,String>> ruleStateDescriptor = new MapStateDescriptor <>("dimquery",BasicTypeInfo.STRING_TYPE_INFO,new MapTypeInfo<>(String.class,String.class));

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
                     public void processBroadcastElement(Map <String, String> value, Context ctx, Collector <Tuple5<String, String, Double, String, Long>> out) throws Exception {
                         log.info("zdm "+value.size());
                         ctx.getBroadcastState(ruleStateDescriptor).put("keyWords", value);
                     }

                     @Override
                     public void processElement(Tuple5<String, String, Double, String, Long> value, ReadOnlyContext ctx, Collector <Tuple5<String, String, Double, String, Long>> out) throws Exception {
                        //Thread.sleep(30);
 						Map<String, String> map= ctx.getBroadcastState(ruleStateDescriptor).get("keyWords");
 						if(map != null) {
 							String result = map.get(value.f0);
 							if (result != null) {
 								out.collect(new Tuple5 <>(result, value.f1, value.f2, value.f3, value.f4));
 							}
 						}
 					}
                 }).keyBy(0);

         //keyedStream.timeWindow(Time.hours(3), Time.minutes(2)).aggregate(new cal(), new WindowResultFunction()).addSink(new QueryRealtimeRedisSink3h()).slotSharingGroup("group1");
         //keyedStream.timeWindow(Time.hours(12), Time.minutes(2)).aggregate(new cal(), new WindowResultFunction()).addSink(new QueryRealtimeRedisSink12h()).slotSharingGroup("group2");

         env.execute();
    }


    public static class CalEntity {
        public int sl = 0;
        public Double correctSl = 0.0;
        public int imp = 0;
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

    public static class WindowResultFunction implements WindowFunction<HashMap<String, CalEntity>, ItemFeatureEntity, Tuple, TimeWindow> {
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
