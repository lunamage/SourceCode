package recommend.mall;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
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
 * @date: 2019/6/25
 * @time: 3:14 PM
 * @Description: sdk文章数据聚合
 */
public class MallFeature {
	
	private static Logger log = LoggerFactory.getLogger(MallFeature.class);
	
	private final static String filter1 = "\"type\":\"tj\"";
    private final static String filter2 = "\"event\":\"show\"";
    private final static String filter3 = "\"event\":\"sec\"";
    private final static String filter4 = "\"首页_推荐_feed流";
    


    public static void main(String[] args) throws Exception {
    	
    	 StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
         env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
         env.enableCheckpointing(60000L);
         env.getCheckpointConfig().setMinPauseBetweenCheckpoints(10000L);
         env.getCheckpointConfig().setCheckpointTimeout(1200000L);
         
         env.setStateBackend((StateBackend) new FsStateBackend("hdfs://cluster/bi/flink_checkpoint/MallFeature",true));
         
         
         
         Properties properties = new Properties();
         properties.setProperty("bootstrap.servers",ReadConfig.getProperties("bootstrap.servers"));
         properties.setProperty("group.id", "MallFeature");
         FlinkKafkaConsumer<String> myConsumer = new FlinkKafkaConsumer<>(ReadConfig.getProperties("kafka.etl.topic"), new SimpleStringSchema(), properties);
         myConsumer.setStartFromGroupOffsets();
         
         //DataStream<String> myConsumer = env.readTextFile ("C:/Users/zhaoyulong/Downloads/simplify_repeat.txt");
 	
         
         KeyedStream<Tuple4<String, String, String, Long>, Tuple> keyedStream = env.addSource(myConsumer).filter((FilterFunction<String>) log -> {
             return log.contains(filter1) && log.contains(filter2) || log.contains(filter3) && log.contains(filter4);
         }).flatMap(new MallFeatureSplitter()).keyBy(0);
         
         
         keyedStream.timeWindow(Time.hours(1),Time.minutes(1)).aggregate(new Cal(), new WindowResultFunction()).addSink(new MallFeatureRedisSink1h());
         keyedStream.timeWindow(Time.hours(8),Time.minutes(1)).aggregate(new Cal(), new WindowResultFunction()).addSink(new MallFeatureRedisSink8h());
         env.execute();
    }
    
    
    public static class CalEntity {
        public int imp = 0;
        public int sec = 0;
    }

    public static class ItemFeatureEntity {
        private String mall;
        private Map<String, Object[]> val;
        private String windowEnd;

        public String getMall() {
            return mall;
        }

        public Map<String, Object[]> getVal() {
            return val;
        }

        public String getWindowEnd() {
            return windowEnd;
        }

        public static ItemFeatureEntity getEntity(String mall, Map<String, Object[]> val, String windowEnd) {
            ItemFeatureEntity entity = new ItemFeatureEntity();
            entity.mall = mall;
            entity.val = val;
            entity.windowEnd = windowEnd;
            return entity;
        }

		@Override
		public String toString() {
			return "ItemFeatureEntity [mall=" + mall + ", val=" + val.toString() + ", windowEnd=" + windowEnd + "]";
		}
        
    }

    public static class Cal implements AggregateFunction<Tuple4<String, String, String, Long>, HashMap<String, CalEntity>, HashMap<String, CalEntity>> {

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
        public HashMap<String, CalEntity> add(Tuple4<String, String, String, Long> val, HashMap<String, CalEntity> entity) {
            if (!entity.containsKey(val.f1)) {
                entity.put(val.f1, new CalEntity());
            }
            if (Objects.equals(val.f2, "show")) {
                entity.get(val.f1).imp += 1;
            }
            if (Objects.equals(val.f2, "sec")) {
                entity.get(val.f1).sec += 1;
            }
            return entity;
        }
    }

    public static class WindowResultFunction implements WindowFunction<HashMap<String, CalEntity>, ItemFeatureEntity, Tuple, TimeWindow> {
        @Override
        public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<HashMap<String, CalEntity>> entitys, Collector<ItemFeatureEntity> collector) {
            String mall = tuple.getField(0);
            Map<String, CalEntity> entity = entitys.iterator().next();
            Map<String, Object[]> val = new HashMap<>();

            for (Map.Entry<String, CalEntity> map : entity.entrySet()) {
                Integer imp = map.getValue().imp;
                Integer sec = map.getValue().sec;
                val.put(map.getKey(), new Object[]{imp, sec});
            }
            collector.collect(ItemFeatureEntity.getEntity(mall, val, String.valueOf(timeWindow.getEnd())));
        }
    }
}



