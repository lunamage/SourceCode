package search.query;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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

import search.query.QueryRealtimeTest.Cal;
import search.query.QueryRealtimeTest.WindowResultFunction;
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
public class QueryRealtimebak {
	
	private static Logger log = LoggerFactory.getLogger(QueryRealtimebak.class);
	
	private final static String filter1 = "\"ec\":\"搜索\"";
    private final static String filter2 = "\"ea\":\"点击\"";
    

    public static void main(String[] args) throws Exception {
    	
    	 StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
         env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
         //20S
         env.enableCheckpointing(60000);
         env.setStateBackend((StateBackend) new FsStateBackend("hdfs://cluster/bi/flink_checkpoint/queryRealtime",true));
         
         //env.registerCachedFile("","");
         
         Properties properties = new Properties();
         properties.setProperty("bootstrap.servers",ReadConfig.getProperties("bootstrap.servers"));
         properties.setProperty("group.id", "queryRealtime");
 	
         FlinkKafkaConsumer<String> myConsumer = new FlinkKafkaConsumer<>(ReadConfig.getProperties("kafka.topic"), new SimpleStringSchema(), properties);
         
         //setStartFromGroupOffsets
         myConsumer.setStartFromGroupOffsets();
 	
         /*DataStream<Tuple4<String, String, String, Long>> stream = env.addSource(myConsumer).filter((FilterFunction<String>) log -> {
             return log.contains(filter1) && log.contains(filter2);
         }).flatMap(new QueryRealtimeSplitter());
  
         String[] configure= ReadConfig.getProperties("db.sf").split("\\|");
        
         TypeInformation[] fieldTypes =new TypeInformation[]{BasicTypeInfo.INT_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO};
 		 RowTypeInfo rowTypeInfo =new RowTypeInfo(fieldTypes);
 		 JDBCInputFormat jdbcInputFormat = JDBCInputFormat.buildJDBCInputFormat()
 			.setDrivername(configure[1])
 			.setDBUrl(configure[0])
 			.setUsername(configure[2])
 			.setPassword(configure[3])
 			.setQuery("SELECT id,query from dim_query")
 			.setRowTypeInfo(rowTypeInfo)
 			.finish();

 		 //DataStreamSource<Row> datasource = 
 		DataStream<Tuple2<String, String>> querystream	= env.createInput(jdbcInputFormat).map(new MapFunction <Row, Tuple2<String, String>>() {
             @Override
             public Tuple2<String, String> map(Row value) throws Exception {
                 return new Tuple2<>(String.valueOf(value.getField(0)),String.valueOf(value.getField(1)));
             }
         });
         
         
         DataStream<Tuple2<String, String>> streamMySql = env.addSource(new JdbcReaderTuple());
 		
         KeyedStream<Tuple4<String, String, String, Long>, Tuple> keyedStream = stream.join(streamMySql).where(new leftkey()).equalTo(new rigthkey()).window(TumblingProcessingTimeWindows.of(Time.seconds(30)))
 		 .apply (new JoinFunction<Tuple4<String, String, String, Long>, Tuple2<String, String>, Tuple4<String, String, String, Long>>(){
             @Override
             public Tuple4<String, String, String, Long> join(Tuple4<String, String, String, Long> first, Tuple2<String, String> second) {
                 return new Tuple4<>(second.f1,first.f1,first.f2,first.f3);
             }
         }).keyBy(0,1,2);*/
 		 
 		//keyedStream.timeWindow(Time.hours(3), Time.minutes(2)).aggregate(new Cal(), new WindowResultFunction()).addSink(new QueryRealtimeRedisSink3h());
        //keyedStream.timeWindow(Time.hours(12), Time.minutes(2)).aggregate(new Cal(), new WindowResultFunction()).addSink(new QueryRealtimeRedisSink12h());

        env.execute();

    }
    
    
    private static class leftkey implements KeySelector<Tuple4<String, String, String, Long>, String> {
        @Override
        public String getKey(Tuple4<String, String, String, Long> value) {
            return value.f0;
        }
    }
    
    private static class rigthkey implements KeySelector<Tuple2<String, String>, String> {
        @Override
        public String getKey(Tuple2<String, String> value) {
            return value.f0;
        }
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
        



