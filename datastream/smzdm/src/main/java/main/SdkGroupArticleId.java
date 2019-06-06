package main;


import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.fs.bucketing.DateTimeBucketer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import service.SdkGroupArticleIdSplitter;
import utils.ReadConfig;

import java.time.ZoneId;
import java.util.*;

/**
 * @author: zyl
 * @date: 2019/6/3
 * @time: 3:14 PM
 * @Description: sdk文章数据聚合
 */
public class SdkGroupArticleId {
	
	private static Logger log = LoggerFactory.getLogger(SdkGroupArticleId.class);
	
	private final static String Exposure_STATUS = "\"ec\":\"01\"";
    private final static String Click_STATUS = "\"ec\":\"首页\"";
    private final static String Event_STATUS = "\"ec\":\"增强型电子商务\"";

    public static void main(String[] args) throws Exception {
    	 StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
         env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
         env.enableCheckpointing(5000);
         
         Properties properties = new Properties();
         properties.setProperty("bootstrap.servers",ReadConfig.getProperties("bootstrap.servers"));
         properties.setProperty("group.id", "SdkGroupArticleId");
 	
         FlinkKafkaConsumer<String> myConsumer = new FlinkKafkaConsumer<>(ReadConfig.getProperties("kafka.topic"), new SimpleStringSchema(), properties);
 	
         myConsumer.setStartFromLatest();
 	
         DataStream<Tuple6<String, String, String, String, String, Long>> stream = env.addSource(myConsumer).filter((FilterFunction<String>) log -> {
             // 过滤出曝光以及点击事件
             return log.contains(Exposure_STATUS) || log.contains(Click_STATUS) || log.contains(Event_STATUS);
         }).flatMap(new SdkGroupArticleIdSplitter());
         
         
         BucketingSink<String> sink = new BucketingSink<String>("/bi/app_ga/app_sdk_group_article_tmp/");
         sink.setBucketer(new DateTimeBucketer<String>("yyyyMMddHHmm", ZoneId.of("Asia/Shanghai")) );
         sink.setBatchSize(1024 * 1024 * 400); // this is 400 MB,
         sink.setBatchRolloverInterval(60* 60 * 1000); // this is 60 mins
         sink.setPendingPrefix("");
         sink.setPendingSuffix("");
         sink.setInProgressPrefix(".");
         
 	
         stream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple6<String, String, String, String, String, Long>>(Time.seconds(1)) {
         /**
 		 * 
 		 */
 		private static final long serialVersionUID = 1L;

 		@Override
         public long extractTimestamp(Tuple6<String, String, String, String, String, Long> tuple3) {
             	return tuple3.f5;
         	}
         }).keyBy(0,1,2,3,4).timeWindow(Time.minutes(5)).aggregate(new Cal(), new WindowResultFunction()).map(record -> {return record.toString();}).addSink(sink);
 	
         env.execute("Run flink");
     }
 	
 	public static class CalEntity {
         public int sl = 0;
     }
 	
 	public static class ItemFeatureEntity {
		private String stat_dt;
		private String type;
		private String abtest;
		private String articleId;
		private String channel;
		private int sl;
		private long windowEnd;

 		public static ItemFeatureEntity getEntity(String stat_dt, String type, String abtest,String articleId, String channel, int sl, long windowEnd) {
             ItemFeatureEntity entity = new ItemFeatureEntity();
             entity.stat_dt = stat_dt;
             entity.type = type;
             entity.abtest = abtest;
             entity.articleId=articleId;
             entity.channel=channel;
             entity.sl = sl;
             entity.windowEnd = windowEnd;
             return entity;
         }
		
		

 		public String getStat_dt() {
 			return stat_dt;
 		}

 		public void setStat_dt(String stat_dt) {
 			this.stat_dt = stat_dt;
 		}

 		public String getType() {
 			return type;
 		}

 		public void setType(String type) {
 			this.type = type;
 		}

 		public String getAbtest() {
 			if (abtest == null) {
 				abtest="smzdm";
 			}
 			return abtest;
 		}

 		public void setAbtest(String abtest) {
 			this.abtest = abtest;
 		}

 		public String getArticleId() {
			return articleId;
		}

		public void setArticleId(String articleId) {
			this.articleId = articleId;
		}

		public String getChannel() {
			return channel;
		}

		public void setChannel(String channel) {
			this.channel = channel;
		}

		public int getSl() {
 			return sl;
 		}

 		public void setSl(int sl) {
 			this.sl = sl;
 		}

 		public long getWindowEnd() {
 			return windowEnd;
 		}

 		public void setWindowEnd(long windowEnd) {
 			this.windowEnd = windowEnd;
 		}

		@Override
		public String toString() {
			return stat_dt + "\001" + 
				   type + "\001" +
		           abtest + "\001" + 
                   articleId + "\001" + 
                   channel + "\001" + 
                   sl + "\001" + 
                   windowEnd;
		}
     }
 	
 	 public static class Cal implements AggregateFunction<Tuple6<String, String, String, String, String, Long>, CalEntity, CalEntity> {

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
 	        public CalEntity add(Tuple6<String, String, String, String, String, Long> val, CalEntity entity) {
 	            entity.sl += 1;
 	            return entity;
 	        }
 	    }

 	    public static class WindowResultFunction implements WindowFunction<CalEntity, ItemFeatureEntity, Tuple, TimeWindow> {
 			private static final long serialVersionUID = 1L;

 			@Override
 	        public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<CalEntity> entitys, Collector<ItemFeatureEntity> collector) throws Exception {
 				String stat_dt = tuple.getField(0);
 				String type = tuple.getField(1);
 				String abtest = tuple.getField(2);
 				String articleId = tuple.getField(3);
 				String channel = tuple.getField(4);
 				int imp = entitys.iterator().next().sl;
 	            collector.collect(ItemFeatureEntity.getEntity(stat_dt, type, abtest, articleId, channel, imp, timeWindow.getEnd()));
 	        }
 	    }
 	
 	
 }
        



