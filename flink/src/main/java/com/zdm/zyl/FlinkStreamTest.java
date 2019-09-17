package com.zdm.zyl;


import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
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

import com.alibaba.fastjson.JSONObject;

import zyl.MessageSplitter;
import zyl.RedisSink;

import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.util.*;

/**
 * @author: liuchen
 * @date: 2019/1/11
 * @time: 3:14 PM
 * @Description: 实时特征计算
 */
public class FlinkStreamTest {
	
	private static Logger log = LoggerFactory.getLogger(FlinkStreamTest.class);
	
    private final static String filter1 = "\"type\":\"tj\"";
    private final static String filter2 = "\"type\":\"hj\"";
    private final static String filter3 = "\"type\":\"ss\"";

    public static void main(String[] args) throws Exception {
        // 任务名称
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(180000);
        env.setStateBackend((StateBackend) new FsStateBackend("hdfs://cluster/bi/flink_checkpoint",true));
        
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", ReadConfig.getProperties("bootstrap.servers"));
        properties.setProperty("group.id", "zyl-24hfeature");

        FlinkKafkaConsumer<String> myConsumer = new FlinkKafkaConsumer<>(ReadConfig.getProperties("kafka.etl.topic"), new SimpleStringSchema(), properties);
        
        myConsumer.setStartFromLatest();
        //SingleOutputStreamOperator<ItemFeatureEntity> 
        DataStream<ItemFeatureEntity> r = env.addSource(myConsumer)
           .filter((FilterFunction<String>) log -> {
        	   // 过滤出曝光以及点击事件
        	   return log.contains(filter1) || log.contains(filter2) || log.contains(filter3);})
           .flatMap(new MessageSplitter())
           .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple4<String, String, String, Long>>(Time.seconds(5)) {
        	   @Override
               public long extractTimestamp(Tuple4<String, String, String, Long> tuple4) {return tuple4.f3;}})
           .keyBy(0)
           .timeWindow(Time.minutes(3))
           .aggregate(new calImpAndClickAgg(), new WindowResultFunction());
        
        //r.addSink(new RedisSink());
        
        //写入hdfs
        DataStream<String> hdfsStream = r.map(record -> {return record.toString();});
        BucketingSink<String> sink = new BucketingSink<String>("/bi/app_ga/app_flink_offline/");
        sink.setBucketer(new DateTimeBucketer<String>("yyyyMMddHHmm", ZoneId.of("Asia/Shanghai")) );
        sink.setBatchSize(1024 * 1024 * 400); // this is 400 MB,
        sink.setBatchRolloverInterval(60* 60 * 1000); // this is 60 mins
        sink.setPendingPrefix("");
        sink.setPendingSuffix("");
        sink.setInProgressPrefix(".");
        hdfsStream.addSink(sink);
        
        
        //.addSink(new RedisSink(propFile));
        env.execute("Run_flink_1d_ctr");
    }
    

    public static class CalEntity {
        public int imp = 0;
        public int click = 0;
        public int s_click = 0;
    }

    public static class ItemFeatureEntity {
        private String userProxyId;
        private Map<String, Object[]> val;
        private String windowEnd;

        public String getUserProxyId() {
            return userProxyId;
        }

        public Map<String, Object[]> getVal() {
            return val;
        }

        public String getWindowEnd() {
            return windowEnd;
        }

        public static ItemFeatureEntity getEntity(String userProxyId, Map<String, Object[]> val, String windowEnd) {
            ItemFeatureEntity entity = new ItemFeatureEntity();
            entity.userProxyId = userProxyId;
            entity.val = val;
            entity.windowEnd = windowEnd;
            return entity;
        }

		@Override
		public String toString() {
			String data=userProxyId + "|";
			
			for (Map.Entry<String, Object[]> entry : val.entrySet()) {
				String propertyItemId = entry.getKey();
				String imp = String.valueOf(entry.getValue()[0]);
				String click = String.valueOf(entry.getValue()[1]);
				String s_click = String.valueOf(entry.getValue()[2]);
				data=data+"{\"key\":\""+propertyItemId+"\",\"imp\":\""+imp+"\",\"click\":\""+click+"\",\"s_click\":\""+s_click+"\"},";
			}
			
			return data.substring(0, data.length()-1) + "|" + windowEnd;
		}
        
        
    }

    public static class calImpAndClickAgg implements AggregateFunction<Tuple4<String, String, String, Long>, HashMap<String, CalEntity>, HashMap<String, CalEntity>> {

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
            if (Objects.equals(val.f2, Utils.IMP)) {
                entity.get(val.f1).imp += 1;
            }
            if (Objects.equals(val.f2, Utils.CLICK)) {
                entity.get(val.f1).click += 1;
            }
            if (Objects.equals(val.f2, "s_click")) {
                entity.get(val.f1).s_click += 1;
            }
            return entity;
        }
    }

    public static class WindowResultFunction implements WindowFunction<HashMap<String, CalEntity>, ItemFeatureEntity, Tuple, TimeWindow> {
        @Override
        public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<HashMap<String, CalEntity>> entitys, Collector<ItemFeatureEntity> collector) {
            String userProxyId = tuple.getField(0);
            Map<String, CalEntity> entity = entitys.iterator().next();
            Map<String, Object[]> val = new HashMap<>();

            for (Map.Entry<String, CalEntity> map : entity.entrySet()) {
                Integer imp = map.getValue().imp;
                Integer click = map.getValue().click;
                Integer s_click = map.getValue().s_click;
                //DecimalFormat df = new DecimalFormat("0.00000");
                //Double ctr = imp == 0 ? null : Double.valueOf(df.format((float) click / imp));
                val.put(map.getKey(), new Object[]{imp, click, s_click});
            }
            collector.collect(ItemFeatureEntity.getEntity(userProxyId, val, DateUtils.formatDate(new Date(timeWindow.getEnd()), DateUtils.YYYYMMDD_HMS)));
        }
    }
}



