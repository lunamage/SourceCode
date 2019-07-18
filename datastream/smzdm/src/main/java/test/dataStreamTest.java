package test;


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

import gmv.olap.OrdersJdmSplitter;
import sdk.hdfs.SdkGroupArticleIdSplitter;
import utils.ReadConfig;

import java.time.ZoneId;
import java.util.*;


public class dataStreamTest {
	
    public static void main(String[] args) throws Exception {
    	 //StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
         //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
         //env.enableCheckpointing(5000);
         
         //DataStream<String> stream_orders_jdm = env.readTextFile ("C:/Users/zhaoyulong/Downloads/stream_orders_jdm.txt");
         //DataStream<String> stream_orders_jd = env.readTextFile ("C:/Users/zhaoyulong/Downloads/stream_orders_jd.txt");
         //DataStream<String> stream_orders_tmm = env.readTextFile ("C:/Users/zhaoyulong/Downloads/stream_orders_tmm.txt");
         //DataStream<String> stream_orders_tm = env.readTextFile ("C:/Users/zhaoyulong/Downloads/stream_orders_tm.txt");
         //stream_orders_jdm.flatMap(new OrdersJdmSplitter()).print();
         //stream_orders_jd.print();
         //stream_orders_tmm.print();
         //stream_orders_tm.print();
         
         //env.execute("Run flink");
    	
    	
    	/*Timer timer = new Timer();  
        timer.schedule(new TimerTask() {  
            public void run() {  
                System.out.println("-------设定要指定任务--------");  
            }  
        }, 1000,3000); */
    	
    	String[] b= ReadConfig.getProperties("db.app").split("\\|");
    	System.out.println(b[0]+"\n"+b[1] );
     }
}



