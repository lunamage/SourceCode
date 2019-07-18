package sdk.druid;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple14;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utils.ReadConfig;

import java.util.*;
import java.util.concurrent.TimeUnit;


/**
 * @author: zyl
 * @date: 2019/6/25
 * @time: 3:14 PM
 * @Description: sdk文章数据聚合
 */
public class SdkOlapEtl {
	
	private static Logger log = LoggerFactory.getLogger(SdkOlapEtl.class);
	
	private final static String Exposure_STATUS = "\"ec\":\"01\"";
    private final static String Click_STATUS = "\"ec\":\"首页\"";
    private final static String Event_STATUS = "\"ec\":\"增强型电子商务\"";

    public static void main(String[] args) throws Exception {
    	 StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
         env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
         env.enableCheckpointing(5000);
         
         Properties properties = new Properties();
         properties.setProperty("bootstrap.servers",ReadConfig.getProperties("bootstrap.servers"));
         properties.setProperty("group.id", "SdkGroupArticleId");
 	
         FlinkKafkaConsumer<String> myConsumer = new FlinkKafkaConsumer<>(ReadConfig.getProperties("kafka.topic"), new SimpleStringSchema(), properties);
 	
         //myConsumer.setStartFromLatest();
 	
         DataStream<Tuple14<String, String, String, String, String, String, String, String, String, String, String, Integer, Integer, Integer>> stream = env.addSource(myConsumer).filter((FilterFunction<String>) log -> {
             // 过滤出曝光以及点击事件
             return log.contains(Exposure_STATUS) || log.contains(Click_STATUS) || log.contains(Event_STATUS);
         }).flatMap(new SdkOlapEtlSplitter());
         
         DataStream<String> result;
         result = AsyncDataStream.unorderedWait(stream,new AsyncMysql(),10000,TimeUnit.MILLISECONDS,50);
         
         
         FlinkKafkaProducer<String> myProducer = new FlinkKafkaProducer<String>(
        		 ReadConfig.getProperties("bootstrap.servers"),            // broker list
        	        "my-topic",                  // target topic
        	        new SimpleStringSchema());   // serialization schema

        	//myProducer.setWriteTimestampToKafka(true);

         result.addSink(myProducer);
         

         env.execute();

    }
 }
        



