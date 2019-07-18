package test;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple14;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.ReadConfig;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLConnection;
import sdk.druid.AsyncMysql;
import sdk.druid.SdkOlapEtlSplitter;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;

import org.apache.flink.configuration.Configuration;


/**
 * @author: zyl
 * @date: 2019/6/25
 * @time: 3:14 PM
 * @Description: sdk文章数据聚合
 */
public class SdkOlapEtlTest {
	
	private static Logger log = LoggerFactory.getLogger(SdkOlapEtlTest.class);
	
	private final static String Exposure_STATUS = "\"ec\":\"01\"";
    private final static String Click_STATUS = "\"ec\":\"首页\"";
    private final static String Event_STATUS = "\"ec\":\"增强型电子商务\"";

    public static void main(String[] args) throws Exception {
    	 StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
         env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
         DataStream<String> myConsumer = env.readTextFile ("C:/Users/zhaoyulong/Downloads/sdkjson.txt");

 	
         DataStream<Tuple14<String, String, String, String, String, String, String, String, String, String, String, Integer, Integer, Integer>> stream = myConsumer.filter((FilterFunction<String>) log -> {
             // 过滤出曝光以及点击事件
             return log.contains(Exposure_STATUS) || log.contains(Click_STATUS) || log.contains(Event_STATUS);
         }).flatMap(new SdkOlapEtlSplitter());
         
         DataStream<String> result;
         result = AsyncDataStream.unorderedWait(
        		 stream,
        		 new AsyncMysql(),
                 30000,
                 TimeUnit.MILLISECONDS,
                 50).setParallelism(1);
         result.print();

         env.execute();

    }
}
        



