package search.query;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.io.ratelimiting.FlinkConnectorRateLimiter;
import org.apache.flink.api.common.io.ratelimiting.GuavaFlinkConnectorRateLimiter;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.ReadConfig;

import java.util.*;




/**
 * @author: zyl
 * @date: 2019/6/25
 * @time: 3:14 PM
 * @Description: sdk文章数据聚合
 */
public class QueryRealtimeToRedis {
	
	private static Logger log = LoggerFactory.getLogger(QueryRealtimeToRedis.class);
	
	public static void main(String[] args) throws Exception {
    	
    	 StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
         env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
         //
         env.enableCheckpointing(10000L);
         env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000L);
         env.getCheckpointConfig().setCheckpointTimeout(1800000L);
         
         env.setStateBackend((StateBackend) new FsStateBackend("hdfs://cluster/bi/flink_checkpoint/searchquery",true));
         
         Properties properties = new Properties();
         properties.setProperty("bootstrap.servers",ReadConfig.getProperties("bootstrap2.servers"));
         properties.setProperty("group.id", "QueryRealtimeToRedis");
 	
         FlinkKafkaConsumer010<String> myConsumer = new FlinkKafkaConsumer010<>(ReadConfig.getProperties("kafka.search.topic"), new SimpleStringSchema(), properties);
         
         myConsumer.setStartFromLatest();
         
         final long globalRate = 20000; // bytes/second
         FlinkConnectorRateLimiter rateLimiter = new GuavaFlinkConnectorRateLimiter();
         rateLimiter.setRate(globalRate);
         myConsumer.setRateLimiter(rateLimiter);
         
         env.addSource(myConsumer).flatMap(new QueryRealtimeToRedisFM()).addSink(new QueryRealtimeRedisSink());
         

         
         env.execute();
    }
    

}



