package recmmend.artRead;

import java.util.Properties;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utils.ReadConfig;




/**
 * @author: zyl
 * @date: 2019/6/25
 * @time: 3:14 PM
 * @Description: sdk文章数据聚合
 */
public class ArtReadFeature {
	
	private static Logger log = LoggerFactory.getLogger(ArtReadFeature.class);
	
	private final static String filter1 = "\"ec\":\"详情页\"";
    private final static String filter2 = "\"ea\":\"详情页阅读\"";

    public static void main(String[] args) throws Exception {
    	
    	 StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
         env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
         //
         env.enableCheckpointing(300000L);
         env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000L);
         env.getCheckpointConfig().setCheckpointTimeout(1800000L);
         env.setStateBackend((StateBackend) new FsStateBackend("hdfs://HDFS80727/bi/flink/checkpoint",true));
         ExecutionConfig executionConfig = new ExecutionConfig();
 		 executionConfig.setUseSnapshotCompression(true);
 		 
 		 StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
 
         Properties properties = new Properties();
         properties.setProperty("bootstrap.servers",ReadConfig.getProperties("bootstrap.servers"));
         properties.setProperty("group.id", "ArtReadFeature");
 	
         FlinkKafkaConsumer010<String> myConsumer = new FlinkKafkaConsumer010<>(ReadConfig.getProperties("kafka.topic"), new SimpleStringSchema(), properties);
         
         myConsumer.setStartFromGroupOffsets();
         DataStream<Tuple4<String, Integer, Double, Long>> stream = env.addSource(myConsumer).filter((FilterFunction<String>) log -> {
             return log.contains(filter1) && log.contains(filter2);
         }).flatMap(new ArtReadFeatureSplitter());
         
         tableEnv.registerDataStream("t", stream, "id,dt,finish,etime,ptime.proctime");
         
         String sql = "select id,sum(dt) dt,sum(finish) finish,count(*) sl from t group by id,TUMBLE(ptime,INTERVAL '15' SECOND)";
         
         Table result = tableEnv.sqlQuery(sql);
         
         DataStream<Tuple2<Boolean, Row>> rw = tableEnv.toRetractStream(result, Row.class);
         
         rw.addSink(new ArtReadFeatureRedisSink());
         
         env.execute("ArtReadFeature");
    }
    
 
}



