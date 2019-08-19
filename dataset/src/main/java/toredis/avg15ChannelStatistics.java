package toredis;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.container.RedisCommandsContainer;
import org.apache.flink.streaming.connectors.redis.common.container.RedisCommandsContainerBuilder;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;
import utils.ReadConfig;

import java.io.IOException;
import java.util.*;

/**
 * @author: liuchen
 * @date: 2019/1/11
 * @time: 3:14 PM
 * @Description: 实时特征计算
 */
public class avg15ChannelStatistics {
	
	private static Logger log = LoggerFactory.getLogger(avg15ChannelStatistics.class);

    public static void main(String[] args) throws Exception {
        // 任务名称
    	ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    	//线上 hdfs://cluster
    	//测试 hdfs://hadoop-test
    	
        DataSet<String> hdfsLines = env.readTextFile("hdfs://cluster/bi/app_ga/app_recommend_avg15ChannelStatistics");
        
        DataSet<Tuple2<String, String>> data = hdfsLines.flatMap(new Tokenizer());
        
        //data.print();
		
		data.output(new OutputFormat<Tuple2<String, String>>() {

			 private JedisPool jedisPool;
            //private Jedis jedis;

            @Override
            public void configure(Configuration configuration) {
            	// TODO Auto-generated method stub
            //    pool = new JedisPool(ReadConfig.getProperties("redis.article.feature"), Integer.valueOf(ReadConfig.getProperties("redis.port")));
            }

            @Override
            public void open(int i, int i1) throws IOException {
            	   // 创建jedis池配置实例
    	        JedisPoolConfig config = new JedisPoolConfig();
    	        // #jedis的最大分配对象#
    	        config.setMaxTotal(Integer.valueOf(ReadConfig.getProperties("jedis.pool.maxActive")));
    	        // #jedis最大保存idel状态对象数 #
    	        config.setMaxIdle(Integer.valueOf(ReadConfig.getProperties("jedis.pool.maxIdle")));

    	        this.jedisPool = new JedisPool(config, ReadConfig.getProperties("redis.address"),
    	                Integer.valueOf(ReadConfig.getProperties("redis.port")),
    	                Integer.valueOf(ReadConfig.getProperties("jedis.pool.timeout")));
            }
            
            
            @Override
            public void writeRecord(Tuple2<String, String> tuple) throws IOException {       	
            	 try (Jedis jedis = jedisPool.getResource(); Pipeline line = jedis.pipelined()) {
     	            line.set(tuple.f0,tuple.f1);
     	            line.sync();
            		//System.out.print(tuple.f0+" "+tuple.f1); 
            		 
     	        } catch (Exception e) {
     	            log.error("set redis error is {} ", e.getMessage());
     	        }
            	
            	
            }

            @Override
            public void close() throws IOException {
                //jedis.close();
            }
        });
		
        env.execute("avg15ChannelStatistics");
    }
    
    public static class Tokenizer implements FlatMapFunction<String,Tuple2<String, String>>{
        public void flatMap(String msg, Collector<Tuple2<String, String>> collector) {
        	try {
            	//String[] data = msg.split("\\|");
            	String[] data = msg.split("\\001");
            	//String userProxyId = data[0];
            	//for (String items : data[1].split(",")) {
            		//Map<String,String> item = JSONObject.parseObject(items, Map.class);
            		//String[] key = item.get("key").split("_");
            		//String imp = item.get("imp");
            		//String click = item.get("click");
            	collector.collect(new Tuple2<>(data[0], data[1]));
                

            } catch (Exception e) {
                log.error("flatMap error msg is {} value is {}", e.getMessage(), msg);
            }
        	
        }
    }
    
   

   
}



