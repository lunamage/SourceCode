package com.zdm.zyl;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
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

import java.io.IOException;
import java.util.*;

/**
 * @author: liuchen
 * @date: 2019/1/11
 * @time: 3:14 PM
 * @Description: 实时特征计算
 */
public class FlinkSet {
	
	private static Logger log = LoggerFactory.getLogger(FlinkSet.class);
	
    private final static String IMP_LOG_STATUS = "\"type\":\"show\"";
    private final static String CLICK_LOG_STATUS = "\"type\":\"event\"";

    public static void main(String[] args) throws Exception {
        // 任务名称
    	ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
       
        DataSet<String> hdfsLines = env.readTextFile("hdfs://hadoop-test/bi/app_ga/app_user_portrait_redis");
        
        DataSet<Tuple3<String, String, String>> data = hdfsLines.flatMap(new Tokenizer());
        
        //data.print();
		
		data.output(new OutputFormat<Tuple3<String, String, String>>() {

            private JedisPool pool;
            private Jedis jedis;

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

                this.pool = new JedisPool(config, ReadConfig.getProperties("redis.article.feature"),
                        Integer.valueOf(ReadConfig.getProperties("redis.port")),
                        Integer.valueOf(ReadConfig.getProperties("jedis.pool.timeout")));
                jedis = pool.getResource();
            }
            
            
            @Override
            public void writeRecord(Tuple3<String, String, String> tuple) throws IOException {
            	//log.info(tuple.f0+tuple.f1+String.valueOf(tuple.f2));
            	jedis.hset(tuple.f0, tuple.f1, tuple.f2);
            	jedis.expire(tuple.f0, 86400);
            }

            @Override
            public void close() throws IOException {
                jedis.close();
            }
        });
		
        env.execute("Run_flink_1d");
    }
    
    public static class Tokenizer implements FlatMapFunction<String,Tuple3<String, String, String>>{
        public void flatMap(String msg, Collector<Tuple3<String, String, String>> collector) {
        	try {
            	String[] data = msg.split("\\|");
            	//String userProxyId = data[0];
            	//for (String items : data[1].split(",")) {
            		//Map<String,String> item = JSONObject.parseObject(items, Map.class);
            		//String[] key = item.get("key").split("_");
            		//String imp = item.get("imp");
            		//String click = item.get("click");
            	collector.collect(new Tuple3<>(data[0], data[1], data[2]));
                

            } catch (Exception e) {
                log.error("flatMap error msg is {} value is {}", e.getMessage(), msg);
            }
        	
        }
    }
    
   

   
}



