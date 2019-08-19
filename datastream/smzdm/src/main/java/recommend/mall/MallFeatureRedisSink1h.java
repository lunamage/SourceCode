package recommend.mall;

import com.alibaba.fastjson.JSONObject;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPipeline;
import redis.clients.jedis.ShardedJedisPool;
import utils.ReadConfig;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class MallFeatureRedisSink1h extends RichSinkFunction<MallFeature.ItemFeatureEntity> {
	
	private static Logger log = LoggerFactory.getLogger(MallFeatureRedisSink1h.class);
	private ShardedJedisPool jedisPool;
	
    @Override
    public void open(Configuration parameters) {
    	 // 创建jedis池配置实例
        JedisPoolConfig config = new JedisPoolConfig();
        // #jedis的最大分配对象#
        config.setMaxTotal(Integer.valueOf(ReadConfig.getProperties("jedis.pool.maxActive")));
        // #jedis最大保存idel状态对象数 #
        config.setMaxIdle(Integer.valueOf(ReadConfig.getProperties("jedis.pool.maxIdle")));
        List<JedisShardInfo> rtShard = Arrays.asList(
                new JedisShardInfo(ReadConfig.getProperties("redis.rt.article.m1"), Integer.valueOf(ReadConfig.getProperties("redis.port")), Integer.valueOf(ReadConfig.getProperties("jedis.pool.timeout"))),
                new JedisShardInfo(ReadConfig.getProperties("redis.rt.article.m2"), Integer.valueOf(ReadConfig.getProperties("redis.port")), Integer.valueOf(ReadConfig.getProperties("jedis.pool.timeout"))));
        this.jedisPool = new ShardedJedisPool(config, rtShard);
    }


	    @Override
	    public void invoke(MallFeature.ItemFeatureEntity value, SinkFunction.Context context) {
	        String mall = value.getMall();
	        String windowEnd = value.getWindowEnd();
	        Map<String, Number> impMap = new HashMap<>();
	        Map<String, Number> secMap = new HashMap<>();
	        
	        int i=0;
	        int s=0;
	        for (Map.Entry<String, Object[]> entry : value.getVal().entrySet()) {
	            Object[] vals = entry.getValue();
	            String cate3Id = entry.getKey();
	            if((Integer) vals[0] !=0 ) {
	            	impMap.put("cate_"+cate3Id, (Integer) vals[0]);
	            }
	            if((Integer) vals[1] !=0 ) {
	            	secMap.put("cate_"+cate3Id, (Integer) vals[1]);
	            }
	            i+=(Integer) vals[0];
	            s+=(Integer) vals[1];
	        }
	        if(i>0) {impMap.put("totle", i);}
	        if(s>0) {secMap.put("totle", s);}
	        
	        Map<String, String> RedisMap = new HashMap<>();
	        
	        if(impMap.size()!=0){
	        	RedisMap.put("1h_imp", JSONObject.toJSONString(impMap));
		        RedisMap.put("1h_impht", windowEnd);
	        }
	        
	        if(secMap.size()!=0){
	        	RedisMap.put("1h_sec", JSONObject.toJSONString(secMap));
		        RedisMap.put("1h_secht", windowEnd);
	        }
	        
	        try (ShardedJedis jedis = jedisPool.getResource()) {
            	jedis.hmset("lastmall_"+mall, RedisMap);    	
            	jedis.expire("lastmall_"+mall, 86400);
        	}catch (Exception e) {
	            log.error("hset redis error mall is {} msg is {} ", mall, e.getMessage());
	        }
	    }
}
