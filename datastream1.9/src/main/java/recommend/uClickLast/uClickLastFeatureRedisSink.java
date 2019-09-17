package recommend.uClickLast;

import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.base.Strings;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;
import utils.ReadConfig;
import utils.Utils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class uClickLastFeatureRedisSink extends RichSinkFunction<Tuple8<String, String, String, String, String, String, String, Long>> {
	
	private static Logger log = LoggerFactory.getLogger(uClickLastFeatureRedisSink.class);
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
	    public void invoke(Tuple8<String, String, String, String, String, String, String, Long> value, SinkFunction.Context context) {
	        
	    	String userProxyId = value.f0;
	    	String level1 = value.f1;
	    	String level2 = value.f2;
	    	String level3 = value.f3;
	    	String level4 = value.f4;
	    	String brand = value.f5;
	    	String mall = value.f6;
	    	String timestamp  = String.valueOf(value.f7);
	    	
	    	Map<String,String> result = new HashMap<>();
	    	if(!Strings.isNullOrEmpty(level1) && !level1.equals("0")) {
	    		result.put("cate1_"+level1, timestamp);
	    		}
	    	else {
	    		result.put("cate1_-1", timestamp);
	    	}
	    	
	    	if(!Strings.isNullOrEmpty(level2) && !level2.equals("0")) {
	    		result.put("cate2_"+level2, timestamp);
	    		}
	    	else {
	    		result.put("cate2_-1", timestamp);
	    	}
	    	if(!Strings.isNullOrEmpty(level3) && !level3.equals("0")) {
	    		result.put("cate3_"+level3, timestamp);
	    		}
	    	else {
	    		result.put("cate3_-1", timestamp);
	    	}
	    	if(!Strings.isNullOrEmpty(level4) && !level4.equals("0")) {
	    		result.put("cate4_"+level4, timestamp);
	    		}
	    	else {
	    		result.put("cate4_-1", timestamp);
	    	}
	    	if(!Strings.isNullOrEmpty(brand) && !brand.equals("0")) {
	    		result.put("brand_"+brand, timestamp);
	    		}
	    	else {
	    		result.put("brand_-1", timestamp);
	    	}
	    	if(Utils.isContainsMall(mall)) {
	    		result.put("mall_"+mall, timestamp);
	    		}
	    	else {
	    		result.put("mall_-1", timestamp);
	    	}
	    	
	    	Map<String,String> redisMap = new HashMap<>();
	    	HashMap<String,Object[]> resultMap = new HashMap<>();
	        
	        try (ShardedJedis jedis = jedisPool.getResource()) {
	        	redisMap = jedis.hgetAll("uclicklast_"+userProxyId);
	        	
	        	if(redisMap != null) {
		        	resultMap=getMapResult(redisMap);
		        	
		        	//if((Long) resultMap.get("cate1")[0]>=10 && !redisMap.containsKey("cate1_"+level1)) {
		        	//	jedis.hdel("uclicklast_"+userProxyId, "cate1_"+level1);
		        	//}
		        	
		        	if((Long) resultMap.get("cate2")[0]>=50 && !redisMap.containsKey("cate2_"+level2) && !Strings.isNullOrEmpty(level2) && !level2.equals("0")) {
		        		jedis.hdel("uclicklast_"+userProxyId, (String) resultMap.get("cate2")[1]);
		        	}
		        	
		        	if((Long) resultMap.get("cate3")[0]>=100 && !redisMap.containsKey("cate3_"+level3) && !Strings.isNullOrEmpty(level3) && !level3.equals("0")) {
		        		jedis.hdel("uclicklast_"+userProxyId, (String) resultMap.get("cate3")[1]);
		        	}
		        	
		        	if((Long) resultMap.get("cate4")[0]>=100 && !redisMap.containsKey("cate4_"+level4) && !Strings.isNullOrEmpty(level4) && !level4.equals("0")) {
		        		jedis.hdel("uclicklast_"+userProxyId, (String) resultMap.get("cate4")[1]);
		        	}
		        	
		        	if((Long) resultMap.get("brand")[0]>=200 && !redisMap.containsKey("brand_"+brand) && !Strings.isNullOrEmpty(brand) && !brand.equals("0")) {
		        		jedis.hdel("uclicklast_"+userProxyId, (String) resultMap.get("brand")[1]);
		        	}
		        	
		        	//if((Long) resultMap.get("mall")[0]>=10 && !redisMap.containsKey("mall_"+mall)) {
		        	//	jedis.hdel("uclicklast_"+userProxyId, "mall_"+mall);
		        	//}
	        	}
	        	
	        	jedis.hmset("uclicklast_"+userProxyId, result);           	
        	}catch (Exception e) {
	            log.error("hset redis error mall is {} msg is {} ", userProxyId, e.getMessage(), e);
	        }
	    }
	    
	    
	    
	    public static HashMap<String,Object[]> getMapResult(Map<String, String> map){
	    	HashMap<String,Object[]> m = new HashMap<>();
	    	Long cate1Count = 0L;
	    	Long cate2Count = 0L;
	    	Long cate3Count = 0L;
	    	Long cate4Count = 0L;
	    	Long brandCount = 0L;
	    	Long mallCount = 0L;
	    	
	    	String cate1Key="";
	    	String cate2Key="";
	    	String cate3Key="";
	    	String cate4Key="";
	    	String brandKey="";
	    	String mallKey="";
	    	
	    	Long cate1Value = 0L;
	    	Long cate2Value = 0L;
	    	Long cate3Value = 0L;
	    	Long cate4Value = 0L;
	    	Long brandValue = 0L;
	    	Long mallValue = 0L;
	    	
	    	
	    	for(Map.Entry<String, String> entry : map.entrySet()){
	             String mapKey = entry.getKey();
	             Long mapValue = Long.valueOf(entry.getValue());
	             //cate1
	             if(mapKey.contains("cate1")) {
	            	 if(mapValue < cate1Value || cate1Value == 0) {
	            		 cate1Value=mapValue;
	            		 cate1Key=mapKey;
	            	 }
	            	 cate1Count += 1;
	             }
	             //cate2
	             if(mapKey.contains("cate2")) {
	            	 if(mapValue < cate2Value || cate2Value == 0) {
	            		 cate2Value=mapValue;
	            		 cate2Key=mapKey;
	            	 }
	            	 cate2Count += 1;
	             }
	             //cate3
	             if(mapKey.contains("cate3")) {
	            	 if(mapValue < cate3Value || cate3Value == 0) {
	            		 cate3Value=mapValue;
	            		 cate3Key=mapKey;
	            	 }
	            	 cate3Count += 1;
	             }
	             //cate4
	             if(mapKey.contains("cate4")) {
	            	 if(mapValue < cate4Value || cate4Value == 0) {
	            		 cate4Value=mapValue;
	            		 cate4Key=mapKey;
	            	 }
	            	 cate4Count += 1;
	             }
	             //brand
	             if(mapKey.contains("brand")) {
	            	 if(mapValue < brandValue || brandValue == 0) {
	            		 brandValue=mapValue;
	            		 brandKey=mapKey;
	            	 }
	            	 brandCount += 1;
	             }
	             //mall
	             if(mapKey.contains("mall")) {
	            	 if(mapValue < mallValue || mallValue == 0) {
	            		 mallValue=mapValue;
	            		 mallKey=mapKey;
	            	 }
	            	 mallCount += 1;
	             } 
	         }
	    	
	    	m.put("cate1", new Object[]{cate1Count, cate1Key});
	    	m.put("cate2", new Object[]{cate2Count, cate2Key});
	    	m.put("cate3", new Object[]{cate3Count, cate3Key});
	    	m.put("cate4", new Object[]{cate4Count, cate4Key});
	    	m.put("brand", new Object[]{brandCount, brandKey});
	    	m.put("mall", new Object[]{mallCount, mallKey});
			
			return m;
		}
	    
}
