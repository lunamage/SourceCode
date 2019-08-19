package search.query;

import com.alibaba.fastjson.JSONObject;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;
import utils.ReadConfig;
import utils.Utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class QueryRealtimeRedisSink3h extends RichSinkFunction<QueryRealtime.ItemFeatureEntity> {
	 private static Logger log = LoggerFactory.getLogger(QueryRealtimeRedisSink3h.class);

	    private JedisPool jedisPool;

	    @Override
	    public void open(Configuration parameters) {
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
	    public void invoke(QueryRealtime.ItemFeatureEntity value, SinkFunction.Context context) {
	        String queryId = value.getQueryId();
	        //Map<String, Object[]> val = value.getVal();
	        String windowEnd=value.getWindowEnd();
	        
	        String queryIdkey=String.valueOf(Integer.parseInt(queryId)%10000);
	        
	        Map<String, String> queryClickMap = new HashMap<>();
	        Map<String, String> queryClickMapTmp = new HashMap<>();
	        Map<String, String> queryClickRfMap = new HashMap<>();
	        Map<String, Integer> queryClickRankMap = new HashMap<>();
	        Map<String, Integer> queryClickRankRfMap = new HashMap<>();
	        Map<String, Double> queryClickratioMap = new HashMap<>();
	        Map<String, String> queryCtrMap = new HashMap<>();
	        Map<String, Integer> queryCtrRankMap = new HashMap<>();
	        
	        for (Map.Entry<String, Object[]> entry : value.getVal().entrySet()) {
	            String k = entry.getKey();
	            Object[] vals = entry.getValue();
	            queryClickMap.put("rcl_"+k+"_3h", String.valueOf(vals[0]));
	            queryClickMapTmp.put(k, String.valueOf(vals[0]));
	            queryClickRfMap.put(k, String.valueOf(vals[1]));
	            //过滤CTR为0数据
	            if(!String.valueOf(vals[2]).equals("0.0")) {
	            	queryCtrMap.put(k, String.valueOf(vals[2]));
	            }
	        }
	        // 无数据不操作。
	        if (queryClickMap.size() == 0 || queryClickRfMap.size() == 0) {return;}
	        
	        queryClickRankMap = Utils.mapOrder(queryClickMapTmp);
	        queryClickRankRfMap = Utils.mapOrder(queryClickRfMap);
	        queryClickratioMap = Utils.mapRatio(queryClickMapTmp);
	        
	        Map<String, String> map = new HashMap<>();
	        map.put(queryId+"_3h", JSONObject.toJSONString(queryClickMap));
	        map.put(queryId+"_3ht", windowEnd);
	        map.put(queryId+"_3h_rf", JSONObject.toJSONString(queryClickRfMap));
	        map.put(queryId+"_3h_rank", JSONObject.toJSONString(queryClickRankMap));
	        map.put(queryId+"_3h_rank_rf", JSONObject.toJSONString(queryClickRankRfMap));
	        map.put(queryId+"_3h_ratio", JSONObject.toJSONString(queryClickratioMap));
	        if (queryCtrMap.size() != 0) {
	        	map.put(queryId+"_3h_ctr", JSONObject.toJSONString(queryCtrMap));
	        	queryCtrRankMap = Utils.mapOrder(queryCtrMap);
	        	map.put(queryId+"_3h_ctr_rank", JSONObject.toJSONString(queryCtrRankMap));
	        }
	        
	        try (Jedis jedis = jedisPool.getResource(); Pipeline line = jedis.pipelined()) {
	            line.hmset("rqac_" + queryIdkey, map);
	            line.expire("rqac_" + queryIdkey, 86400);
	            line.sync();
	        } catch (Exception e) {
	            log.error("hset redis error queryId is {} msg is {} ", queryId, e.getMessage());
	        }
	    }
}
