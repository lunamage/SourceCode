package search.query;

import com.alibaba.fastjson.JSONObject;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import search.query.QueryRealtimeToKafka.ItemFeatureEntity;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple14;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utils.DateUtils;
import utils.ReadConfig;
import utils.Utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/**
 * @author: zyl
 * @date: 2019/6/26
 * @time: 3:11 PM
 * @Description:
 */
public class QueryRealtimeFlatMapper12h extends RichFlatMapFunction<ItemFeatureEntity, String> {

    /**
	 * 
	 */
	private static final long serialVersionUID = -8284916412921643582L;
	private static Logger log = LoggerFactory.getLogger(QueryRealtimeFlatMapper12h.class);
	
	
	@Override
    public void flatMap(ItemFeatureEntity value, Collector<String> collector) {
    	
    	String queryId = value.getQueryId();
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
        
		try {
	        for (Map.Entry<String, Object[]> entry : value.getVal().entrySet()) {
	            String k = entry.getKey();
	            Object[] vals = entry.getValue();
	            if(!String.valueOf(vals[0]).equals("0")) {
	            	queryClickMap.put("rcl_"+k+"_12h", String.valueOf(vals[0]));
		            queryClickMapTmp.put(k, String.valueOf(vals[0]));
		            queryClickRfMap.put(k, String.valueOf(vals[1]));
	            }
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
	        
	        collector.collect(windowEnd+"|rqac_" + queryIdkey+"|"+queryId+"_12h|"+JSONObject.toJSONString(queryClickMap));
	        collector.collect(windowEnd+"|rqac_" + queryIdkey+"|"+queryId+"_12ht|"+windowEnd);
	        collector.collect(windowEnd+"|rqac_" + queryIdkey+"|"+queryId+"_12h_rf|"+JSONObject.toJSONString(queryClickRfMap));
	        collector.collect(windowEnd+"|rqac_" + queryIdkey+"|"+queryId+"_12h_rank|"+JSONObject.toJSONString(queryClickRankMap));
	        collector.collect(windowEnd+"|rqac_" + queryIdkey+"|"+queryId+"_12h_rank_rf|"+JSONObject.toJSONString(queryClickRankRfMap));
	        collector.collect(windowEnd+"|rqac_" + queryIdkey+"|"+queryId+"_12h_ratio|"+JSONObject.toJSONString(queryClickratioMap));

	        if (queryCtrMap.size() != 0) {
	        	collector.collect(windowEnd+"|rqac_" + queryIdkey+"|"+queryId+"_12h_ctr|"+JSONObject.toJSONString(queryCtrMap));
	        	queryCtrRankMap = Utils.mapOrder(queryCtrMap);
	        	collector.collect(windowEnd+"|rqac_" + queryIdkey+"|"+queryId+"_12h_ctr_rank|"+JSONObject.toJSONString(queryCtrRankMap));
	        	}
        	} catch (Exception e) {
        		log.error("flatMap error msg is {} value is {}", e.getMessage(), queryId,e);
        		return;
        	}
    }
    
}
