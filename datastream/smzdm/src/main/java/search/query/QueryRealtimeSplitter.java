package search.query;

import com.alibaba.fastjson.JSONObject;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

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
public class QueryRealtimeSplitter extends RichFlatMapFunction<String, Tuple5<String, String, Double, String, Long>> {

    /**
	 * 
	 */
	private static final long serialVersionUID = -8284916412921643582L;
	private static Logger log = LoggerFactory.getLogger(QueryRealtimeSplitter.class);
	
	private JedisPool jedisPool;
	private Map<String, String> queryMap = new ConcurrentHashMap<>();
	
	
	@Override
    public void open(Configuration parameters) {
        // 创建jedis池配置实例
        JedisPoolConfig config = new JedisPoolConfig();
        // #jedis的最大分配对象#
        config.setMaxTotal(Integer.valueOf(ReadConfig.getProperties("jedis.pool.maxActive")));
        // #jedis最大保存idel状态对象数 #
        config.setMaxIdle(Integer.valueOf(ReadConfig.getProperties("jedis.pool.maxIdle")));
        // #在borrow一个jedis实例时，是否提前进行validate操作；如果为true，则得到的jedis实例均是可用的
        //config.setTestOnBorrow(false);
        // #jedis调用returnObject方法时，是否进行有效检查 #
        //config.setTestOnReturn(false);

        this.jedisPool = new JedisPool(config, ReadConfig.getProperties("redis.query.address"),
                Integer.valueOf(ReadConfig.getProperties("redis.port")),
                Integer.valueOf(ReadConfig.getProperties("jedis.pool.timeout")));

        Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.HOUR_OF_DAY, 3);//控制小时
        calendar.set(Calendar.MINUTE, 30);//控制分钟
        calendar.set(Calendar.SECOND, 0);//控制秒
        Date time = calendar.getTime();//执行任务时间为3:00:00
        Date newDate = DateUtils.getDateBeforeOrAfterDays(time, 1);
        Timer timer = new Timer();
        //每天定时3:00执行操作，延迟一天后再执行
        timer.schedule(new TimerTaskClearMap(), newDate, 1000 * 60 * 60 * 24);
    }
	

    @SuppressWarnings("unchecked")
	@Override
    public void flatMap(String msg, Collector<Tuple5<String, String, Double, String, Long>> collector) {
    	
    	/*曝光：ec='搜索' and ea='点击'*/
		try {
			if(!isJson(msg)) {return;}
			Map<String,String> sdk = JSONObject.parseObject(msg, Map.class);
			if(!isJson(sdk.get("ecp"))) {return;}
			Map<String,String> ecp = JSONObject.parseObject(sdk.get("ecp"), Map.class);
			if(ecp == null) {return;}
			//解析json
			String ec = sdk.get("ec");
			Long timestamp = System.currentTimeMillis();
			
			if(ec.equals("搜索")) {
				String articleId = ecp.get("4");
				String channelId = ecp.get("28");
				String query = ecp.get("46");
				Double positionValue = Utils.getQueryPositionValue(ecp.get("p")) == null ? 28.57 : Utils.getQueryPositionValue(ecp.get("p"));
				//异常值处理
				if(articleId == null) {return;}
				if(channelId == null || !channelId.matches("^1|2|5|21$")) {return;}
				if(query == null) {return;}
				String queryId = getQueryId(Utils.getMD5(query.toUpperCase()));
				if(queryId == null) {return;}
				collector.collect(new Tuple5<>(queryId,channelId+"_"+articleId,positionValue,"click",timestamp));
			}
			
			if(ec.equals("04")) {
				String articleId = ecp.get("a");
				String channelId = ecp.get("c");
				String query = ecp.get("qu");
				Double positionValue = Utils.getQueryPositionValue(ecp.get("p")) == null ? 28.57 : Utils.getQueryPositionValue(ecp.get("p"));
				
				String spp = ecp.get("spp");
				String srm = ecp.get("srm");
				String cd37 = ecp.get("37");
				
				//异常值处理
				if(articleId == null) {return;}
				if(channelId == null || !channelId.matches("^1|2|5|21$")) {return;}
				if(spp == null || !spp.equals("综合")) {return;}
				if(srm == null || !srm.equals("通用_0")) {return;}
				if(cd37 == null || !Pattern.compile("内容:无_分类:无_商城:无_品牌:无_价格:无~无").matcher(cd37).find() || !Pattern.compile("综合排序|最新排序").matcher(cd37).find()) {return;}
				
				
				if(query == null) {return;}
				String queryId = getQueryId(Utils.getMD5(query.toUpperCase()));
				if(queryId == null) {return;}
				collector.collect(new Tuple5<>(queryId,channelId+"_"+articleId,positionValue,"imp",timestamp));
			}

        	} catch (Exception e) {
        		log.error("flatMap error msg is {} value is {}", e.getMessage(), msg,e);
        		return;
        	}
    }
    
    private String getQueryId(String query) {
        String queryId = queryMap.get(query);
        if (queryId != null) return queryId;
        // 未缓存，则读取redis
        try (Jedis jedis = jedisPool.getResource()) {
        	queryId = jedis.get(query);
            if (queryId == null) return null;
            queryMap.put(query, queryId);
            return queryId;
        } catch (Exception e) {
            log.error("flatMap jedis get value error msg is {} itemId is {} ", e.getMessage(), query, e);
        }
        return null;
    }

    
    
    
    public boolean isJson(String content){
  	  try {
  		  JSONObject.parseObject(content);
  		  return true;
  	  	} catch (Exception e) {
  	  		return false;
  	  	}
  	}
    
    class TimerTaskClearMap extends TimerTask {
        // 每日3点定期清空缓存的文章数据
        @Override
        public void run() {
            log.info("delete catesMap size {} , time {} ", queryMap.size(), DateUtils.formatDate(new Date(), DateUtils.YYYYMMDD_HMS));
            queryMap.clear();
        }
    }
}
