package recmmend.artRead;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.base.Strings;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utils.DateUtils;
import utils.ReadConfig;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
/**
 * @author: zyl
 * @date: 2019/6/26
 * @time: 3:11 PM
 * @Description:
 */
public class ArtReadFeatureSplitter extends RichFlatMapFunction<String, Tuple4<String, Integer, Double, Long>> {

    /**
	 * 
	 */
	private static final long serialVersionUID = -8284916412921643582L;
	private static Logger log = LoggerFactory.getLogger(ArtReadFeatureSplitter.class);
	
	private final static String ITEM_BASE_INFO = "ib_%s_%s";
	private ShardedJedisPool jedisFeaturePool;
	private Map<String, String> pubDateMap = new ConcurrentHashMap<>();
	
	@Override
    public void open(Configuration parameters) {
        // 创建jedis池配置实例
        JedisPoolConfig config = new JedisPoolConfig();
        // #jedis的最大分配对象#
        config.setMaxTotal(Integer.valueOf(ReadConfig.getProperties("jedis.pool.maxActive")));
        // #jedis最大保存idel状态对象数 #
        config.setMaxIdle(Integer.valueOf(ReadConfig.getProperties("jedis.pool.maxIdle")));
        // #在borrow一个jedis实例时，是否提前进行validate操作；如果为true，则得到的jedis实例均是可用的
        //config.setTestOnBorrow(Boolean.valueOf(ReadConfig.getProperties("jedis.pool.testOnBorrow")));
        // #jedis调用returnObject方法时，是否进行有效检查 #
        //config.setTestOnReturn(Boolean.valueOf(ReadConfig.getProperties("jedis.pool.testOnReturn")));
        List<JedisShardInfo> shards = Arrays.asList(
                new JedisShardInfo(ReadConfig.getProperties("jedis.pool.feature.m1"), Integer.valueOf(ReadConfig.getProperties("redis.port")), Integer.valueOf(ReadConfig.getProperties("jedis.pool.timeout"))),
                new JedisShardInfo(ReadConfig.getProperties("jedis.pool.feature.m2"), Integer.valueOf(ReadConfig.getProperties("redis.port")), Integer.valueOf(ReadConfig.getProperties("jedis.pool.timeout"))));

        this.jedisFeaturePool = new ShardedJedisPool(config, shards);
        
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
	
	@Override
    public void flatMap(String msg, Collector<Tuple4<String, Integer, Double, Long>> collector) {
    	
    	/*曝光：ec='搜索' and ea='点击'*/
		try {
			if(!isJson(msg)) {return;}
			Map<String,String> sdk = JSONObject.parseObject(msg, Map.class);
			if(!isJson(sdk.get("ecp"))) {return;}
			Map<String,String> ecp = JSONObject.parseObject(sdk.get("ecp"), Map.class);
			if(ecp == null) {return;}
			//解析json
			String el = sdk.get("el");
			String channel = ecp.get("11");
			String slt = String.valueOf(sdk.get("slt"));
			Long timestamp = slt == null || Objects.equals(slt, "0") ? System.currentTimeMillis() : Long.valueOf(slt) * 1000;
			
			String[] tmp = el.split("\\_");
			String id = tmp[0];
			String time = tmp[3];
			String finish = tmp[4].replace("%", "");
			
			if(Strings.isNullOrEmpty(channel)) {return;}

			if(!"youhui,faxian,haitao".contains(channel) || Strings.isNullOrEmpty(el) || !isNumber(time) || !isNumber(finish)) {return;}
			
			Long pubTime = Long.valueOf(getPubDate(id));
			//log.info("qaz "+id+" "+time+" "+finish+" "+timestamp+" "+pubTime);

			if(timestamp/1000 - pubTime<=259200) {
				collector.collect(new Tuple4<>(id,Integer.parseInt(time),Double.valueOf(finish),timestamp));
			}

        	} catch (Exception e) {
        		log.error("flatMap error msg is {} value is {}", e.getMessage(), msg,e);
        		return;
        	}
    }
 
    public boolean isJson(String content){
  	  try {
  		  JSONObject.parseObject(content);
  		  return true;
  	  	} catch (Exception e) {
  	  		return false;
  	  	}
  	}
    
    /**
	* 判断字符串是否是整数
	*/
	public static boolean isInteger(String value) {
		try {
			Integer.parseInt(value);
			return true;
			} catch (NumberFormatException e) {
			return false;
		}
	}
	/**
	* 判断字符串是否是浮点数
	*/
	public static boolean isDouble(String value) {
		try {
			Double.parseDouble(value);
			if (value.contains("."))
			return true;
			return false;
			} catch (NumberFormatException e) {
			return false;
		}
	}
	
	/**
	* 判断字符串是否是数字
	*/
	public static boolean isNumber(String value) {
		return isInteger(value) || isDouble(value);
	}
	
	/**
     * 获取商品属性，品类/品牌
     *
     * @param itemId 商品id
     * @return 商品属性
     */
    private String getPubDate(String itemId) {
        String pubDate = pubDateMap.get(itemId);
        if (pubDate != null)
            return pubDate;
        //从特征中心获取，不管是否成功，均存入缓存key（防止一些完全没有特征的文章导致频繁访问特征中心）
        try (ShardedJedis jedis = jedisFeaturePool.getResource()) {
            String feature = jedis.get(String.format(ITEM_BASE_INFO, itemId, 3));
            if (Strings.isNullOrEmpty(feature)) {
                return "0";
            }
            JSONObject jo = JSON.parseObject(feature);
            String pub_time = jo.getString("pub_time");
           
            if (!Strings.isNullOrEmpty(pub_time) && !Objects.equals(pub_time, "0")) {
            	pubDateMap.put(itemId,pub_time);
            	return pub_time;
            }
            return "0";
        } catch (Exception e) {
            log.error("flatMap jedisFeaturePool error msg is {} itemId is {} ", e.getMessage(), itemId, e);
        }
        
        return "0";
    }
	
	
	class TimerTaskClearMap extends TimerTask {
        // 每日3点定期清空缓存的文章数据
        @Override
        public void run() {
            log.info("delete catesMap size {} , time {} ", pubDateMap.size(), DateUtils.formatDate(new Date(), DateUtils.YYYYMMDD_HMS));
            pubDateMap.clear();
        }
    }
    
}
