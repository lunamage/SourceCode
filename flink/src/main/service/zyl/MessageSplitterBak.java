package zyl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.zdm.zyl.DateUtils;
import com.zdm.zyl.Hashids;
import com.zdm.zyl.ReadConfig;
import com.zdm.zyl.Utils;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.base.Strings;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author: liuchen
 * @date: 2019/1/14
 * @time: 3:11 PM
 * @Description:
 */
public class MessageSplitterBak extends RichFlatMapFunction<String, Tuple4<String, String, String, Long>> {

    private static Logger log = LoggerFactory.getLogger(MessageSplitterBak.class);

    private final static List<String> IMP_EC_VAL = Arrays.asList("01", "06");
    private final static List<String> IMP_EA_VAL = Arrays.asList("01", "35", "36");

    private final static List<String> CLICK_EC_VAL = Arrays.asList("首页", "好价");
    private final static List<String> CLICK_EA_VAL = Arrays.asList("首页站内文章点击", "精选好价", "全部好价", "全部好价_文章点击");
    
    private final static List<String> S_CHANNEL = Arrays.asList("youhui","haitao","faxian");
    
    private static final String HASH_NAME = "smzdm-article";

    private static final int HASH_LENGTH = 6;

    private static final String HASH_SALT = "abcdefghijklmnopqrstuvwxyz1234567890";
    
    private final static String ITEM_BASE_INFO = "ib_%s_%s";

    private static Hashids hashids = new Hashids(HASH_NAME, HASH_LENGTH, HASH_SALT);

    private JedisPool jedisPool;
    
    private ShardedJedisPool jedisFeaturePool;


    private Map<String, PropertyEntity> catesMap = new ConcurrentHashMap<>();

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

        this.jedisPool = new JedisPool(config, ReadConfig.getProperties("redis.article.detail"),
                Integer.valueOf(ReadConfig.getProperties("redis.port")),
                Integer.valueOf(ReadConfig.getProperties("jedis.pool.timeout")));

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
    public void flatMap(String msg, Collector<Tuple4<String, String, String, Long>> collector) {
        try {
            ImpLogEntity entity = JSONObject.parseObject(msg, ImpLogEntity.class);
            //Long timestamp = entity.getSlt() == null || Objects.equals(entity.getSlt(), "0") ? System.currentTimeMillis() : Long.valueOf(entity.getSlt()) * 1000;
            Long timestamp = System.currentTimeMillis();
            // 曝光数据
            if (Objects.equals("show", entity.getType()) && IMP_EA_VAL.contains(entity.getEa()) && IMP_EC_VAL.contains(entity.getEc())) {
                String userId = entity.getUid();
                // 获取用户id，优先userId，否则deviceId
                String userProxyId = Strings.isNullOrEmpty(userId) || Objects.equals(userId, "0") ? getDeviceid(entity) : userId;
                ItemEntity itemEntity = JSON.parseObject(entity.getEcp(), ItemEntity.class);
                if (!Objects.equals(itemEntity.getSp(), "0"))
                    return;
                String itemId = itemEntity.getItemId();
                if (Strings.isNullOrEmpty(itemId))
                    return;
                if (itemId.startsWith("a")) {
                    StringBuilder articleId = new StringBuilder();
                    String hashId = itemId.substring(1);
                    long[] ar = hashids.decode(hashId);
                    for (long anAr : ar) {
                        articleId.append(anAr);
                    }
                    itemId = articleId.toString();
                }
                PropertyEntity property = getProperty(itemId);
                if (property == null) {
                    return;}
                if (!Strings.isNullOrEmpty(property.getCate())) {
                    for (String cate : property.getCate().split(",")) {
                        collector.collect(new Tuple4<>(userProxyId, Utils.CATE + "_" + cate, Utils.IMP, timestamp));
                    }
                }
                if (!Strings.isNullOrEmpty(property.getBrand())) {
                    for (String brand : property.getBrand().split(",")) {
                        collector.collect(new Tuple4<>(userProxyId, Utils.BRAND + "_" + brand, Utils.IMP, timestamp));
                    }
                }
                return;
            }

            // 点击数据
            if (Objects.equals("event", entity.getType()) && CLICK_EA_VAL.contains(entity.getEa())
                    && CLICK_EC_VAL.contains(entity.getEc())) {
                String userId = entity.getUid();
                // 获取用户id，优先userId，否则deviceId
                String userProxyId = Strings.isNullOrEmpty(userId) || Objects.equals(userId, "0") ? getDeviceid(entity) : userId;
                String el = entity.getEl();
                if (el == null)
                    return;
                String itemId = getClickItemId(entity.getEc(), el.split("_"));
                if (Strings.isNullOrEmpty(itemId))
                    return;
                if (itemId.startsWith("a")) {
                    StringBuilder articleId = new StringBuilder();
                    String hashId = itemId.substring(1);
                    long[] ar = hashids.decode(hashId);
                    for (long anAr : ar) {
                        articleId.append(anAr);
                    }
                    itemId = articleId.toString();
                }
                PropertyEntity property = getProperty(itemId);
                
                if (property == null)
                    return;
                if (!Strings.isNullOrEmpty(property.getCate())) {
                    for (String cate : property.getCate().split(",")) {
                        collector.collect(new Tuple4<>(userProxyId, Utils.CATE + "_" + cate, Utils.CLICK, timestamp));
                    }
                }
                if (!Strings.isNullOrEmpty(property.getBrand())) {
                    for (String brand : property.getBrand().split(",")) {
                        collector.collect(new Tuple4<>(userProxyId, Utils.BRAND + "_" + brand, Utils.CLICK, timestamp));
                    }
                }
                
            }
            
         // 搜索点击数据
            if (Objects.equals("event", entity.getType()) && Objects.equals("点击",entity.getEa()) && Objects.equals("搜索",entity.getEc())) {
                String userId = entity.getUid();
                // 获取用户id，优先userId，否则deviceId
                String userProxyId = Strings.isNullOrEmpty(userId) || Objects.equals(userId, "0") ? getDeviceid(entity) : userId;
                Map<String,String> ecp = JSONObject.parseObject(entity.getEcp(), Map.class);
                String cd11 = ecp.get("11");
                String cd4 = ecp.get("4");
                
                if (S_CHANNEL.contains(cd11)) {
                	String itemId = cd4;
                	if (Strings.isNullOrEmpty(itemId))
                		return;
                	
	                PropertyEntity property = getProperty(itemId);
	                
	                if (property == null)
	                    return;
	                if (!Strings.isNullOrEmpty(property.getCate())) {
	                    for (String cate : property.getCate().split(",")) {
	                        collector.collect(new Tuple4<>(userProxyId, Utils.CATE + "_" + cate, "s_click", timestamp));
	                    }
	                }
	                if (!Strings.isNullOrEmpty(property.getBrand())) {
	                    for (String brand : property.getBrand().split(",")) {
	                        collector.collect(new Tuple4<>(userProxyId, Utils.BRAND + "_" + brand, "s_click", timestamp));
	                    }
	                }
                } 
            }
        } catch (Exception e) {
            log.error("flatMap error msg is {} value is {}", e.getMessage(), msg,e);
            return;
        }
    }


    /**
     * 获取商品属性，品类/品牌
     *
     * @param itemId 商品id
     * @return 商品属性
     */
    private PropertyEntity getProperty(String itemId) {
        PropertyEntity property = catesMap.get(itemId);
        if (property != null)
            return property;
        // 未缓存，则读取redis
        try (Jedis jedis = jedisPool.getResource()) {
            Map<String, String> mapVal = jedis.hgetAll("frt_" + itemId);
            if (mapVal != null && mapVal.size() > 0) {
            PropertyEntity entity = new PropertyEntity();
            if (!Strings.isNullOrEmpty(mapVal.get(Utils.CATE))) {
                entity.setCate(mapVal.get(Utils.CATE));
            }
            if (!Strings.isNullOrEmpty(mapVal.get(Utils.BRAND))) {
                entity.setBrand(mapVal.get(Utils.BRAND));
            }
            catesMap.put(itemId, entity);
            return entity;
            }
           }
            catch (Exception e) {
            log.error("flatMap jedis get value error msg is {} itemId is {} ", e.getMessage(), itemId, e);
        }
        
     // 从特征中心获取，不管是否成功，均存入缓存key（防止一些完全没有特征的文章导致频繁访问特征中心）
        try (ShardedJedis jedis = jedisFeaturePool.getResource()) {
            String feature = jedis.get(String.format(ITEM_BASE_INFO, itemId, 3));
            if (Strings.isNullOrEmpty(feature)) {
                return null;
            }
            JSONObject jo = JSON.parseObject(feature);
            String level1 = jo.getString("level1");
            String level2 = jo.getString("level2");
            String level3 = jo.getString("level3");
            String level4 = jo.getString("level4");
            String brandid = jo.getString("brandid");
            StringBuilder cates = new StringBuilder();
            if (!Strings.isNullOrEmpty(level1) && !Objects.equals(level1, "0")) {
                cates.append(level1).append(",");
            }
            if (!Strings.isNullOrEmpty(level2) && !Objects.equals(level2, "0")) {
                cates.append(level2).append(",");
            }
            if (!Strings.isNullOrEmpty(level3) && !Objects.equals(level3, "0")) {
                cates.append(level3).append(",");
            }
            if (!Strings.isNullOrEmpty(level4) && !Objects.equals(level4, "0")) {
                cates.append(level4).append(",");
            }
            PropertyEntity entity = new PropertyEntity();
            if (cates.length() > 0) {
                entity.setCate(cates.substring(0, cates.length() - 1));
                entity.setBrand(Strings.isNullOrEmpty(brandid) ? "" : brandid);
                catesMap.put(itemId, entity);
                return entity;
            }
            return entity;
        } catch (Exception e) {
            log.error("flatMap jedisFeaturePool error msg is {} itemId is {} ", e.getMessage(), itemId, e);
        }
        
        return null;
    }


    // 在点击事件中获取文章id
    private String getClickItemId(String ec, String[] els) {
        if (Objects.equals(ec, "首页")) {
            // itemId获取位置不同，1或2
            return Utils.isNumeric(els[1]) ? els[1] : els[2];
        }
        if (Objects.equals(ec, "好价")) {
            return els[0];
        }
        return null;
    }

    // 获取用户deviceid
    private String getDeviceid(ImpLogEntity entity) {
        String deviceId = ("1".equals(entity.getDt()) || "3".equals(entity.getDt())) ? (entity.getDid() == null ? "" : entity.getDid()) : entity.getNd() == null ? "" : entity.getNd();
        deviceId = deviceId.replaceAll(" ", "+");
        return deviceId;
    }

    class TimerTaskClearMap extends TimerTask {
        // 每日3点定期清空缓存的文章数据
        @Override
        public void run() {
            log.info("delete catesMap size {} , time {} ", catesMap.size(), DateUtils.formatDate(new Date(), DateUtils.YYYYMMDD_HMS));
            catesMap.clear();
        }
    }
}
