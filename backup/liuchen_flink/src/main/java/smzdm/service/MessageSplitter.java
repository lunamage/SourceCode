package smzdm.service;

import com.alibaba.fastjson.JSON;
import smzdm.entity.ImpLogEntity;
import smzdm.entity.ItemEntity;
import smzdm.entity.PropertyEntity;
import smzdm.utils.Hashids;
import smzdm.utils.Utils;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.*;

/**
 * 消息解析器
 */
public class MessageSplitter extends RichFlatMapFunction<String, Tuple4<String, String, String, Long>> {

    private static final Logger logger = LoggerFactory.getLogger(MessageSplitter.class);
    
    private static final Set<String> IMP_EA_VALUES = Set.of("01", "35", "36");
    private static final Set<String> IMP_EC_VALUES = Set.of("01", "06");
    private static final Set<String> CLICK_EA_VALUES = Set.of("55", "56", "57", "58", "59", "60");
    private static final Set<String> CLICK_EC_VALUES = Set.of("首页", "好价");
    
    private static final Hashids hashids = new Hashids("smzdm-article", 6, "abcdefghijklmnopqrstuvwxyz1234567890");
    
    private JedisPool jedisPool;
    private final Properties properties;

    public MessageSplitter(Properties properties) {
        this.properties = properties;
    }

    @Override
    public void open(Configuration parameters) {
        initializeJedisPool();
    }

    private void initializeJedisPool() {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(10);
        config.setMaxIdle(5);
        config.setTestOnBorrow(true);
        
        String host = properties.getProperty("redis.article.detail");
        int port = Integer.parseInt(properties.getProperty("redis.port", "6379"));
        int timeout = Integer.parseInt(properties.getProperty("jedis.pool.timeout", "2000"));
        
        this.jedisPool = new JedisPool(config, host, port, timeout);
        logger.info("Jedis pool initialized: {}:{}", host, port);
    }

    @Override
    public void flatMap(String message, Collector<Tuple4<String, String, String, Long>> collector) {
        try {
            ImpLogEntity entity = parseMessage(message);
            if (entity == null) return;
            
            long timestamp = getTimestamp(entity);
            String userProxyId = getUserProxyId(entity);
            
            if (timestamp <= 0 || userProxyId == null) return;
            
            if (isImpressionEvent(entity)) {
                processImpressionEvent(entity, userProxyId, timestamp, collector);
            } else if (isClickEvent(entity)) {
                processClickEvent(entity, userProxyId, timestamp, collector);
            }
        } catch (Exception e) {
            logger.error("Error processing message: {}", message, e);
        }
    }

    private ImpLogEntity parseMessage(String message) {
        try {
            return JSON.parseObject(message, ImpLogEntity.class);
        } catch (Exception e) {
            logger.debug("Failed to parse message: {}", message);
            return null;
        }
    }

    private long getTimestamp(ImpLogEntity entity) {
        String slt = entity.getSlt();
        if (slt == null || slt.equals("0")) {
            return System.currentTimeMillis();
        }
        try {
            return Long.parseLong(slt);
        } catch (NumberFormatException e) {
            return System.currentTimeMillis();
        }
    }

    private String getUserProxyId(ImpLogEntity entity) {
        String uid = entity.getUid();
        return (uid != null && !uid.equals("0")) ? uid : getDeviceId(entity);
    }

    private boolean isImpressionEvent(ImpLogEntity entity) {
        String ea = entity.getEa();
        String ec = entity.getEc();
        return IMP_EA_VALUES.contains(ea) && IMP_EC_VALUES.contains(ec);
    }

    private boolean isClickEvent(ImpLogEntity entity) {
        String ea = entity.getEa();
        String ec = entity.getEc();
        return CLICK_EA_VALUES.contains(ea) || CLICK_EC_VALUES.contains(ec);
    }

    private void processImpressionEvent(ImpLogEntity entity, String userProxyId, 
                                      long timestamp, Collector<Tuple4<String, String, String, Long>> collector) {
        try {
            String itemId = extractItemIdFromImpression(entity);
            if (itemId != null) {
                PropertyEntity property = getPropertyFromRedis(itemId);
                if (property != null) {
                    emitFeatureTuples(userProxyId, property, Utils.IMP, timestamp, collector);
                }
            }
        } catch (Exception e) {
            logger.error("Error processing impression event", e);
        }
    }

    private void processClickEvent(ImpLogEntity entity, String userProxyId,
                                 long timestamp, Collector<Tuple4<String, String, String, Long>> collector) {
        try {
            String itemId = extractItemIdFromClick(entity);
            if (itemId != null) {
                PropertyEntity property = getPropertyFromRedis(itemId);
                if (property != null) {
                    emitFeatureTuples(userProxyId, property, Utils.CLICK, timestamp, collector);
                }
            }
        } catch (Exception e) {
            logger.error("Error processing click event", e);
        }
    }

    private String extractItemIdFromImpression(ImpLogEntity entity) {
        String el = entity.getEl();
        if (el == null) return null;
        
        try {
            ItemEntity[] items = JSON.parseObject(el, ItemEntity[].class);
            if (items != null && items.length > 0) {
                return decodeItemId(items[0].getItemId());
            }
        } catch (Exception e) {
            logger.debug("Failed to parse impression items: {}", el);
        }
        return null;
    }

    private String extractItemIdFromClick(ImpLogEntity entity) {
        String ec = entity.getEc();
        String el = entity.getEl();
        
        if (el == null) return null;
        
        String[] parts = el.split("\\|");
        return getClickItemId(ec, parts);
    }

    private String decodeItemId(String itemId) {
        if (itemId == null) return null;
        
        try {
            if (Utils.isNumeric(itemId)) {
                return itemId;
            } else {
                long[] decoded = hashids.decode(itemId);
                return decoded.length > 0 ? String.valueOf(decoded[0]) : null;
            }
        } catch (Exception e) {
            logger.debug("Failed to decode item ID: {}", itemId);
            return null;
        }
    }

    private void emitFeatureTuples(String userProxyId, PropertyEntity property, String actionType, 
                                 long timestamp, Collector<Tuple4<String, String, String, Long>> collector) {
        
        if (property.getCate() != null) {
            String[] categories = property.getCate().split(",");
            for (String cate : categories) {
                if (!cate.trim().isEmpty()) {
                    collector.collect(new Tuple4<>(userProxyId, Utils.CATE + "_" + cate.trim(), actionType, timestamp));
                }
            }
        }
        
        if (property.getBrand() != null) {
            String[] brands = property.getBrand().split(",");
            for (String brand : brands) {
                if (!brand.trim().isEmpty()) {
                    collector.collect(new Tuple4<>(userProxyId, Utils.BRAND + "_" + brand.trim(), actionType, timestamp));
                }
            }
        }
    }

    private PropertyEntity getPropertyFromRedis(String itemId) {
        if (itemId == null) return null;
        
        try (Jedis jedis = jedisPool.getResource()) {
            String key = "frt_" + itemId;
            String json = jedis.get(key);
            return json != null ? JSON.parseObject(json, PropertyEntity.class) : null;
        } catch (Exception e) {
            logger.error("Error loading property from Redis for item: {}", itemId, e);
            return null;
        }
    }

    private String getDeviceId(ImpLogEntity entity) {
        String dm = entity.getDm();
        
        if ("ios".equals(dm)) {
            String did = entity.getDid();
            return (did != null && !did.trim().isEmpty()) ? did.trim() : null;
        } else if ("android".equals(dm)) {
            String imei = entity.getImei();
            return (imei != null && !imei.trim().isEmpty()) ? imei.trim() : null;
        }
        
        return null;
    }

    private String getClickItemId(String ec, String[] parts) {
        if ("首页".equals(ec) && parts.length > 4) {
            String itemId = parts[4];
            return Utils.isNumeric(itemId) ? itemId : null;
        } else if ("好价".equals(ec) && parts.length > 1) {
            String itemId = parts[1];
            return Utils.isNumeric(itemId) ? itemId : null;
        }
        return null;
    }

    @Override
    public void close() throws Exception {
        if (jedisPool != null) {
            jedisPool.close();
        }
        super.close();
    }
}
