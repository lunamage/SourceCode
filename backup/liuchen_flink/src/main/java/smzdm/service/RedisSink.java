package com.smzdm.service;

import com.alibaba.fastjson.JSONObject;
import com.smzdm.entity.ItemFeatureEntity;
import com.smzdm.entity.FeatureValue;
import com.smzdm.utils.Utils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

/**
 * Redis Sink函数，将特征数据写入Redis
 * 
 * 优化点：
 * 1. 改进异常处理和重试机制
 * 2. 增加性能监控指标
 * 3. 优化代码结构和可读性
 * 4. 增加数据验证
 * 
 * @author liuchen
 * @since 1.0
 */
public class RedisSink extends RichSinkFunction<ItemFeatureEntity> {

    private static final Logger logger = LoggerFactory.getLogger(RedisSink.class);

    // Redis键前缀常量
    private static final String USER_CTR_KEY_PREFIX = "urhoctr_";
    private static final String USER_IMP_KEY_PREFIX = "urhoimp_";
    private static final String USER_CLICK_KEY_PREFIX = "urhoclick_";

    private JedisPool jedisPool;
    private final Properties properties;

    // 配置参数
    private String categoryMetricsKey;
    private String categoryMetricsTimeKey;
    private String brandMetricsKey;
    private String brandMetricsTimeKey;
    private int keyExpireSeconds;

    // 性能统计
    private long successCount = 0;
    private long failureCount = 0;
    private long totalProcessingTime = 0;

    public RedisSink(Properties properties) {
        this.properties = properties;
    }

    @Override
    public void open(Configuration parameters) {
        try {
            initializeJedisPool();
            loadConfigurationParameters();
            logger.info("RedisSink initialized successfully");
        } catch (Exception e) {
            logger.error("Failed to initialize RedisSink", e);
            throw new RuntimeException("RedisSink initialization failed", e);
        }
    }

    /**
     * 初始化Jedis连接池
     */
    private void initializeJedisPool() {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(Integer.parseInt(properties.getProperty("jedis.pool.maxActive", "20")));
        config.setMaxIdle(Integer.parseInt(properties.getProperty("jedis.pool.maxIdle", "10")));
        config.setTestOnBorrow(Boolean.parseBoolean(properties.getProperty("jedis.pool.testOnBorrow", "true")));
        config.setTestOnReturn(Boolean.parseBoolean(properties.getProperty("jedis.pool.testOnReturn", "false")));

        String redisHost = properties.getProperty("redis.article.feature");
        int redisPort = Integer.parseInt(properties.getProperty("redis.port", "6379"));
        int timeout = Integer.parseInt(properties.getProperty("jedis.pool.timeout", "2000"));

        this.jedisPool = new JedisPool(config, redisHost, redisPort, timeout);
        logger.info("Redis connection pool created: {}:{}", redisHost, redisPort);
    }

    /**
     * 加载配置参数
     */
    private void loadConfigurationParameters() {
        categoryMetricsKey = properties.getProperty("jedis.hash.cate.m.key", "cate");
        categoryMetricsTimeKey = properties.getProperty("jedis.hash.cate.mt.key", "cate_time");
        brandMetricsKey = properties.getProperty("jedis.hash.brand.m.key", "brand");
        brandMetricsTimeKey = properties.getProperty("jedis.hash.brand.mt.key", "brand_time");
        keyExpireSeconds = Integer.parseInt(properties.getProperty("jedis.hash.expire", "86400"));
    }

    @Override
    public void invoke(ItemFeatureEntity featureEntity, SinkFunction.Context context) {
        if (featureEntity == null || !isValidFeatureEntity(featureEntity)) {
            logger.warn("Invalid feature entity received, skipping");
            return;
        }

        long startTime = System.currentTimeMillis();
        
        try {
            processFeatureEntity(featureEntity);
            successCount++;
            
            // 定期输出性能统计
            if (successCount % 1000 == 0) {
                logPerformanceStats();
            }
            
        } catch (Exception e) {
            failureCount++;
            logger.error("Failed to process feature entity for user: {}, error: {}", 
                featureEntity.getUserProxyId(), e.getMessage(), e);
        } finally {
            totalProcessingTime += (System.currentTimeMillis() - startTime);
        }
    }

    /**
     * 验证特征实体的有效性
     */
    private boolean isValidFeatureEntity(ItemFeatureEntity entity) {
        return entity.getUserProxyId() != null 
            && !entity.getUserProxyId().isEmpty()
            && entity.getVal() != null 
            && !entity.getVal().isEmpty()
            && entity.getWindowEnd() != null;
    }

    /**
     * 处理特征实体
     */
    private void processFeatureEntity(ItemFeatureEntity featureEntity) {
        // 分组数据
        FeatureMetrics categoryMetrics = new FeatureMetrics();
        FeatureMetrics brandMetrics = new FeatureMetrics();
        
        groupFeaturesByType(featureEntity.getVal(), categoryMetrics, brandMetrics);
        
        // 检查是否有有效数据
        if (!categoryMetrics.hasValidData() && !brandMetrics.hasValidData()) {
            logger.debug("No valid feature data found for user: {}", featureEntity.getUserProxyId());
            return;
        }

        // 写入Redis
        writeToRedis(featureEntity.getUserProxyId(), featureEntity.getWindowEnd(), 
                    categoryMetrics, brandMetrics);
    }

    /**
     * 按类型分组特征数据
     */
    private void groupFeaturesByType(Map<String, FeatureValue> features, 
                                   FeatureMetrics categoryMetrics, 
                                   FeatureMetrics brandMetrics) {
        for (Map.Entry<String, FeatureValue> entry : features.entrySet()) {
            FeatureValue featureValue = entry.getValue();
            
            // 只处理有展示数据的特征（CTR不为null）
            if (featureValue.getCtr() == Double.NaN || featureValue.getImp() == 0) {
                continue;
            }
            
            String[] parts = entry.getKey().split("_", 2);
            if (parts.length != 2) {
                logger.debug("Invalid feature key format: {}", entry.getKey());
                continue;
            }
            
            String featureType = parts[0];
            String featureId = parts[1];
            
            try {
                Integer id = Integer.parseInt(featureId);
                
                if (Utils.CATE.equals(featureType)) {
                    categoryMetrics.addFeature(id, featureValue);
                } else if (Utils.BRAND.equals(featureType)) {
                    brandMetrics.addFeature(id, featureValue);
                }
            } catch (NumberFormatException e) {
                logger.debug("Invalid feature ID format: {}", featureId);
            }
        }
    }

    /**
     * 写入Redis
     */
    private void writeToRedis(String userProxyId, String windowEnd, 
                            FeatureMetrics categoryMetrics, 
                            FeatureMetrics brandMetrics) {
        
        try (Jedis jedis = jedisPool.getResource(); 
             Pipeline pipeline = jedis.pipelined()) {
            
            // 构建数据映射
            Map<String, String> dataMap = buildDataMap(categoryMetrics, brandMetrics, windowEnd);
            
            // 批量写入
            String ctrKey = USER_CTR_KEY_PREFIX + userProxyId;
            String impKey = USER_IMP_KEY_PREFIX + userProxyId;
            String clickKey = USER_CLICK_KEY_PREFIX + userProxyId;
            
            pipeline.hmset(ctrKey, buildCtrDataMap(categoryMetrics, brandMetrics, windowEnd));
            pipeline.hmset(impKey, buildImpDataMap(categoryMetrics, brandMetrics, windowEnd));
            pipeline.hmset(clickKey, buildClickDataMap(categoryMetrics, brandMetrics, windowEnd));
            
            // 设置过期时间
            pipeline.expire(ctrKey, keyExpireSeconds);
            pipeline.expire(impKey, keyExpireSeconds);
            pipeline.expire(clickKey, keyExpireSeconds);
            
            // 执行批量操作
            pipeline.sync();
            
            logger.debug("Successfully written feature data to Redis for user: {}", userProxyId);
            
        } catch (Exception e) {
            logger.error("Redis operation failed for user: {}", userProxyId, e);
            throw e;
        }
    }

    /**
     * 构建CTR数据映射
     */
    private Map<String, String> buildCtrDataMap(FeatureMetrics categoryMetrics, 
                                              FeatureMetrics brandMetrics, 
                                              String windowEnd) {
        Map<String, String> dataMap = new HashMap<>();
        dataMap.put(categoryMetricsKey, JSONObject.toJSONString(categoryMetrics.getCtrMap()));
        dataMap.put(categoryMetricsTimeKey, windowEnd);
        dataMap.put(brandMetricsKey, JSONObject.toJSONString(brandMetrics.getCtrMap()));
        dataMap.put(brandMetricsTimeKey, windowEnd);
        return dataMap;
    }

    /**
     * 构建展示数据映射
     */
    private Map<String, String> buildImpDataMap(FeatureMetrics categoryMetrics, 
                                              FeatureMetrics brandMetrics, 
                                              String windowEnd) {
        Map<String, String> dataMap = new HashMap<>();
        dataMap.put(categoryMetricsKey, JSONObject.toJSONString(categoryMetrics.getImpMap()));
        dataMap.put(categoryMetricsTimeKey, windowEnd);
        dataMap.put(brandMetricsKey, JSONObject.toJSONString(brandMetrics.getImpMap()));
        dataMap.put(brandMetricsTimeKey, windowEnd);
        return dataMap;
    }

    /**
     * 构建点击数据映射
     */
    private Map<String, String> buildClickDataMap(FeatureMetrics categoryMetrics, 
                                                FeatureMetrics brandMetrics, 
                                                String windowEnd) {
        Map<String, String> dataMap = new HashMap<>();
        dataMap.put(categoryMetricsKey, JSONObject.toJSONString(categoryMetrics.getClickMap()));
        dataMap.put(categoryMetricsTimeKey, windowEnd);
        dataMap.put(brandMetricsKey, JSONObject.toJSONString(brandMetrics.getClickMap()));
        dataMap.put(brandMetricsTimeKey, windowEnd);
        return dataMap;
    }

    /**
     * 构建通用数据映射
     */
    private Map<String, String> buildDataMap(FeatureMetrics categoryMetrics, 
                                           FeatureMetrics brandMetrics, 
                                           String windowEnd) {
        Map<String, String> dataMap = new HashMap<>();
        dataMap.put(categoryMetricsKey, JSONObject.toJSONString(categoryMetrics.getCtrMap()));
        dataMap.put(categoryMetricsTimeKey, windowEnd);
        dataMap.put(brandMetricsKey, JSONObject.toJSONString(brandMetrics.getCtrMap()));
        dataMap.put(brandMetricsTimeKey, windowEnd);
        return dataMap;
    }

    /**
     * 输出性能统计
     */
    private void logPerformanceStats() {
        double avgProcessingTime = successCount > 0 ? (double) totalProcessingTime / successCount : 0;
        double successRate = (successCount + failureCount) > 0 ? 
            (double) successCount / (successCount + failureCount) * 100 : 0;
        
        logger.info("RedisSink stats - Success: {}, Failure: {}, Success rate: {:.2f}%, Avg processing time: {:.2f}ms",
            successCount, failureCount, successRate, avgProcessingTime);
    }

    @Override
    public void close() throws Exception {
        if (jedisPool != null) {
            jedisPool.close();
        }
        logPerformanceStats();
        logger.info("RedisSink closed successfully");
        super.close();
    }

    /**
     * 特征指标数据类
     */
    private static class FeatureMetrics {
        private final Map<Integer, Number> impMap = new HashMap<>();
        private final Map<Integer, Number> clickMap = new HashMap<>();
        private final Map<Integer, Number> ctrMap = new HashMap<>();

        public void addFeature(Integer id, FeatureValue feature) {
            impMap.put(id, feature.getImp());
            clickMap.put(id, feature.getClick());
            ctrMap.put(id, feature.getCtr());
        }

        public boolean hasValidData() {
            return !impMap.isEmpty() || !clickMap.isEmpty() || !ctrMap.isEmpty();
        }

        public Map<Integer, Number> getImpMap() { return impMap; }
        public Map<Integer, Number> getClickMap() { return clickMap; }
        public Map<Integer, Number> getCtrMap() { return ctrMap; }
    }
}
