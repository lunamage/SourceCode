package com.smzdm.service;

import com.alibaba.fastjson.JSONObject;
import com.smzdm.FlinkStreamTest;
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
 * @author: liuchen
 * @date: 2019/1/23
 * @time: 6:19 PM
 * @Description:
 */
public class RedisSink extends RichSinkFunction<FlinkStreamTest.ItemFeatureEntity> {

    private static Logger log = LoggerFactory.getLogger(RedisSink.class);

    private JedisPool jedisPool;

    private final Properties properties;

    public RedisSink(Properties properties) {
        this.properties = properties;
    }

    private final static String USER_CTR_KEY = "urhoctr_";
    private final static String USER_IMP_KEY = "urhoimp_";
    private final static String USER_CLICK_KEY = "urhoclick_";

    private String cateM;
    private String cateMt;

    private String brandM;
    private String brandMt;

    private Integer expire;

    @Override
    public void open(Configuration parameters) {
        // 创建jedis池配置实例
        JedisPoolConfig config = new JedisPoolConfig();
        // #jedis的最大分配对象#
        config.setMaxTotal(Integer.valueOf(properties.getProperty("jedis.pool.maxActive")));
        // #jedis最大保存idel状态对象数 #
        config.setMaxIdle(Integer.valueOf(properties.getProperty("jedis.pool.maxIdle")));

        this.jedisPool = new JedisPool(config, properties.getProperty("redis.article.feature"),
                Integer.valueOf(properties.getProperty("redis.port")),
                Integer.valueOf(properties.getProperty("jedis.pool.timeout")));

        cateM = properties.getProperty("jedis.hash.cate.m.key");
        cateMt = properties.getProperty("jedis.hash.cate.mt.key");

        brandM = properties.getProperty("jedis.hash.brand.m.key");
        brandMt = properties.getProperty("jedis.hash.brand.mt.key");

        expire = Integer.valueOf(properties.getProperty("jedis.hash.expire"));
    }


    @Override
    public void invoke(FlinkStreamTest.ItemFeatureEntity value, SinkFunction.Context context) {
        String userProxyId = value.getUserProxyId();
        String windowEnd = value.getWindowEnd();
        Map<Integer, Number> cateImpMap = new HashMap<>();
        Map<Integer, Number> cateClickMap = new HashMap<>();
        Map<Integer, Number> cateCtrMap = new HashMap<>();
        Map<Integer, Number> brandImpMap = new HashMap<>();
        Map<Integer, Number> brandClickMap = new HashMap<>();
        Map<Integer, Number> brandCtrMap = new HashMap<>();
        for (Map.Entry<String, Object[]> entry : value.getVal().entrySet()) {
            Object[] vals = entry.getValue();
            // 无曝光数据，ctr值为null
            if (null == vals[2])
                continue;
            String propertyItemId = entry.getKey();
            // 格式应该为 品类/品牌+'_'+文章id
            String[] pis = propertyItemId.split("_");
            if (pis.length != 2) {
                continue;
            }
            // 品类
            if (Objects.equals(Utils.CATE, pis[0])) {
                cateImpMap.put(Integer.valueOf(pis[1]), (Integer) vals[0]);
                cateClickMap.put(Integer.valueOf(pis[1]), (Integer) vals[1]);
                cateCtrMap.put(Integer.valueOf(pis[1]), (Double) vals[2]);
            }
            // 品牌
            if (Objects.equals(Utils.BRAND, pis[0])) {
                brandImpMap.put(Integer.valueOf(pis[1]), (Integer) vals[0]);
                brandClickMap.put(Integer.valueOf(pis[1]), (Integer) vals[1]);
                brandCtrMap.put(Integer.valueOf(pis[1]), (Double) vals[2]);
            }
        }
        // 无数据不操作。产生原因有可能是只有点击没有曝光
        if ((cateImpMap.size() == 0 && cateClickMap.size() == 0 && cateCtrMap.size() == 0) ||
                (brandImpMap.size() == 0 && brandClickMap.size() == 0 && brandCtrMap.size() == 0))
            return;

        try (Jedis jedis = jedisPool.getResource(); Pipeline line = jedis.pipelined()) {
            // ctr
            line.hmset(USER_CTR_KEY + userProxyId, getClildKey(JSONObject.toJSONString(cateCtrMap), JSONObject.toJSONString(brandCtrMap), windowEnd));
            // 曝光
            line.hmset(USER_IMP_KEY + userProxyId, getClildKey(JSONObject.toJSONString(cateImpMap), JSONObject.toJSONString(brandImpMap), windowEnd));
            // 点击
            line.hmset(USER_CLICK_KEY + userProxyId, getClildKey(JSONObject.toJSONString(cateClickMap), JSONObject.toJSONString(brandClickMap), windowEnd));

            line.expire(USER_CTR_KEY + userProxyId, expire);
            line.expire(USER_IMP_KEY + userProxyId, expire);
            line.expire(USER_CLICK_KEY + userProxyId, expire);
            line.sync();
        } catch (Exception e) {
            log.error("hset redis error proxyId is {} msg is {} ", value.getUserProxyId(), e.getMessage());
        }
    }

    private Map<String, String> getClildKey(String cateStr, String brandStr, String windowEnd) {
        Map<String, String> map = new HashMap<>();
        map.put(cateM, cateStr);
        map.put(cateMt, windowEnd);
        map.put(brandM, brandStr);
        map.put(brandMt, windowEnd);
        return map;
    }
}
