package zyl;

import com.alibaba.fastjson.JSONObject;
import com.zdm.zyl.FlinkStreamTest;
import com.zdm.zyl.ReadConfig;
import com.zdm.zyl.Utils;

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

    //private final static String USER_IMP_KEY = "u1dimp_";
    //private final static String USER_CLICK_KEY = "u1dclick_";

    private Integer expire;

    @Override
    public void open(Configuration parameters) {
        // 创建jedis池配置实例
        JedisPoolConfig config = new JedisPoolConfig();
        // #jedis的最大分配对象#
        config.setMaxTotal(Integer.valueOf(ReadConfig.getProperties("jedis.pool.maxActive")));
        // #jedis最大保存idel状态对象数 #
        config.setMaxIdle(Integer.valueOf(ReadConfig.getProperties("jedis.pool.maxIdle")));

        this.jedisPool = new JedisPool(config, ReadConfig.getProperties("redis.article.feature"),
                Integer.valueOf(ReadConfig.getProperties("redis.port")),
                Integer.valueOf(ReadConfig.getProperties("jedis.pool.timeout")));

        expire = Integer.valueOf(ReadConfig.getProperties("jedis.hash.expire"));
    }

    @Override
    public void invoke(FlinkStreamTest.ItemFeatureEntity value, SinkFunction.Context context) {
        String userProxyId = value.getUserProxyId();
        
        //log.info("dubbb");
        
        try (Jedis jedis = jedisPool.getResource(); Pipeline line = jedis.pipelined()) {
        
        for (Map.Entry<String, Object[]> entry : value.getVal().entrySet()) {
            Object[] vals = entry.getValue();
            String propertyItemId = entry.getKey();
            // 格式应该为 品类/品牌+'_'+文章id
            String[] pis = propertyItemId.split("_");
            if (pis.length != 2) {
                continue;
            }
            
            //log.info("dubbb"+pis[0]+"|"+ pis[1]+"|"+vals[0]+"|"+vals[1]);
 
        	//曝光 cate
        	if (Objects.equals(Utils.CATE, pis[0]) && (Integer) vals[0] != 0) {
        		line.hincrBy("u1dcateimp_" + userProxyId, pis[1], (Integer) vals[0]);
        		line.expire("u1dcateimp_" + userProxyId, expire);
        	}
        	//点击 cate
        	if (Objects.equals(Utils.CATE, pis[0]) && (Integer) vals[1] != 0) {
        		line.hincrBy("u1dcateclick_" + userProxyId, pis[1], (Integer) vals[1]);
        		line.expire("u1dcateclick_" + userProxyId, expire);
        	}
        	
        	//曝光 brand
        	if (Objects.equals(Utils.BRAND, pis[0]) && (Integer) vals[0] != 0) {
        		line.hincrBy("u1dbrandimp_" + userProxyId, pis[1], (Integer) vals[0]);
        		line.expire("u1dbrandimp_" + userProxyId, expire);
        	}
        	//点击  brand
        	if (Objects.equals(Utils.BRAND, pis[0]) && (Integer) vals[1] != 0) {
        		line.hincrBy("u1dbrandclick_" + userProxyId, pis[1], (Integer) vals[1]);
        		line.expire("u1dbrandclick_" + userProxyId, expire);
        	}
        	
            line.sync();
        }}
        catch (Exception e) {
            log.error("hincrBy redis error proxyId is {} msg is {} ", value.getUserProxyId(), e.getMessage(),e);
            return;
        }
        
    }
}
