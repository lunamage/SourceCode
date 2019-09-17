package recmmend.artRead;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;
import utils.ReadConfig;


public class ArtReadFeatureRedisSink extends RichSinkFunction<Tuple2<Boolean, Row>> {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -5733091892833302958L;
	private static Logger log = LoggerFactory.getLogger(ArtReadFeatureRedisSink.class);
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
    public void invoke(Tuple2<Boolean, Row> value, SinkFunction.Context context) {
        String key = (String) value.f1.getField(0);
        Double time = Double.valueOf((Integer) value.f1.getField(1))/10000;
        Double finish = (Double) value.f1.getField(2)/10000;
        Double count = Double.valueOf((Long) value.f1.getField(3))/10000;
      
        try (ShardedJedis jedis = jedisPool.getResource()) {
        	jedis.hincrByFloat("artread_"+key, "time", time);
        	jedis.hincrByFloat("artread_"+key, "finish", finish);
        	jedis.hincrByFloat("artread_"+key, "count", count);
        	jedis.expire("artread_"+key, 86400*4);
    	}catch (Exception e) {
            log.error("hset redis error key is {} msg is {} ", key, e.getMessage());
        }
    }
}
