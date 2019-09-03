package recommend.deviceType;

import org.apache.flink.api.java.tuple.Tuple3;
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


public class UserDeviceFeatureRedisSink extends RichSinkFunction<Tuple3 <String, String, Long>> {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -5733091892833302958L;
	private static Logger log = LoggerFactory.getLogger(UserDeviceFeatureRedisSink.class);
	private JedisPool jedisPool;
	
    @Override
    public void open(Configuration parameters) {
    	 // 创建jedis池配置实例
        JedisPoolConfig config = new JedisPoolConfig();
        // #jedis的最大分配对象#
        config.setMaxTotal(Integer.valueOf(ReadConfig.getProperties("jedis.pool.maxActive")));
        // #jedis最大保存idel状态对象数 #
        config.setMaxIdle(Integer.valueOf(ReadConfig.getProperties("jedis.pool.maxIdle")));

        this.jedisPool = new JedisPool(config, ReadConfig.getProperties("redis.user.address"),
                Integer.valueOf(ReadConfig.getProperties("redis.port")),
                Integer.valueOf(ReadConfig.getProperties("jedis.pool.timeout")));
    }

    @SuppressWarnings("rawtypes")
	@Override
    public void invoke(Tuple3 <String, String, Long> value, SinkFunction.Context context) {
        String key = value.f0;
        
        try (Jedis jedis = jedisPool.getResource(); Pipeline line = jedis.pipelined()) {
        	line.hset("u_"+key, "device_typeid", value.f1);
        	line.sync();
    	}catch (Exception e) {
            log.error("hset redis error key is {} msg is {} ", key, e.getMessage());
        }
    }
}
