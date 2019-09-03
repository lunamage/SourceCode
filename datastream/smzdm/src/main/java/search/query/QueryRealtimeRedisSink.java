package search.query;

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

public class QueryRealtimeRedisSink extends RichSinkFunction<Tuple3<String, String, String>> {
	 private static Logger log = LoggerFactory.getLogger(QueryRealtimeRedisSink.class);

	    private JedisPool jedisPool;

	    @Override
	    public void open(Configuration parameters) {
	        // 创建jedis池配置实例
	        JedisPoolConfig config = new JedisPoolConfig();
	        // #jedis的最大分配对象#
	        config.setMaxTotal(Integer.valueOf(ReadConfig.getProperties("jedis.pool.maxActive")));
	        // #jedis最大保存idel状态对象数 #
	        config.setMaxIdle(Integer.valueOf(ReadConfig.getProperties("jedis.pool.maxIdle")));

	        this.jedisPool = new JedisPool(config, ReadConfig.getProperties("redis.address"),
	                Integer.valueOf(ReadConfig.getProperties("redis.port")),
	                Integer.valueOf(ReadConfig.getProperties("jedis.pool.timeout")));
	    }


	    @Override
	    public void invoke(Tuple3<String, String, String> tuple, SinkFunction.Context context) {
	        String key = tuple.f0;
	        String field = tuple.f1;
	        String value = tuple.f2;
	       	        
	        try (Jedis jedis = jedisPool.getResource(); Pipeline line = jedis.pipelined()) {
	            line.hset(key, field, value);
	            //line.expire("rqac_" + queryIdkey, 86400);
	            line.sync();
	        } catch (Exception e) {
	            log.error("hset redis error queryId is {} msg is {} ", field, e.getMessage());
	        }
	    }
}
