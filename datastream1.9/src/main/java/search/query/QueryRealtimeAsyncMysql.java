package search.query;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLConnection;

import org.apache.flink.api.java.tuple.Tuple14;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
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
 * @Description:异步mysql
 */
public class QueryRealtimeAsyncMysql extends RichAsyncFunction<Tuple4<String, String, String, Long>, Tuple4<String, String, String, Long>> {
    
	/**
	 * 
	 */
	private static final long serialVersionUID = -4787197776575925778L;
	private static Logger log = LoggerFactory.getLogger(QueryRealtimeAsyncMysql.class);
	private transient SQLClient mySQLClient;
    private Map<String,String> memoryDB = new ConcurrentHashMap<>();
    

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        JsonObject mySQLClientConfig = new JsonObject();
        //读配置
        String[] configure= ReadConfig.getProperties("db.sf").split("\\|");
        mySQLClientConfig.put("url", configure[0])
                .put("driver_class", configure[1])
                .put("max_pool_size", 50)
                .put("user", configure[2])
//                .put("max_idle_time",1000)
                .put("password", configure[3]);

        VertxOptions vo = new VertxOptions();
        vo.setEventLoopPoolSize(16);
        vo.setWorkerPoolSize(50);

        Vertx vertx = Vertx.vertx(vo);
        mySQLClient = JDBCClient.createNonShared(vertx, mySQLClientConfig);
        
        //3小时执行一次
        Timer timer = new Timer();
        timer.schedule(new TimerTaskClearMap(), 1000, 3000 * 60 * 60);

    }

    @Override
    public void close() throws Exception {
        super.close();
        if (mySQLClient != null)
            mySQLClient.close();
    }

    @Override
    public void asyncInvoke(Tuple4<String, String, String, Long> input,ResultFuture<Tuple4<String, String, String, Long>> resultFuture) throws Exception {
    	
    	String query = input.f0;
    	String channelId = input.f1;
    	String articleId = input.f2;
    	Long timestamp = input.f3;
    	
    	//base64
		String queryBase64 = Base64.getEncoder().encodeToString(query.getBytes("UTF-8"));
		
        String queryId=memoryDB.get(queryBase64);
        if(queryId != null) {
        	 log.info("smzdmME"+queryId+" "+channelId+" "+articleId);
        	 resultFuture.complete(Collections.singleton(new Tuple4<>(queryId,channelId,articleId,timestamp)));
             return;
        }
        		
        mySQLClient.getConnection(conn -> {
            if (conn.failed()) {
                //Treatment failures
                resultFuture.completeExceptionally(conn.cause());
                return;
            }

            final SQLConnection connection = conn.result();
			//结合自己的查询逻辑，拼凑出相应的sql，然后返回结果。
            String querySql = "SELECT id "
            		+ "FROM dim_query "
            		+ "where query = '" + queryBase64 + "'";
            connection.query(querySql, res2 -> {
                if (res2.failed()) {
                    return;
                }

                if (res2.succeeded()) {
                    ResultSet rs = res2.result();
                    List<JsonObject> rows = rs.getRows();
                    if (rows.size() <= 0) {
                        return;
                    }
                    for (JsonObject json : rows) {
                        String id = String.valueOf(json.getInteger("id"));
                        memoryDB.put(queryBase64, id);
                        //log
                        log.info("smzdmDB"+id+" "+channelId+" "+articleId);
                        resultFuture.complete(Collections.singleton(new Tuple4<>(id,channelId,articleId,timestamp)));
                    }
                    // Do something with results
                } else {
                	return;
                }
            });

            connection.close(done -> {
                if (done.failed()) {
                    throw new RuntimeException(done.cause());
                }
            });

        });
    }

    class TimerTaskClearMap extends TimerTask {
        // 清空缓存的数据
        @Override
        public void run() {
            log.info("delete catesMap size {} , time {} ", memoryDB.size(), DateUtils.formatDate(new Date(), DateUtils.YYYYMMDD_HMS));
            memoryDB.clear();
        }
    }


}
