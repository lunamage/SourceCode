package sdk.druid;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLConnection;

import org.apache.flink.api.java.tuple.Tuple14;
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
public class AsyncMysql extends RichAsyncFunction<Tuple14<String, String, String, String, String, String, String, String, String, String, String, Integer, Integer, Integer>, String> {
    
	/**
	 * 
	 */
	private static final long serialVersionUID = -4787197776575925778L;
	private static Logger log = LoggerFactory.getLogger(AsyncMysql.class);
	private transient SQLClient mySQLClient;
    private Map<String, ArrayList<String>> memoryDB = new ConcurrentHashMap<>();
    

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        JsonObject mySQLClientConfig = new JsonObject();
        //读配置
        String[] configure= ReadConfig.getProperties("db.app").split("\\|");
        mySQLClientConfig.put("url", configure[0])
                .put("driver_class", configure[1])
                .put("max_pool_size", 50)
                .put("user", configure[2])
//                .put("max_idle_time",1000)
                .put("password", configure[3]);

        VertxOptions vo = new VertxOptions();
        vo.setEventLoopPoolSize(10);
        vo.setWorkerPoolSize(50);

        Vertx vertx = Vertx.vertx(vo);
        mySQLClient = JDBCClient.createNonShared(vertx, mySQLClientConfig);
        
        //一小时执行一次
        Timer timer = new Timer();
        timer.schedule(new TimerTaskClearMap(), 1000, 1000 * 60 * 60);

    }

    @Override
    public void close() throws Exception {
        super.close();
        if (mySQLClient != null)
            mySQLClient.close();
    }

    @Override
    public void asyncInvoke(
			Tuple14<String, String, String, String, String, String, String, String, String, String, String, Integer, Integer, Integer> input,
			ResultFuture<String> resultFuture) throws Exception {
    	
    	String stat_dt = input.f0;
    	String dt = input.f1;
    	String sid = input.f2;
    	String uid = input.f3;
    	String av = input.f4;
    	String did = input.f5;
    	String isnp = input.f6;
    	String lng = input.f7;
    	String lat = input.f8;
    	String tv = input.f9;
        String articleid = input.f10;
        Integer exposure = input.f11;
        Integer click = input.f12;
        Integer event = input.f13;
        
        String tmp = "{\"stat_dt\":\"" + stat_dt +
        			 "\",\"dt\":\"" + dt +
        			 "\",\"sid\":\"" + sid +
        			 "\",\"uid\":\"" + uid +
        			 "\",\"av\":\"" + av +
        			 "\",\"did\":\"" + did +
        			 "\",\"isnp\":\"" + isnp +
        			 "\",\"lng\":\"" + lng +
        			 "\",\"lat\":\"" + lat +
        			 "\",\"tv\":\"" + tv +
        			 "\",\"articleid\":\"" + articleid +
        			 "\",\"exposure\":" + exposure.toString() +
        			 ",\"click\":" + click.toString() +
        			 ",\"event\":" + event.toString() +",";
        String failtext = tmp + "\"channel\":\"\","+
        		      "\"channel_item\":\"\","+
        		      "\"mall_type\":\"\","+
        		      "\"mall\":\"\","+
        		      "\"brand\":\"\","+
        		      "\"cate_level1\":\"\","+
        		      "\"cate_level2\":\"\","+
        		      "\"cate_level3\":\"\","+
        		      "\"cate_level4\":\"\","+
        		      "\"pubdate\":\"\","+
        		      "\"digital_price\":\"\","+
        		      "\"tags_name\":\"\","+
        		      "\"worthy\":\"\","+
        		      "\"unworthy\":\"\","+
        		      "\"comment_count\":\"\","+
        		      "\"collection_count\":\"\","+
        		      "\"reward_count\":\"\"}";
        
        List<String> dimList=memoryDB.get(articleid);
        if(dimList != null) {
        	String channel = dimList.get(0);
            String channel_item = dimList.get(1);
            String mall_type = dimList.get(2);
            String mall = dimList.get(3);
            String brand = dimList.get(4);
            String cate_level1 = dimList.get(5);
            String cate_level2 = dimList.get(6);
            String cate_level3 = dimList.get(7);
            String cate_level4 = dimList.get(8);
            String pubdate = dimList.get(9);
            String digital_price = dimList.get(10);
            String tags_name = dimList.get(11);
            String worthy = dimList.get(12);
            String unworthy = dimList.get(13);
            String comment_count = dimList.get(14);
            String collection_count = dimList.get(15);
            String reward_count = dimList.get(16);
        	
            String successtext = tmp + "\"channel\":\""+ channel + "\","
                    + "\"channel_item\":\""+ channel_item + "\","
                    + "\"mall_type\":\""+ mall_type + "\","
                    + "\"mall\":\""+ mall + "\","
                    + "\"brand\":\""+ brand + "\","
                    + "\"cate_level1\":\""+ cate_level1 + "\","
                    + "\"cate_level2\":\""+ cate_level2 + "\","
                    + "\"cate_level3\":\""+ cate_level3 + "\","
                    + "\"cate_level4\":\""+ cate_level4 + "\","
                    + "\"pubdate\":\""+ pubdate + "\","
                    + "\"digital_price\":\""+ digital_price + "\","
                    + "\"tags_name\":\""+ tags_name + "\","
                    + "\"worthy\":\""+ worthy + "\","
                    + "\"unworthy\":\""+ unworthy + "\","
                    + "\"comment_count\":\""+ comment_count + "\","
                    + "\"collection_count\":\""+ collection_count + "\","
                    + "\"reward_count\":\""+ reward_count + "\"}";

             resultFuture.complete(Collections.singleton(successtext));
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
            String querySql = "SELECT channel,channel_item,mall_type,mall,brand,cate_level1,cate_level2,cate_level3,cate_level4,pubdate,digital_price,tags_name,worthy,unworthy,comment_count,collection_count,reward_count "
            		+ "FROM dim_article "
            		+ "where articleid = '" + articleid + "'";
            connection.query(querySql, res2 -> {
                if (res2.failed()) {
                    resultFuture.complete(Collections.singleton(failtext));
                    return;
                }

                if (res2.succeeded()) {
                    ResultSet rs = res2.result();
                    List<JsonObject> rows = rs.getRows();
                    if (rows.size() <= 0) {
                    	resultFuture.complete(Collections.singleton(failtext));
                        return;
                    }
                    for (JsonObject json : rows) {
                        String channel = json.getString("channel");
                        String channel_item = json.getString("channel_item");
                        String mall_type = json.getString("mall_type");
                        String mall = json.getString("mall");
                        String brand = json.getString("brand");
                        String cate_level1 = json.getString("cate_level1");
                        String cate_level2 = json.getString("cate_level2");
                        String cate_level3 = json.getString("cate_level3");
                        String cate_level4 = json.getString("cate_level4");
                        String pubdate = json.getString("pubdate");
                        String digital_price = json.getString("digital_price");
                        String tags_name = json.getString("tags_name");
                        String worthy = json.getString("worthy");
                        String unworthy = json.getString("unworthy");
                        String comment_count = json.getString("comment_count");
                        String collection_count = json.getString("collection_count");
                        String reward_count = json.getString("reward_count");
                        
                        String successtext = tmp + "\"channel\":\""+ channel + "\","
                                                 + "\"channel_item\":\""+ channel_item + "\","
                                                 + "\"mall_type\":\""+ mall_type + "\","
                                                 + "\"mall\":\""+ mall + "\","
                                                 + "\"brand\":\""+ brand + "\","
                                                 + "\"cate_level1\":\""+ cate_level1 + "\","
                                                 + "\"cate_level2\":\""+ cate_level2 + "\","
                                                 + "\"cate_level3\":\""+ cate_level3 + "\","
                                                 + "\"cate_level4\":\""+ cate_level4 + "\","
                                                 + "\"pubdate\":\""+ pubdate + "\","
                                                 + "\"digital_price\":\""+ digital_price + "\","
                                                 + "\"tags_name\":\""+ tags_name + "\","
                                                 + "\"worthy\":\""+ worthy + "\","
                                                 + "\"unworthy\":\""+ unworthy + "\","
                                                 + "\"comment_count\":\""+ comment_count + "\","
                                                 + "\"collection_count\":\""+ collection_count + "\","
                                                 + "\"reward_count\":\""+ reward_count + "\"}";
                        //缓存数据
                        ArrayList<String> memoryAdd=new ArrayList<String>();
                        memoryAdd.add(channel);
                        memoryAdd.add(channel_item);
                        memoryAdd.add(mall_type);
                        memoryAdd.add(mall);
                        memoryAdd.add(brand);
                        memoryAdd.add(cate_level1);
                        memoryAdd.add(cate_level2);
                        memoryAdd.add(cate_level3);
                        memoryAdd.add(cate_level4);
                        memoryAdd.add(pubdate);
                        memoryAdd.add(digital_price);
                        memoryAdd.add(tags_name);
                        memoryAdd.add(worthy);
                        memoryAdd.add(unworthy);
                        memoryAdd.add(comment_count);
                        memoryAdd.add(collection_count);
                        memoryAdd.add(reward_count);
                        memoryDB.put(articleid, memoryAdd);
                        
                        resultFuture.complete(Collections.singleton(successtext));
                    }
                    // Do something with results
                } else {
                    resultFuture.complete(null);
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
