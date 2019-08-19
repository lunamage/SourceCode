package search.query;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author: zyl
 * @date: 2019/6/26
 * @time: 3:11 PM
 * @Description:异步mysql
 */
public class QueryRealtimeAsyncMysqlJDBC extends RichAsyncFunction<Tuple4<String, String, String, Long>, Tuple4<String, String, String, Long>> {
    
	/**
	 * 
	 */
	private static final long serialVersionUID = -4787197776575925778L;
	private static Logger log = LoggerFactory.getLogger(QueryRealtimeAsyncMysqlJDBC.class);
    private Map<String,String> memoryDB = new ConcurrentHashMap<>();
    private Connection conn=null;
    


    @Override
    public void asyncInvoke(Tuple4<String, String, String, Long> input,ResultFuture<Tuple4<String, String, String, Long>> resultFuture) throws Exception {
    	
    	 CompletableFuture.supplyAsync(() -> {
             Tuple4<String, String, String, Long> result = null;
             Connection conn = null;
             String query = input.f0;
         	 String channelId = input.f1;
         	 String articleId = input.f2;
         	 Long timestamp = input.f3;
         	
         	 
             try {
            	//base64
             	 String queryBase64 = Base64.getEncoder().encodeToString(query.getBytes("UTF-8"));
             	 
            	 String[] configure= ReadConfig.getProperties("db.sf").split("\\|");
                 // query db
                 Class.forName(configure[1]);
                 conn = DriverManager.getConnection(configure[0],configure[2],configure[3]);
                 PreparedStatement pstmt = conn.prepareStatement("SELECT id from dim_query where query= '" + queryBase64 + "';");
                 ResultSet rs = pstmt.executeQuery();
                 rs.next();
                 String id = String.valueOf(rs.getInt("id"));
                 log.info("smzdm"+" "+id+" "+channelId+" "+articleId+" "+timestamp);
                 result = Tuple4.of(id,channelId,articleId,timestamp);
             } catch (Exception e) {
                 e.printStackTrace();
             } finally {
                 if (conn != null) {
                     try {
                         conn.close();
                     } catch (SQLException e) {
                         e.printStackTrace();
                     }
                 }
             }
             return result;
         }).whenComplete((v, e) -> {
             if (e == null && v != null) {
                 resultFuture.complete(Collections.singleton(v));
             } else if (e != null) {
                 resultFuture.completeExceptionally(e);
             } else {
                 resultFuture.completeExceptionally(new Exception("query fail,no data return....."));
             }
         });
    	
    }

}
