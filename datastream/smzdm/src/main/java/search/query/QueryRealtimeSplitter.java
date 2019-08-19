package search.query;

import com.alibaba.fastjson.JSONObject;


import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple14;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utils.DateUtils;
import utils.ReadConfig;
import utils.Utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/**
 * @author: zyl
 * @date: 2019/6/26
 * @time: 3:11 PM
 * @Description:
 */
public class QueryRealtimeSplitter extends RichFlatMapFunction<String, Tuple5<String, String, Double, String, Long>> {

    /**
	 * 
	 */
	private static final long serialVersionUID = -8284916412921643582L;
	private static Logger log = LoggerFactory.getLogger(QueryRealtimeSplitter.class);
	

    @SuppressWarnings("unchecked")
	@Override
    public void flatMap(String msg, Collector<Tuple5<String, String, Double, String, Long>> collector) {
    	
    	/*曝光：ec='搜索' and ea='点击'*/
		try {
			if(!isJson(msg)) {return;}
			Map<String,String> sdk = JSONObject.parseObject(msg, Map.class);
			if(!isJson(sdk.get("ecp"))) {return;}
			Map<String,String> ecp = JSONObject.parseObject(sdk.get("ecp"), Map.class);
			if(ecp == null) {return;}
			//解析json
			String ec = sdk.get("ec");
			String slt = String.valueOf(sdk.get("slt"));
			Long timestamp = slt == null || Objects.equals(slt, "0") ? System.currentTimeMillis() : Long.valueOf(slt) * 1000;
			
			if(ec.equals("搜索")) {
				String articleId = ecp.get("4");
				String channelId = ecp.get("28");
				String query = ecp.get("46");
				Double positionValue = Utils.getQueryPositionValue(ecp.get("p")) == null ? 28.57 : Utils.getQueryPositionValue(ecp.get("p"));
				//异常值处理
				if(articleId == null) {return;}
				if(channelId == null || !channelId.matches("^1|2|5|21$")) {return;}
				if(query == null) {return;}
				String queryBase64 = Base64.getEncoder().encodeToString(query.getBytes("UTF-8"));
				collector.collect(new Tuple5<>(queryBase64,channelId+"_"+articleId,positionValue,"click",timestamp));
			}
			
			if(ec.equals("04")) {
				String articleId = ecp.get("a");
				String channelId = ecp.get("c");
				String query = ecp.get("qu");
				Double positionValue = Utils.getQueryPositionValue(ecp.get("p")) == null ? 28.57 : Utils.getQueryPositionValue(ecp.get("p"));
				
				String spp = ecp.get("spp");
				String srm = ecp.get("srm");
				String cd37 = ecp.get("37");
				
				//异常值处理
				if(articleId == null) {return;}
				if(channelId == null || !channelId.matches("^1|2|5|21$")) {return;}
				if(spp == null || !spp.equals("综合")) {return;}
				if(srm == null || !srm.equals("通用_0")) {return;}
				if(cd37 == null || !Pattern.compile("内容:无_分类:无_商城:无_品牌:无_价格:无~无").matcher(cd37).find() || !Pattern.compile("综合排序|最新排序").matcher(cd37).find()) {return;}
				
				
				if(query == null) {return;}
				String queryBase64 = Base64.getEncoder().encodeToString(query.getBytes("UTF-8"));
				collector.collect(new Tuple5<>(queryBase64,channelId+"_"+articleId,positionValue,"imp",timestamp));
			}

        	} catch (Exception e) {
        		log.error("flatMap error msg is {} value is {}", e.getMessage(), msg,e);
        		return;
        	}
    }
    
    public boolean isJson(String content){
  	  try {
  		  JSONObject.parseObject(content);
  		  return true;
  	  	} catch (Exception e) {
  	  		return false;
  	  	}
  	}
}
