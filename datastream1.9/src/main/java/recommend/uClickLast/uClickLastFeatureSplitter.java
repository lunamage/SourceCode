package recommend.uClickLast;

import com.alibaba.fastjson.JSONObject;


import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple14;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.base.Strings;
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
public class uClickLastFeatureSplitter extends RichFlatMapFunction<String, Tuple8<String, String, String, String, String, String, String, Long>> {

    /**
	 * 
	 */
	private static final long serialVersionUID = -8284916412921643582L;
	private static Logger log = LoggerFactory.getLogger(uClickLastFeatureSplitter.class);
	
    @SuppressWarnings("unchecked")
	@Override
    public void flatMap(String msg, Collector<Tuple8<String, String, String, String, String, String, String, Long>> collector) {
		try {
			if(!isJson(msg)) {return;}
			Map<String,String> sdk = JSONObject.parseObject(msg, Map.class);
			if(!isJson(sdk.get("ecp"))) {return;}
			Map<String,String> ecp = JSONObject.parseObject(sdk.get("ecp"), Map.class);
			if(ecp == null) {return;}
			//解析json
			String slt = String.valueOf(sdk.get("slt"));
			String uid = sdk.get("uid");
			String did = sdk.get("did");
			String imei = sdk.get("imei");
			String anid = sdk.get("anid");
			String level1 = sdk.get("level1") == null ? "" : sdk.get("level1");
			String level2 = sdk.get("level2") == null ? "" : sdk.get("level2");
			String level3 = sdk.get("level3") == null ? "" : sdk.get("level3");
			String level4 = sdk.get("level4") == null ? "" : sdk.get("level4");
			String brand = sdk.get("brand") == null ? "" : sdk.get("brand");
			String mall = sdk.get("mall") == null ? "" : sdk.get("mall");
			
			String userProxyId = Strings.isNullOrEmpty(uid) || Objects.equals(uid, "0") ? getDeviceid(imei, did) : uid;
			Long timestamp = slt == null || Objects.equals(slt, "0") ? System.currentTimeMillis() : Long.valueOf(slt) * 1000;
					
			collector.collect(new Tuple8<>(userProxyId, level1, level2, level3, level4, brand, mall, timestamp));
			if(!Strings.isNullOrEmpty(anid)) {
				collector.collect(new Tuple8<>(anid, level1, level2, level3, level4, brand, mall, timestamp));
			}

        	} catch (Exception e) {
        		log.error("flatMap error msg is {} value is {}", e.getMessage(), msg,e);
        		return;
        	}
    }
    
    private String getDeviceid(String imei, String did) {
        String deviceId = Strings.isNullOrEmpty(imei) ? did : imei;
        deviceId = deviceId.replaceAll(" ", "+");
        return deviceId;
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
