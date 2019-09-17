package zyl;

import com.alibaba.fastjson.JSONObject;
import com.zdm.zyl.Utils;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.shaded.guava18.com.google.common.base.Strings;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Pattern;

/**
 * @author: liuchen
 * @date: 2019/1/14
 * @time: 3:11 PM
 * @Description:
 */
public class MessageSplitter extends RichFlatMapFunction<String, Tuple4<String, String, String, Long>> {

    private static Logger log = LoggerFactory.getLogger(MessageSplitter.class);

    @Override
    public void flatMap(String msg, Collector<Tuple4<String, String, String, Long>> collector) {
        try {
        	if(!isJson(msg)) {return;}
			Map<String,String> sdk = JSONObject.parseObject(msg, Map.class);
			if(!isJson(sdk.get("ecp"))) {return;}
			Map<String,String> ecp = JSONObject.parseObject(sdk.get("ecp"), Map.class);
			if(ecp == null) {return;}
			//解析json
			String type = sdk.get("type");
			String event = sdk.get("event");
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
			
			String userProxyId = Strings.isNullOrEmpty(uid) || Objects.equals(uid, "0") ? getDeviceid(imei, did) : uid;
			
			Long timestamp = slt == null || Objects.equals(slt, "0") ? System.currentTimeMillis() : Long.valueOf(slt) * 1000;
			
			String regex = "^[1-9]{1}[0-9]*$";
			Pattern p = Pattern.compile(regex);
			
			// 曝光数据
			if (Objects.equals("show", event) && (Objects.equals("tj", type) || Objects.equals("hj", type))) {
				if(p.matcher(level1).find()) {
					collector.collect(new Tuple4<>(userProxyId, Utils.CATE + "_" + level1, Utils.IMP, timestamp));
					if(!Strings.isNullOrEmpty(anid)) {
						collector.collect(new Tuple4<>(anid, Utils.CATE + "_" + level1, Utils.IMP, timestamp));
					}
				}
				
				if(p.matcher(level2).find()) {
					collector.collect(new Tuple4<>(userProxyId, Utils.CATE + "_" + level2, Utils.IMP, timestamp));
					if(!Strings.isNullOrEmpty(anid)) {
						collector.collect(new Tuple4<>(anid, Utils.CATE + "_" + level2, Utils.IMP, timestamp));
					}
				}
				
				if(p.matcher(level3).find()) {
					collector.collect(new Tuple4<>(userProxyId, Utils.CATE + "_" + level3, Utils.IMP, timestamp));
					if(!Strings.isNullOrEmpty(anid)) {
						collector.collect(new Tuple4<>(anid, Utils.CATE + "_" + level3, Utils.IMP, timestamp));
					}
				}
				
				if(p.matcher(level4).find()) {
					collector.collect(new Tuple4<>(userProxyId, Utils.CATE + "_" + level4, Utils.IMP, timestamp));
					if(!Strings.isNullOrEmpty(anid)) {
						collector.collect(new Tuple4<>(anid, Utils.CATE + "_" + level4, Utils.IMP, timestamp));
					}
				}
				
				if(p.matcher(brand).find()) {
					collector.collect(new Tuple4<>(userProxyId, Utils.BRAND + "_" + brand, Utils.IMP, timestamp));
					if(!Strings.isNullOrEmpty(anid)) {
						collector.collect(new Tuple4<>(anid, Utils.BRAND + "_" + brand, Utils.IMP, timestamp));
					}
				}
			}
			
			// 点击数据
			if (Objects.equals("click", event) && (Objects.equals("tj", type) || Objects.equals("hj", type))) {
				if(p.matcher(level1).find()) {
					collector.collect(new Tuple4<>(userProxyId, Utils.CATE + "_" + level1, Utils.CLICK, timestamp));
					if(!Strings.isNullOrEmpty(anid)) {
						collector.collect(new Tuple4<>(anid, Utils.CATE + "_" + level1, Utils.CLICK, timestamp));
					}
				}
				
				if(p.matcher(level2).find()) {
					collector.collect(new Tuple4<>(userProxyId, Utils.CATE + "_" + level2, Utils.CLICK, timestamp));
					if(!Strings.isNullOrEmpty(anid)) {
						collector.collect(new Tuple4<>(anid, Utils.CATE + "_" + level2, Utils.CLICK, timestamp));
					}
				}
				
				if(p.matcher(level3).find()) {
					collector.collect(new Tuple4<>(userProxyId, Utils.CATE + "_" + level3, Utils.CLICK, timestamp));
					if(!Strings.isNullOrEmpty(anid)) {
						collector.collect(new Tuple4<>(anid, Utils.CATE + "_" + level3, Utils.CLICK, timestamp));
					}
				}
				
				if(p.matcher(level4).find()) {
					collector.collect(new Tuple4<>(userProxyId, Utils.CATE + "_" + level4, Utils.CLICK, timestamp));
					if(!Strings.isNullOrEmpty(anid)) {
						collector.collect(new Tuple4<>(anid, Utils.CATE + "_" + level4, Utils.CLICK, timestamp));
					}
				}
				
				if(p.matcher(brand).find()) {
					collector.collect(new Tuple4<>(userProxyId, Utils.BRAND + "_" + brand, Utils.CLICK, timestamp));
					if(!Strings.isNullOrEmpty(anid)) {
						collector.collect(new Tuple4<>(anid, Utils.BRAND + "_" + brand, Utils.CLICK, timestamp));
					}
				}
				
			}
            
            //搜索点击数据
			if (Objects.equals("click", event) && Objects.equals("ss", type)) {
				if(p.matcher(level1).find()) {
					collector.collect(new Tuple4<>(userProxyId, Utils.CATE + "_" + level1, "s_click", timestamp));
					if(!Strings.isNullOrEmpty(anid)) {
						collector.collect(new Tuple4<>(anid, Utils.CATE + "_" + level1, "s_click", timestamp));
					}
				}
				
				if(p.matcher(level2).find()) {
					collector.collect(new Tuple4<>(userProxyId, Utils.CATE + "_" + level2, "s_click", timestamp));
					if(!Strings.isNullOrEmpty(anid)) {
						collector.collect(new Tuple4<>(anid, Utils.CATE + "_" + level2, "s_click", timestamp));
					}
				}
				
				if(p.matcher(level3).find()) {
					collector.collect(new Tuple4<>(userProxyId, Utils.CATE + "_" + level3, "s_click", timestamp));
					if(!Strings.isNullOrEmpty(anid)) {
						collector.collect(new Tuple4<>(anid, Utils.CATE + "_" + level3, "s_click", timestamp));
					}
				}
				
				if(p.matcher(level4).find()) {
					collector.collect(new Tuple4<>(userProxyId, Utils.CATE + "_" + level4, "s_click", timestamp));
					if(!Strings.isNullOrEmpty(anid)) {
						collector.collect(new Tuple4<>(anid, Utils.CATE + "_" + level4, "s_click", timestamp));
					}
				}
				
				if(p.matcher(brand).find()) {
					collector.collect(new Tuple4<>(userProxyId, Utils.BRAND + "_" + brand, "s_click", timestamp));
					if(!Strings.isNullOrEmpty(anid)) {
						collector.collect(new Tuple4<>(anid, Utils.BRAND + "_" + brand, "s_click", timestamp));
					}
				}
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
