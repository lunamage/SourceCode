package sdk.druid;

import com.alibaba.fastjson.JSONObject;


import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple14;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utils.DateUtils;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;

/**
 * @author: zyl
 * @date: 2019/6/26
 * @time: 3:11 PM
 * @Description:
 */
public class SdkOlapEtlSplitter extends RichFlatMapFunction<String, Tuple14<String, String, String, String, String, String, String, String, String, String, String, Integer, Integer, Integer>> {

    /**
	 * 
	 */
	private static final long serialVersionUID = -8284916412921643582L;
	private static Logger log = LoggerFactory.getLogger(SdkOlapEtlSplitter.class);

    @SuppressWarnings("unchecked")
	@Override
    public void flatMap(String msg, Collector<Tuple14<String, String, String, String, String, String, String, String, String, String, String, Integer, Integer, Integer>> collector) {
    	/*曝光：ec='01' and ea='01' and ecp.sp='0'；abtest：ecp.tv
                                  点击：ec='首页' and ea='首页站内文章点击' and el regexp '^推荐_'；abtest：ecp.13
                                  电商点击：ec='增强型电子商务' and ea='添加到购物车' and ecp.21 regexp '^首页_推荐_feed流_'；abtest：ecp.13*/
		try {
			if(!isJson(msg)) {return;}
			Map<String,String> sdk = JSONObject.parseObject(msg, Map.class);
			if(!isJson(sdk.get("ecp"))) {return;}
			Map<String,String> ecp = JSONObject.parseObject(sdk.get("ecp"), Map.class);
			if(ecp == null) {return;}
			//解析json
			String ec = sdk.get("ec");
			String ea = sdk.get("ea");
			String el = sdk.get("el");
			String sp = ecp.get("sp");
			String tv = ecp.get("tv");
			String cd13 = ecp.get("13");
			String cd21 = ecp.get("21");
			String it = sdk.get("it");
			String dt = sdk.get("dt");
			String sid = sdk.get("sid");
			String uid = sdk.get("uid");
			String av = sdk.get("av");
			String did = sdk.get("did");
			String isnp = sdk.get("isnp");
			String lng = sdk.get("lng");
			String lat = sdk.get("lat");
			
			//异常值处理
			if(ec == null) {return;}
			if(ea == null) {return;}
			if(el == null) {el="";}
			if(sp == null) {sp="";}
			if(tv == null) {tv="";}
			if(cd13 == null) {cd13="";}
			if(cd21 == null) {cd21="";}
			if(dt.equals("1")) {dt="iPhone";} else if(dt.equals("2") || dt.equals("4")){dt="Android";} else {dt="Other";}
			if(sid == null) {sid="";}
			if(uid == null) {uid="";}
			if(av == null) {av="";}
			if(did == null) {did="";}
			if(isnp == null) {isnp="0";}
			if(lng == null) {lng="";}
			if(lat == null) {lat="";}
		
			if(it != null && Pattern.compile("^9.").matcher(av).find()) {
				Date date = new Date(Long.parseLong(it));
				SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				String stat_dt = dateFormat.format(date);
				Long d_min = DateUtils.getCurrentDay0click();
				Long d_max = d_min + 86400000;
				if(Long.parseLong(it) < d_min || Long.parseLong(it) > d_max) {return;}
			
				//曝光    
				if (Objects.equals("01",ec) && Objects.equals("01",ea) && Objects.equals("0",sp)) {
					String articleId = ecp.get("a");
					if(articleId == null) {return;}
					collector.collect(new Tuple14<>(stat_dt,dt,sid,uid,av,did,isnp,lng,lat,tv,articleId,1,0,0));
					return;}
				//点击
				if (Objects.equals("首页",ec) && Objects.equals("首页站内文章点击",ea) && Pattern.compile("^推荐_").matcher(el).find()) {
					String articleId = el.split("_")[2];
					if(articleId == null) {return;}
					collector.collect(new Tuple14<>(stat_dt,dt,sid,uid,av,did,isnp,lng,lat,cd13,articleId,0,1,0));
					return;}
				//电商点击
				if (Objects.equals("增强型电子商务",ec) && Objects.equals("添加到购物车",ea) && Pattern.compile("^首页_推荐_feed流_").matcher(cd21).find()) {
					String articleId = ecp.get("4");
					if(articleId == null) {return;}
					collector.collect(new Tuple14<>(stat_dt,dt,sid,uid,av,did,isnp,lng,lat,cd13,articleId,0,0,1));
					return;}
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
