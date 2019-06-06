package service;

import com.alibaba.fastjson.JSONObject;


import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utils.DateUtils;
import utils.Utils;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;

/**
 * @author: zyl
 * @date: 2019/6/3
 * @time: 3:11 PM
 * @Description:
 */
public class SdkGroupArticleIdSplitter extends RichFlatMapFunction<String, Tuple6<String, String, String, String, String, Long>> {

    private static Logger log = LoggerFactory.getLogger(SdkGroupArticleIdSplitter.class);

    @Override
    public void flatMap(String msg, Collector<Tuple6<String, String, String, String, String, Long>> collector) {
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
			
			if(ec == null) {ec="smzdm";}
			if(ea == null) {ea="smzdm";}
			if(el == null) {el="smzdm";}
			if(sp == null) {sp="smzdm";}
			if(tv == null) {tv="smzdm";}
			if(cd13 == null) {cd13="smzdm";}
			if(cd21 == null) {cd21="smzdm";}
		
		
			if(it != null) {
				Date date = new Date(Long.parseLong(it));
				SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH");
				String stat_dt = dateFormat.format(date);
				Long d_min = DateUtils.getCurrentDay0click();
				Long d_max = d_min + 86400000;
				if(Long.parseLong(it) < d_min || Long.parseLong(it) > d_max) {return;}
			
				//曝光    
				if (Objects.equals("01",ec) && Objects.equals("01",ea) && Objects.equals("0",sp)) {
					String articleId = ecp.get("a");
					String channel = ecp.get("atp");
					if(articleId == null) {articleId="smzdm";}
					if(channel == null) {channel="smzdm";}
					collector.collect(new Tuple6<>(stat_dt,"exposure",tv,articleId,channel,System.currentTimeMillis()));
					return;}
				//点击
				if (Objects.equals("首页",ec) && Objects.equals("首页站内文章点击",ea) && Pattern.compile("^推荐_").matcher(el).find()) {
					String articleId = el.split("_")[2];
					String channel = ecp.get("20");
					if(articleId == null) {articleId="smzdm";}
					if(channel == null) {channel="smzdm";}
					collector.collect(new Tuple6<>(stat_dt,"click",cd13,articleId,channel,System.currentTimeMillis()));
					return;}
				//电商点击
				if (Objects.equals("增强型电子商务",ec) && Objects.equals("添加到购物车",ea) && Pattern.compile("^首页_推荐_feed流_").matcher(cd21).find()) {
					String articleId = ecp.get("4");
					String channel = ecp.get("20");
					if(articleId == null) {articleId="smzdm";}
					if(channel == null) {channel="smzdm";}
					collector.collect(new Tuple6<>(stat_dt,"event",cd13,articleId,channel,System.currentTimeMillis()));
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
