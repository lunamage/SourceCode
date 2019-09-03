package test.uv;

import com.alibaba.fastjson.JSONObject;


import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utils.DateUtils;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author: zyl
 * @date: 2019/6/26
 * @time: 3:11 PM
 * @Description:
 */
public class Splitter extends RichFlatMapFunction<String, Tuple2<String, Long>> {
    /**
	 * 
	 */
	private static final long serialVersionUID = -8284916412921643582L;
	private static Logger log = LoggerFactory.getLogger(Splitter.class);
	
	@Override
    public void flatMap(String msg, Collector<Tuple2<String, Long>> collector) {
		try {
			if(!isJson(msg)) {return;}
			Map<String,String> sdk = JSONObject.parseObject(msg, Map.class);
			
			//解析json
			String did = sdk.get("did");
			
			Long timestamp = System.currentTimeMillis();

			collector.collect(new Tuple2<>(did,timestamp));

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
