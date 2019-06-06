package service;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.alibaba.fastjson.JSONObject;

import model.SdkLog;

@SuppressWarnings("serial")
public class SdkLogFlatMap implements FlatMapFunction<String,SdkLog>{
	
	private static Logger log = LoggerFactory.getLogger(SdkLogFlatMap.class);
	
	public void flatMap(String msg, Collector<SdkLog> collector) {
		try {
			SdkLog sdkLog = JSONObject.parseObject(msg, SdkLog.class);
			collector.collect(sdkLog);
			} catch (Exception e) {
                //log.error("flatMap error msg is {} value is {}", e.getMessage(), msg,e);
                return;
            }
        	
	}
}
