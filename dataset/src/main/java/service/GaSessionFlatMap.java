package service;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.alibaba.fastjson.JSONObject;

import model.GaSession;

@SuppressWarnings("serial")
public class GaSessionFlatMap implements FlatMapFunction<String,GaSession>{
	
	private static Logger log = LoggerFactory.getLogger(GaSessionFlatMap.class);
	
	public void flatMap(String msg, Collector<GaSession> collector) {
		try {
			GaSession gaSession = JSONObject.parseObject(msg, GaSession.class);
			collector.collect(gaSession);
			} catch (Exception e) {
                log.error("flatMap error msg is {} value is {}", e.getMessage(), msg,e);
                return;
            }
        	
	}
}
