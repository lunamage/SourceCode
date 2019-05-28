package service;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.alibaba.fastjson.JSONObject;

import model.GaHits;
import model.GaSession;

@SuppressWarnings("serial")
public class GaHitsFlatMap implements FlatMapFunction<String,GaHits>{
	
	private static Logger log = LoggerFactory.getLogger(GaHitsFlatMap.class);
	
	public void flatMap(String msg, Collector<GaHits> collector) {
		try {
			GaHits gaHits = JSONObject.parseObject(msg, GaHits.class);
			collector.collect(gaHits);
			} catch (Exception e) {
                log.error("flatMap error msg is {} value is {}", e.getMessage(), msg);
            }
        	
	}
}
