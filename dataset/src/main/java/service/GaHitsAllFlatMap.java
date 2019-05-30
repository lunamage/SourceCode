package service;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.alibaba.fastjson.JSONObject;

import model.GaHitsAll;

@SuppressWarnings("serial")
public class GaHitsAllFlatMap implements FlatMapFunction<String,GaHitsAll>{
	
	private static Logger log = LoggerFactory.getLogger(GaHitsAllFlatMap.class);
	
	public void flatMap(String msg, Collector<GaHitsAll> collector) {
		try {
			GaHitsAll gaHitsAll = JSONObject.parseObject(msg, GaHitsAll.class);
			collector.collect(gaHitsAll);
			} catch (Exception e) {
                log.error("flatMap error msg is {} value is {}", e.getMessage(), msg);
            }
        	
	}
}
