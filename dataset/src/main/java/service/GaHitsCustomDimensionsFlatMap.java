package service;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.alibaba.fastjson.JSONObject;

import model.GaHits;
import model.GaHitsCustomDimensions;
import model.GaSession;

@SuppressWarnings("serial")
public class GaHitsCustomDimensionsFlatMap implements FlatMapFunction<String,GaHitsCustomDimensions>{
	
	private static Logger log = LoggerFactory.getLogger(GaHitsCustomDimensionsFlatMap.class);
	
	public void flatMap(String msg, Collector<GaHitsCustomDimensions> collector) {
		try {
			
			
			GaHitsCustomDimensions gaHitsCustomDimensions = JSONObject.parseObject(msg, GaHitsCustomDimensions.class);
			collector.collect(gaHitsCustomDimensions);
			} catch (Exception e) {
                log.error("flatMap error msg is {} value is {}", e.getMessage(), msg,e);
                return;
            }
        	
	}
}
