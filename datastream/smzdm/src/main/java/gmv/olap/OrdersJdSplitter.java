package gmv.olap;

import com.alibaba.fastjson.JSONObject;


import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
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
public class OrdersJdSplitter extends RichFlatMapFunction<String, Tuple5<String, String, String, Double, Long>> {

    private static Logger log = LoggerFactory.getLogger(OrdersJdSplitter.class);

    @Override
    public void flatMap(String msg, Collector<Tuple5<String, String, String, Double, Long>> collector) {
		try {
			if(!isJson(msg)) {return;}
			Map<String,String> order = JSONObject.parseObject(msg, Map.class);
			if(order == null) {return;}
			//解析json
			
			String keplerCustomerInfo = order.get("KeplerCustomerInfo");
			
			String orderTime = order.get("OrderTime");
			String orderID = order.get("OrderNo");
			Object OrderAmount = order.get("OrderAmount");
			Object yn = order.get("Yn");
			
			collector.collect(new Tuple5<>("jd",orderID,orderTime,Double.valueOf(OrderAmount.toString()),System.currentTimeMillis()));
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
