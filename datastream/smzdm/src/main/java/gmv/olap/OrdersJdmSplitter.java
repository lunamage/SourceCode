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
public class OrdersJdmSplitter extends RichFlatMapFunction<String, Tuple5<String, String, String, Double, Long>> {

    private static Logger log = LoggerFactory.getLogger(OrdersJdmSplitter.class);

    @Override
    public void flatMap(String msg, Collector<Tuple5<String, String, String, Double, Long>> collector) {
		try {
			if(!isJson(msg)) {return;}
			Map<String,Map<String,String>> gmv = JSONObject.parseObject(msg, Map.class);
			Map<String,String> order = gmv.get("Order");
			if(order == null) {return;}
			//解析json
			String orderTime = order.get("OrderTime");
			String orderID = order.get("OrderID");
			Object cosPrice = order.get("CosPrice");
			collector.collect(new Tuple5<>("jd",orderID,orderTime,Double.valueOf(cosPrice.toString()),System.currentTimeMillis()));
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
