package recommend.deviceType;

import com.alibaba.fastjson.JSONObject;


import org.apache.flink.api.common.functions.RichFlatMapFunction;
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
public class UserDeviceFeatureSplitter extends RichFlatMapFunction<String, Tuple3<String, String, Long>> {
    /**
	 * 
	 */
	private static final long serialVersionUID = -8284916412921643582L;
	private static Logger log = LoggerFactory.getLogger(UserDeviceFeatureSplitter.class);
	
	private Map<String, String> currentMap = new ConcurrentHashMap<>();
	
	@Override
    public void open(Configuration parameters) {
		//currentMap.clear();
		
        Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.HOUR_OF_DAY, 3);//控制小时
        calendar.set(Calendar.MINUTE, 30);//控制分钟
        calendar.set(Calendar.SECOND, 0);//控制秒
        Date time = calendar.getTime();//执行任务时间为3:00:00
        Date newDate = DateUtils.getDateBeforeOrAfterDays(time, 1);
        Timer timer = new Timer();
        //每天定时3:00执行操作，延迟一天后再执行
        timer.schedule(new TimerTaskClearMap(), newDate, 1000 * 60 * 60 * 24);
    }
	
    @SuppressWarnings("unchecked")
	@Override
    public void flatMap(String msg, Collector<Tuple3<String, String, Long>> collector) {
		try {
			if(!isJson(msg)) {return;}
			Map<String,String> sdk = JSONObject.parseObject(msg, Map.class);
			
			//解析json
			String uid = sdk.get("uid");
			String did = sdk.get("did");
			String dm = sdk.get("dm");
			String slt = String.valueOf(sdk.get("slt"));
			String dt = sdk.get("dt");
			String nd = sdk.get("nd");
			
			String key = null;
			
			Long timestamp = slt == null || Objects.equals(slt, "0") ? System.currentTimeMillis() : Long.valueOf(slt) * 1000;
					
			//异常值处理
			if(dm == null) {return;}
			if(uid == null || !uid.matches("^[0-9]+$")) {
				if(dt.equals("1") || dt.equals("3")) {
					key = did;
				}else {
					key = nd;
				}
				
			}else {
				key = uid;
			}

			if(key == null) {return;}
			
			//缓存
			
			if(currentMap != null) {
				if(currentMap.get(key) != null) {return;}
			}
			
			currentMap.put(key, "1");
			
			collector.collect(new Tuple3<>(key,dm,timestamp));

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
    
    class TimerTaskClearMap extends TimerTask {
        // 每日3点定期清空缓存的文章数据
        @Override
        public void run() {
            log.info("delete Map size {} , time {} ", currentMap.size(), DateUtils.formatDate(new Date(), DateUtils.YYYYMMDD_HMS));
            currentMap.clear();
        }
    }

}
