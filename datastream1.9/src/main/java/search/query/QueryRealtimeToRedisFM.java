package search.query;


import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author: zyl
 * @date: 2019/6/26
 * @time: 3:11 PM
 * @Description:
 */
public class QueryRealtimeToRedisFM extends RichFlatMapFunction<String, Tuple3<String, String, String>> {

    /**
	 * 
	 */
	private static final long serialVersionUID = -8284916412921643582L;
	private static Logger log = LoggerFactory.getLogger(QueryRealtimeToRedisFM.class);
	
	@Override
    public void flatMap(String msg, Collector<Tuple3<String, String, String>> collector) {
    	
		try {
				String[] a=msg.split("\\|");
				collector.collect(new Tuple3<>(a[1],a[2],a[3]));
        	} catch (Exception e) {
        		log.error("flatMap error msg is {} value is {}", e.getMessage(), msg,e);
        		return;
        	}
    }
}
