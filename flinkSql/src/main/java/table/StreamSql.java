package table;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.TableSink ;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.List;


/**
 * 
 * @author user
 *
 */
public class StreamSql {

	private static final Logger LOG = LoggerFactory.getLogger(StreamSql.class);

	public static void main(String[] args) throws Exception {

		LOG.info("StreamSql start ");
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

		DataStream<Tuple5<String, String, String, String, Long>> data = env.addSource(new TestSource())
				.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple5<String, String, String, String, Long>>(Time.seconds(5)) {
		        	   @Override
		               public long extractTimestamp(Tuple5<String, String, String, String, Long> tuple) {return tuple.f4;}}) ;
		
		tableEnv.registerDataStream("t", data, "user_id,mall,cate3,type,dt.rowtime,ptime.proctime");
		

		//Table table = tableEnv.fromDataStream(data, "user_id,mall,cate3,type,dt,ptime.proctime");
		
		//table.printSchema();
		//orders表名
		//tableEnv.registerTable("t",table);
		
		
		
		String sql = " select mall,count(*) as pv,count(distinct user_id) as uv,HOP_END(ptime, INTERVAL '10' SECOND, INTERVAL '10' MINUTE) as stat_dt "
				   + " from t" 
				   //+ table 
				   + " group by HOP(ptime, INTERVAL '10' SECOND, INTERVAL '10' MINUTE),mall";
		
		Table result = tableEnv.sqlQuery("select * from t");
		
		//Table result = tableEnv.sqlQuery(sql);

		
		DataStream<Tuple2<Boolean, Row>> rw = tableEnv.toRetractStream(result, Row.class);
		rw.print();
		

		env.execute("StreamSql");

	}

	
}
