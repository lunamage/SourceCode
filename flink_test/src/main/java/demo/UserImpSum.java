package demo;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class UserImpSum {
	public static void main(String[] args) throws Exception {
		
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(10000);
		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
		
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
		
		DataStream<Tuple2<String,Long>> tmp = env.addSource(new Source());
		
		//tmp.keyBy(0).timeWindow(Time.seconds(10)).sum(1).print();
		tableEnv.registerDataStream("t", tmp, "user_id,imp,dt.proctime");
		
		String sql = "select user_id,sum(imp) imp from t group by user_id,tumble(dt,interval '10' second)";
		Table q = tableEnv.sqlQuery(sql);
		tableEnv.toRetractStream(q, Row.class).print();
		
		env.execute();
	}
}
