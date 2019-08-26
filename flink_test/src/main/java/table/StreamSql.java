package table;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
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

		DataStream<List> ds = env.addSource(new GeneUISource());
//		ds.print();
		DataStream<Tuple5<String, String, Long, String, String>> data = ds
				.map(new MapFunction<List, Tuple5<String, String, Long, String, String>>() {
					@Override
					public Tuple5<String, String, Long, String, String> map(List value) throws Exception {
						return new Tuple5<String, String, Long, String, String>(value.get(0).toString(),
								value.get(1).toString(), Long.parseLong(value.get(2).toString()),
								value.get(3).toString(), value.get(4).toString());
					}
				});
//
//				.assignTimestampsAndWatermarks(
//						new AssignerWithPunctuatedWatermarks<Tuple5<String, String, Long, String, String>>() {
//
//							@Override
//							public long extractTimestamp(Tuple5<String, String, Long, String, String> element,
//									long previousElementTimestamp) {
//
//
//								return element.f2;
//							}
//
//							@Override
//							public Watermark checkAndGetNextWatermark(
//									Tuple5<String, String, Long, String, String> lastElement, long extractedTimestamp) {
//								// TODO Auto-generated method stub
//								return new Watermark(lastElement.f2);
//							}
//						});
//
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
//
//
		Table table = tableEnv.fromDataStream(data, "province,id,datestamp,date,c,p.proctime");
////		Table result = tableEnv.sqlQuery(
////				"select province,TUMBLE_START(rowtime, INTERVAL '10' SECOND) as wStart, count(distinct id) as uv,count(id) as pv from "
////						+ table + "  group by TUMBLE(rowtime, INTERVAL '10' SECOND) , province");
//
		Table result = tableEnv.sqlQuery("select p from "+ table);
//
		tableEnv.toRetractStream(result, Row.class).print();
		env.execute("StreamSql");

	}

	public static class Result {
		private Timestamp wStart;
		private String province;
		private Long pv;
		private Long uv;

		public Timestamp getwStart() {
			return wStart;
		}

		public void setwStart(Timestamp wStart) {
			this.wStart = wStart;
		}

		public String getProvince() {
			return province;
		}

		public void setProvince(String province) {
			this.province = province;
		}

		public Long getPv() {
			return pv;
		}

		public void setPv(Long pv) {
			this.pv = pv;
		}

		public Long getUv() {
			return uv;
		}

		public void setUv(Long uv) {
			this.uv = uv;
		}

		@Override
		public String toString() {
			return "Result [wStart=" + wStart + ", province=" + province + ", pv=" + pv + ", uv=" + uv + "]";
		}

	}
}
