package demo;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class Source implements SourceFunction<Tuple2<String,Long>> {

	private volatile boolean isRunning = true;
	
	@Override
	public void run(SourceContext<Tuple2<String, Long>> ctx) throws Exception {
		while(isRunning) {
			Thread.sleep(1000);
			
			String userid = "u"+String.valueOf((int)(Math.random()*5));
			Long imp = 1L;
			ctx.collect(new Tuple2<String, Long>(userid,imp));
		}
		
	}

	@Override
	public void cancel() {
		isRunning=false;
		
	}


}
