package ArtReadFeatureTest;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class Source implements SourceFunction<Tuple4<String,Integer,Integer,Long>> {

	private volatile boolean isRunning = true;
	
	@Override
	public void run(SourceContext<Tuple4<String,Integer,Integer,Long>> ctx) throws Exception {
		while(isRunning) {
			Thread.sleep(1000);
			
			String id = String.valueOf((int)(Math.random()*5));
			Integer time = (int)(Math.random()*30);
			Integer finish = (int)(Math.random()*100);
			Long etime = 0L;
			
			ctx.collect(new Tuple4<String,Integer,Integer,Long>(id,time,finish,etime));
		}
		
	}

	@Override
	public void cancel() {
		isRunning=false;
		
	}


}
