package test.uv;

import com.alibaba.fastjson.JSONObject;


import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.flink.api.common.state.StateTtlConfig;

import utils.DateUtils;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author: zyl
 * @date: 2019/6/26
 * @time: 3:11 PM
 * @Description:
 */
public class StateSplitter extends RichFlatMapFunction<Tuple2<String, Long>, Tuple3<String,Long, Long>> {
   
	/** The state for the current key. */
	private ValueState<String> currentState; 

	@Override
	public void open(Configuration conf) {
		// get access to the state object
		//currentState = getRuntimeContext().getState(new ValueStateDescriptor<>("state",TypeInformation.of(new TypeHint<String>() {})));
		StateTtlConfig config = StateTtlConfig.newBuilder(Time.seconds(100)).setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite).setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired).build();
		ValueStateDescriptor<String> descriptor = new ValueStateDescriptor<>("state",String.class);
		currentState = getRuntimeContext().getState(descriptor);
        descriptor.enableTimeToLive(config);
		
	}


	@Override
	public void flatMap(Tuple2<String, Long> value, Collector<Tuple3<String,Long, Long>> out) throws Exception {
		// TODO Auto-generated method stub
		// get the current state for the key (source address)
				// if no state exists, yet, the state must be the state machine's initial state
				String state = currentState.value(); 
				if (state == null) {
					currentState.update(value.f0);
					out.collect(new Tuple3<>(value.f0,1L,value.f1));
					//currentState.clear();
				}
	}
}