package test;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

import service.GaHitsCustomDimensionsFlatMap;
import service.GaHitsFlatMap;
import service.GaSessionFlatMap;
import service.SdkLogFlatMap;

public class AnalysisSDKJsonTest {
		
	public static void main(String[] args) throws Exception {
		
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> hdfsLines = env.readTextFile("C:/Users/zhaoyulong/Downloads/sdkjson.txt");
        hdfsLines.flatMap(new SdkLogFlatMap()).print();
}
}
