package test;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

import service.GaHitsCustomDimensionsFlatMap;
import service.GaHitsFlatMap;
import service.GaSessionFlatMap;

public class AnalysisGaJsonTest {
		
	public static void main(String[] args) throws Exception {
		
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> hdfsLines = env.readTextFile("C:/Users/zhaoyulong/Downloads/gajson.txt");
        //hdfsLines.flatMap(new GaSessionFlatMap()).print();
        //hdfsLines.flatMap(new GaHitsFlatMap()).print();
        hdfsLines.flatMap(new GaHitsCustomDimensionsFlatMap()).print();
}
}
