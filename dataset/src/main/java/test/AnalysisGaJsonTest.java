package test;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

import service.GaHitsFlatMap;
import service.GaSessionFlatMap;

public class AnalysisGaJsonTest {
		
	public static void main(String[] args) throws Exception {
		
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> localLines = env.readTextFile("C:/Users/zhaoyulong/Downloads/gajson.txt");
        //localLines.flatMap(new GaSessionFlatMap()).print();
        localLines.flatMap(new GaHitsFlatMap()).print();
     
}
}
