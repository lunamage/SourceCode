package main;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.FileSystem.WriteMode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import model.SdkLog;
import service.SdkLogFlatMap;



/**
 * @author: zhaoyulong
 * @date: 2019/5/21
 * @time: 4:24 PM
 * @Description: 解析GA数据
 */
public class AnalysisSDKJson {
	
	private static Logger log = LoggerFactory.getLogger(AnalysisSDKJson.class);

    public static void main(String[] args) throws Exception {
        // 任务名称
    	ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        
    	//String txdate = args[0];
    	
        DataSet<String> hdfsLines = env.readTextFile("hdfs://cluster/bi/ori_log/app_sdk_rsync_log/2019/05/30/");
        
        
        DataSet<SdkLog> sdkLog = hdfsLines.flatMap(new SdkLogFlatMap()).first(100) ;
        
        sdkLog.writeAsText("hdfs://cluster/bi/ods_ga/ods_app_sdk_log_test/dt=2019-05-30", WriteMode.OVERWRITE);
        
        
        
        //HadoopOutputFormat<Text, LongWritable> hadoopOutputFormat =
		//		new HadoopOutputFormat<Text, LongWritable>(new TextOutputFormat<Text, LongWritable>(), new JobConf());
		//hadoopOutputFormat.getJobConf().set("mapred.textoutputformat.separator", " ");
		//TextOutputFormat.setOutputPath(hadoopOutputFormat.getJobConf(), new Path("asas")); 
        
        
        
      
        
        env.execute("sdk_dataset");
    }
    
     
}



