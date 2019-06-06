package main;



import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.api.java.hadoop.mapred.HadoopOutputFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import model.GaHits;
import model.GaHitsCustomDimensions;
import model.GaSession;
import service.GaHitsCustomDimensionsFlatMap;
import service.GaHitsFlatMap;
import service.GaSessionFlatMap;

//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapred.JobConf;
//import org.apache.hadoop.mapred.TextOutputFormat;

/**
 * @author: zhaoyulong
 * @date: 2019/5/21
 * @time: 4:24 PM
 * @Description: 解析GA数据
 */
public class AnalysisGaJson {
	
	private static Logger log = LoggerFactory.getLogger(AnalysisGaJson.class);

    public static void main(String[] args) throws Exception {
        // 任务名称
    	ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        
    	String txdate = args[0];
    	
        DataSet<String> hdfsLines = env.readTextFile("hdfs://cluster/bi/stg_ga/stg_ga_clean_data/dt="+txdate);
        
        //
        //DataSet<GaSession> gaSession = hdfsLines.flatMap(new GaSessionFlatMap());
        //gaSession.writeAsText("hdfs://cluster/bi/ods_ga/ods_ga_sessions_data_test/dt="+txdate, WriteMode.OVERWRITE);
        
        
        
        DataSet<GaHits> gaHits = hdfsLines.flatMap(new GaHitsFlatMap());
        
        /*HadoopOutputFormat<Text, LongWritable> hadoopOutputFormat =
				new HadoopOutputFormat<Text, LongWritable>(new TextOutputFormat<Text, LongWritable>(), new JobConf());
		hadoopOutputFormat.getJobConf().set("mapred.textoutputformat.separator", " ");
		TextOutputFormat.setOutputPath(hadoopOutputFormat.getJobConf(), new Path("asas")); */
        
        /*val c = classOf[org.apache.hadoop.io.compress.GzipCodec]
hadoopOutputFormat.getJobConf.set("mapred.textoutputformat.separator", " ")
hadoopOutputFormat.getJobConf.setCompressMapOutput(true)
hadoopOutputFormat.getJobConf.set("mapred.output.compress", "true")
hadoopOutputFormat.getJobConf.setMapOutputCompressorClass(c)
hadoopOutputFormat.getJobConf.set("mapred.output.compression.codec", c.getCanonicalName)
hadoopOutputFormat.getJobConf.set("mapred.output.compression.type", CompressionType.BLOCK.toString)*/
        
        
        gaHits.writeAsText("hdfs://cluster/bi/ods_ga/ods_ga_hits_data_test/dt="+txdate, WriteMode.OVERWRITE);
        
        //DataSet<GaHitsCustomDimensions> gaHitsCustomDimensions = hdfsLines.flatMap(new GaHitsCustomDimensionsFlatMap());
        //gaHitsCustomDimensions.writeAsText("hdfs://cluster/bi/ods_ga/ods_ga_hits_customDimensions_data_test/dt="+txdate, WriteMode.OVERWRITE);
        
        //DataSet<GaHitsCustomDimensions> gaHitsCustomDimensions1 = hdfsLines.flatMap(new GaHitsCustomDimensionsFlatMap());
        //gaHitsCustomDimensions1.writeAsText("hdfs://cluster/bi/dw_ga/fact_ga_hits_data_test/dt="+txdate, WriteMode.OVERWRITE);
        
        env.execute("Run_flink_1d");
    }
    
     
}



