package main;



import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import model.GaSession;
import service.GaSessionFlatMap;

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
        DataSet<GaSession> gasession = hdfsLines.flatMap(new GaSessionFlatMap());
        gasession.writeAsText("hdfs://cluster/bi/ods_ga/ods_ga_sessions_data_test/dt="+txdate, WriteMode.OVERWRITE);
        
        env.execute("Run_flink_1d");
    }
    
     
}



