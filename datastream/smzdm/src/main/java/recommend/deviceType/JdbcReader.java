package recommend.deviceType;

import com.mysql.jdbc.Driver;

import utils.ReadConfig;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
 
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Hashtable;
import java.util.Map;
 
//RichSourceFunction RichParallelSourceFunction
public class JdbcReader extends RichSourceFunction<Map<String, String>> {
    private static final Logger logger = LoggerFactory.getLogger(JdbcReader.class);
 
    private Connection connection = null;
    private PreparedStatement ps = null;
    private volatile boolean isRunning = true;
 
    //该方法主要用于打开数据库连接，下面的ConfigKeys类是获取配置的类
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        String[] configure= ReadConfig.getProperties("db.sf").split("\\|");
        Class.forName(configure[1]);
        //DriverManager.registerDriver(new Driver());
        connection = DriverManager.getConnection(configure[0],configure[2],configure[3]);//获取连接
        ps = connection.prepareStatement("SELECT id,device_name from dim_device");
        
    }
 
    //执行查询并获取结果
    @Override
    public void run(SourceContext<Map<String, String>> ctx) throws Exception {
        Map<String, String> DeviceMap = new Hashtable<String, String>();
        try {
            while (isRunning) {
                ResultSet resultSet = ps.executeQuery();
                while (resultSet.next()) {
                    String id = String.valueOf(resultSet.getInt("id"));
                    String query = resultSet.getString("device_name");
                    if (!(id.isEmpty() && query.isEmpty())) {
                        DeviceMap.put(query, id);
                    }
                }
                logger.info("qwe "+DeviceMap.size());
                ctx.collect(DeviceMap);//发送结果
                DeviceMap.clear();
                Thread.sleep(70000 * 86400);
            }
        } catch (Exception e) {
            logger.error("runException:{}", e);
        }
    }
 
    //关闭数据库连接
    @Override
    public void cancel() {
        try {
            super.close();
            if (connection != null) {
                connection.close();
            }
            if (ps != null) {
                ps.close();
            }
        } catch (Exception e) {
            logger.error("runException:{}", e);
        }
        isRunning = false;
    }
}
