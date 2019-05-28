package test;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;

public class DataSetTest {
		
	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		 
		// 从本地文件系统读
		DataSet<String> localLines = env.readTextFile("file:///path/to/my/textfile");
		 
		// 读取HDFS文件
		DataSet<String> hdfsLines = env.readTextFile("hdfs://nnHost:nnPort/path/to/my/textfile");
		 
		// 读取CSV文件
		DataSet<Tuple3<Integer, String, Double>> csvInput = env.readCsvFile("hdfs:///the/CSV/file").types(Integer.class, String.class, Double.class);
		 
		// 读取CSV文件中的部分
		//DataSet<Tuple2<String, Double>> csvInput = env.readCsvFile("hdfs:///the/CSV/file").includeFields("10010").types(String.class, Double.class);
		 
		// 读取CSV映射为一个java类
		//DataSet<Person>> csvInput = env.readCsvFile("hdfs:///the/CSV/file").pojoType(Person.class, "name", "age", "zipcode");
		 
		// 读取一个指定位置序列化好的文件
		//DataSet<Tuple2<IntWritable, Text>> tuples = env.readSequenceFile(IntWritable.class, Text.class, "hdfs://nnHost:nnPort/path/to/file");
		 
		// 从输入字符创建
		DataSet<String> value = env.fromElements("Foo", "bar", "foobar", "fubar");
		 
		// 创建一个数字序列
		DataSet<Long> numbers = env.generateSequence(1, 10000000);
		 
		// 从关系型数据库读取
		//DataSet<Tuple2<String, Integer> dbData =
		//env.createInput(JDBCInputFormat.buildJDBCInputFormat().setDrivername("org.apache.derby.jdbc.EmbeddedDriver").setDBUrl("jdbc:derby:memory:persons")
		//.setQuery("select name, age from persons")
		//.setRowTypeInfo(new RowTypeInfo(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO))
		//.finish());
		
		
		
		DataSet<Tuple4<Integer, String, Integer, String>> dataSet = env.fromElements(
	            new Tuple4<Integer, String, Integer, String>(1, "haus",     4, "garten"),
                new Tuple4<Integer, String, Integer, String>(2, "xarten",   4, "garten"),
                new Tuple4<Integer, String, Integer, String>(3, "gartex",   4, "garten"),
                new Tuple4<Integer, String, Integer, String>(4, "garten",   4, "garten"),
                new Tuple4<Integer, String, Integer, String>(5, "gaxten",   4, "garten"),
                new Tuple4<Integer, String, Integer, String>(6, "arten",    4, "garten"),
                new Tuple4<Integer, String, Integer, String>(7, "gar",      4, "garten"));


        dataSet.distinct(0).project(0,1).print();
        value.print();
     
        
        
        
        
        /*
        // text data
		DataSet<String> textData = // [...]
 
		// write DataSet to a file on the local file system
		textData.writeAsText("file:///my/result/on/localFS");
 
		// write DataSet to a file on a HDFS with a namenode running at nnHost:nnPort
		textData.writeAsText("hdfs://nnHost:nnPort/my/result/on/localFS");
 
		// write DataSet to a file and overwrite the file if it exists
		textData.writeAsText("file:///my/result/on/localFS", WriteMode.OVERWRITE);
 
		// tuples as lines with pipe as the separator "a|b|c"
		DataSet<Tuple3<String, Integer, Double>> values = // [...]
		values.writeAsCsv("file:///path/to/the/result/file", "\n", "|");
 
		// this writes tuples in the text formatting "(a, b, c)", rather than as CSV lines
		values.writeAsText("file:///path/to/the/result/file");
 
		// this writes values as strings using a user-defined TextFormatter object
		values.writeAsFormattedText("file:///path/to/the/result/file",
    		new TextFormatter<Tuple2<Integer, Integer>>() {
        		public String format (Tuple2<Integer, Integer> value) {
            		return value.f1 + " - " + value.f0;
        		}
    		});
    
		DataSet<Tuple3<String, Integer, Double>> myResult = [...]
 
		// write Tuple DataSet to a relational database
		myResult.output(
    	// build and configure OutputFormat
    	JDBCOutputFormat.buildJDBCOutputFormat()
                    .setDrivername("org.apache.derby.jdbc.EmbeddedDriver")
                    .setDBUrl("jdbc:derby:memory:persons")
                    .setQuery("insert into persons (name, age, height) values (?,?,?)")
                    .finish());
                    
         */
        
        
        
        
}
}
