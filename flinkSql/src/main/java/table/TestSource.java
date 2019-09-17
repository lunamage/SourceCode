package table;

import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;


public class TestSource implements SourceFunction<Tuple5<String, String, String, String, Long>> {

	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	private volatile boolean isRunning = true;
	private static final long serialVersionUID = 1L;
	private Date date;
	private AtomicLong al = new AtomicLong(0L);

	String mall[] = new String[] {"京东","天猫精选","其他","海囤全球","网易考拉","苏宁易购","天猫国际官方直营","拼多多","飞猪","天猫超市","淘宝精选","招商银行网上商城","天猫国际","小米官网","当当","讲究","聚划算","唯品会","途虎养车"};
	String cate3[] = new String[] {"禽蛋肉类","书写工具","海鲜水产","其他","手机","纸品湿巾","饮料","新鲜水果","日常办公","米面杂粮","骑行运动","男裤","显示器","收纳用品","男上装","水具酒具","笔记本电脑","烹饪锅具","移动电源","甜品"};
	String type[] = new String[] {"show","sec"};

	@Override
	public void run(SourceContext<Tuple5<String, String, String, String, Long>> ctx) throws Exception {
		
		while (isRunning) {
			Thread.sleep(1000);
			
			date = new Date();
			int userid = (int) (Math.random() * 30) ;
			String t1 = mall[(int) (Math.random() * 19)];
			String t2 = cate3[(int) (Math.random() * 20)];
			String t3 = type[(int) (Math.random() * 2)];
			
			ctx.collect(new Tuple5<>(String.valueOf(userid),t1,t2,t3,date.getTime()));
		}
	}

	@Override
	public void cancel() {
		isRunning = false;
	}

}