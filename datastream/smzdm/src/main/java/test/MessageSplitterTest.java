package test;


import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import utils.DateUtils;
import utils.ReadConfig;

public class MessageSplitterTest {
	
	public static void main(String[] args) throws Exception {
    
		System.out.println(DateUtils.getCurrentDay0click());
		
		
		//推荐_24小时内-非同步到首页的国内/海淘数据_14140254_9_无
		//推荐_24小时内-非同步到首页的国内/海淘数据_14156320_7_无
		//推荐_预售数据_14111595_18_无
		//cd20 3,76
		
		System.out.println(ReadConfig.getProperties("bootstrap.servers"));
		
		//电商点击 cd4 cd20

	}
}
