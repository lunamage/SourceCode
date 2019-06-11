package test;


import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;

import com.alibaba.fastjson.JSONObject;

import utils.DateUtils;
import utils.ReadConfig;

public class MessageSplitterTest {
	
	public static void main(String[] args) throws Exception {
    
		//System.out.println(DateUtils.getCurrentDay0click());
		
		
		//推荐_24小时内-非同步到首页的国内/海淘数据_14140254_9_无
		//推荐_24小时内-非同步到首页的国内/海淘数据_14156320_7_无
		//推荐_预售数据_14111595_18_无
		//cd20 3,76
		
		//System.out.println(ReadConfig.getProperties("bootstrap.servers"));
		
		//电商点击 cd4 cd20
		
		String msg="{\"Order\":{\"OrderTime\":\"2019-06-07T19:04:34\",\"OrderID\":\"97379499249\",\"ParentID\":\"96918612140\",\"TotalMoney\":44.9000,\"CosPrice\":42.9000,\"YgCosFee\":0.8600,\"SplitTypeID\":0,\"UnionUsername\":\"1000089893\",\"Details\":null,\"SubUnion\":null,\"PopID\":\"0\",\"Yn\":1,\"SourceEmt\":2,\"Balance\":2,\"Plus\":1,\"JosAccountID\":1,\"SPlatformCode\":null,\"SSourceCode\":null,\"SChannelCode\":null,\"SArticleID\":0,\"SUserID\":0,\"EnteredDate\":\"2019-06-07 19:05:34\",\"validCode\":16,\"FinishTime\":null},\"OrderProducts\":[{\"ProductID\":\"1582013\",\"Qty\":1,\"FirstCategoryID\":737,\"SecondCategoryID\":752,\"ThirdCategoryID\":760,\"YgCosFee\":0.8600,\"CommissionReal\":null,\"BaseCategoryID\":27,\"CommissionRate\":0.00,\"CosPrice\":42.90,\"ProductName\":\"%e5%bf%97%e9%ab%98%ef%bc%88CHIGO%ef%bc%89%e7%94%b5%e7%83%ad%e6%b0%b4%e5%a3%b6+304%e4%b8%8d%e9%94%88%e9%92%a2+%e5%8f%8c%e5%b1%82%e9%98%b2%e7%83%ab%e7%83%a7%e6%b0%b4%e5%a3%b6+ZD18A-708G8+1.8L%e7%94%b5%e6%b0%b4%e5%a3%b6+%e7%b4%ab%e8%89%b2\",\"ReturnNum\":0,\"UnitPrice\":44.90,\"SubSideRate\":90.00,\"SubsidyRate\":10.00,\"WareID\":\"1582013\",\"SubUnionID\":null,\"JosAccountID\":1,\"validCode\":16,\"BaseCategoryID1\":27,\"BaseCategoryID2\":33,\"BaseCategoryID3\":0,\"OrderTime\":\"2019-06-07T19:04:34\"}]}";
		
		@SuppressWarnings("unchecked")
		Map<String,Map<String,String>> gmv = JSONObject.parseObject(msg, Map.class);
		Map<String,String> order = gmv.get("Order");
		Object cosPrice=order.get("CosPrice");
		System.out.println("");
		
		
		
		
//		Map<String,String> order = JSONObject.parseObject(gmv.get("Order").toString(), Map.class);

	}
}
