package test;


import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
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
		
		/*String msg="{\"Order\":{\"OrderTime\":\"2019-06-07T19:04:34\",\"OrderID\":\"97379499249\",\"ParentID\":\"96918612140\",\"TotalMoney\":44.9000,\"CosPrice\":42.9000,\"YgCosFee\":0.8600,\"SplitTypeID\":0,\"UnionUsername\":\"1000089893\",\"Details\":null,\"SubUnion\":null,\"PopID\":\"0\",\"Yn\":1,\"SourceEmt\":2,\"Balance\":2,\"Plus\":1,\"JosAccountID\":1,\"SPlatformCode\":null,\"SSourceCode\":null,\"SChannelCode\":null,\"SArticleID\":0,\"SUserID\":0,\"EnteredDate\":\"2019-06-07 19:05:34\",\"validCode\":16,\"FinishTime\":null},\"OrderProducts\":[{\"ProductID\":\"1582013\",\"Qty\":1,\"FirstCategoryID\":737,\"SecondCategoryID\":752,\"ThirdCategoryID\":760,\"YgCosFee\":0.8600,\"CommissionReal\":null,\"BaseCategoryID\":27,\"CommissionRate\":0.00,\"CosPrice\":42.90,\"ProductName\":\"%e5%bf%97%e9%ab%98%ef%bc%88CHIGO%ef%bc%89%e7%94%b5%e7%83%ad%e6%b0%b4%e5%a3%b6+304%e4%b8%8d%e9%94%88%e9%92%a2+%e5%8f%8c%e5%b1%82%e9%98%b2%e7%83%ab%e7%83%a7%e6%b0%b4%e5%a3%b6+ZD18A-708G8+1.8L%e7%94%b5%e6%b0%b4%e5%a3%b6+%e7%b4%ab%e8%89%b2\",\"ReturnNum\":0,\"UnitPrice\":44.90,\"SubSideRate\":90.00,\"SubsidyRate\":10.00,\"WareID\":\"1582013\",\"SubUnionID\":null,\"JosAccountID\":1,\"validCode\":16,\"BaseCategoryID1\":27,\"BaseCategoryID2\":33,\"BaseCategoryID3\":0,\"OrderTime\":\"2019-06-07T19:04:34\"}]}";
		
		@SuppressWarnings("unchecked")
		Map<String,Map<String,String>> gmv = JSONObject.parseObject(msg, Map.class);
		Map<String,String> order = gmv.get("Order");
		Object cosPrice=order.get("CosPrice");
		System.out.println(""); */
		
		try {
		
		String msg="[\"8\",\"146\",\"22\",\"14268401\",\"1997266766\",\"9.4.11\",\"b4837e052fa67e29d57dd071192e98dd\",\"{}\",\"{\\\"dimension64\\\":\\\"\\u9996\\u9875_\\u63a8\\u8350_feed\\u6d41_q_\\u53d1\\u73b0\\u5934\\u90e8\\u6570\\u636e\\\",\\\"g_abtoken\\\":\\\"12_a,14_a,16_a,18_a,20_b,22_b,24_a,27_a,32_a,34_a,36_a,39_b,42_f,43_b\\\",\\\"haojia_content_abtest\\\":\\\"a\\\",\\\"haojia_style_abtest\\\":\\\"b\\\",\\\"haojia_title_abtest\\\":\\\"a\\\",\\\"middle_stage_gmv\\\":\\\"T2_tl\\\\u003d2_ab\\\\u003dq_hjab\\\\u003di_hwab\\\\u003df_xhab\\\\u003da_pd\\\\u003dfaxian_pl\\\\u003d\\u6bcd\\u5a74\\u7528\\u54c1\\/\\u5976\\u7c89\\/\\u5a74\\u513f\\u5976\\u7c89\\/\\u65e0_eq\\\\u003dandroid_id\\\\u003d14268401\\\"}\"]";
		
		//String[] a = msg.replaceAll("\\[|\\]", "").split(",");
		//System.out.println(a[0].substring(1, a[0].length()-1) );
		//System.out.println(a[8].substring(1, a[8].length()-1) );
		
		char arr[] = msg.toCharArray();
		ArrayList arr1 = new ArrayList();
		String x = "";
		
		for (int i = 1; i < arr.length-1; i++) {
			if(",".equals(String.valueOf(arr[i])) && x.indexOf("{") != -1 && x.indexOf("}") != -1 || ",".equals(String.valueOf(arr[i])) && x.indexOf("{") == -1) {
				arr1.add(x);
				x = "";
			}else {
				x += arr[i];
			}
		}
		
		arr1.add(unicodeToString(x));
		System.out.println(arr1.get(0));
		System.out.println(arr1.get(1));
		System.out.println(arr1.get(2));
		System.out.println(arr1.get(3));
		System.out.println(arr1.get(4));
		System.out.println(arr1.get(5));
		System.out.println(arr1.get(6));
		System.out.println(arr1.get(7));
		System.out.println(arr1.get(8));
		
		
		
		//while(msg.length() != 0) {
		//	System.out.println( msg.toCharArray() );
		//}
		
		
		}
		catch(Exception e){
			System.out.println("null");
		}
		
//		Map<String,String> order = JSONObject.parseObject(gmv.get("Order").toString(), Map.class);

	}
	
	public static String unicodeToString(String unicode) {
		StringBuffer sb = new StringBuffer();
		String[] hex = unicode.split("\\\\u");
		for (int i = 1; i < hex.length; i++) {
			int index = Integer.parseInt(hex[i], 16);
			sb.append((char) index);
		}
		return sb.toString();
	}
}
