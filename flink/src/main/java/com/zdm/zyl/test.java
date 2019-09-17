package com.zdm.zyl;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.shaded.guava18.com.google.common.base.Strings;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import zyl.ImpLogEntity;
import zyl.ItemEntity;
public class test {
	@SuppressWarnings("unchecked")
	
	public static void main(String[] args) throws Exception {
		//long current = System.currentTimeMillis();
        //long zero = current/(1000*3600*24)*(1000*3600*24) - TimeZone.getDefault().getRawOffset();
        //System.out.print(zero);
		
		//String js2="{\"visitNumber\":\"1\",\"visitId\":\"1531708301\",\"visitStartTime\":\"1531708301\",\"date\":\"20180716\",\"totals\":{\"visits\":\"1\",\"hits\":\"1\",\"pageviews\":\"1\",\"bounces\":\"1\",\"newVisits\":\"1\",\"sessionQualityDim\":\"0\"},\"trafficSource\":{\"referralPath\":\"(not set)\",\"campaign\":\"(not set)\",\"source\":\"(direct)\",\"medium\":\"(none)\",\"keyword\":\"(not set)\",\"adContent\":\"(not set)\",\"adwordsClickInfo\":{},\"isTrueDirect\":true},\"device\":{\"browser\":\"Safari (in-app)\",\"browserVersion\":\"(not set)\",\"browserSize\":\"380x600\",\"operatingSystem\":\"iOS\",\"operatingSystemVersion\":\"11.4\",\"isMobile\":true,\"mobileDeviceBranding\":\"Apple\",\"mobileDeviceModel\":\"iPhone\",\"mobileInputSelector\":\"touchscreen\",\"mobileDeviceInfo\":\"Apple iPhone\",\"mobileDeviceMarketingName\":\"(not set)\",\"flashVersion\":\"(not set)\",\"javaEnabled\":false,\"language\":\"zh-cn\",\"screenColors\":\"32-bit\",\"screenResolution\":\"375x667\",\"deviceCategory\":\"mobile\"},\"geoNetwork\":{\"continent\":\"Asia\",\"subContinent\":\"Eastern Asia\",\"country\":\"China\",\"region\":\"Hunan\",\"metro\":\"(not set)\",\"city\":\"Changsha\",\"cityId\":\"1003541\",\"networkDomain\":\"unknown.unknown\",\"latitude\":\"28.2282\",\"longitude\":\"112.9388\",\"networkLocation\":\"chinanet hunan province network\"},\"customDimensions\":[{\"index\":\"8\",\"value\":\"Mozilla/5.0 (iPhone; CPU iPhone OS 11_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15F79\"}],\"hits\":[{\"hitNumber\":\"1\",\"time\":\"0\",\"hour\":\"10\",\"minute\":\"31\",\"isInteraction\":true,\"isEntrance\":true,\"isExit\":true,\"referer\":\"https://m.smzdm.com/fenlei/sushenku/\",\"page\":{\"pagePath\":\"/wiki.m.smzdm.com/p/egy971/\",\"hostname\":\"wiki.m.smzdm.com\",\"pageTitle\":\"MAIDENFORM Flexees 女士塑身无痕打底裤【价格 测评 怎么样】_什么值得买\",\"pagePathLevel1\":\"/wiki.m.smzdm.com/\",\"pagePathLevel2\":\"/p/\",\"pagePathLevel3\":\"/egy971/\",\"pagePathLevel4\":\"/\"},\"appInfo\":{\"screenName\":\"wiki.m.smzdm.com/wiki.m.smzdm.com/p/egy971/\",\"landingScreenName\":\"wiki.m.smzdm.com/wiki.m.smzdm.com/p/egy971/\",\"exitScreenName\":\"wiki.m.smzdm.com/wiki.m.smzdm.com/p/egy971/\",\"screenDepth\":\"0\"},\"exceptionInfo\":{\"isFatal\":true},\"product\":[{\"productSKU\":\"2403061\",\"v2ProductName\":\"MAIDENFORM Flexees  女士塑身无痕打底裤\",\"v2ProductCategory\":\"塑身裤/无/无/无\",\"productVariant\":\"(not set)\",\"productBrand\":\"MAIDENFORM\",\"productPrice\":\"159000000\",\"localProductPrice\":\"0\",\"customDimensions\":[],\"customMetrics\":[],\"productListName\":\"WAP\",\"productListPosition\":\"0\",\"productCouponCode\":\"(not set)\"}],\"promotion\":[],\"eCommerceAction\":{\"action_type\":\"2\",\"step\":\"1\"},\"experiment\":[],\"customVariables\":[],\"customDimensions\":[{\"index\":\"1\",\"value\":\"无\"},{\"index\":\"2\",\"value\":\"无\"},{\"index\":\"3\",\"value\":\"MAIDENFORM\"},{\"index\":\"4\",\"value\":\"无\"},{\"index\":\"5\",\"value\":\"无\"},{\"index\":\"6\",\"value\":\"塑身裤\"},{\"index\":\"7\",\"value\":\"无\"},{\"index\":\"8\",\"value\":\"Mozilla/5.0 (iPhone; CPU iPhone OS 11_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15F79\"},{\"index\":\"13\",\"value\":\"wiki\"},{\"index\":\"35\",\"value\":\"无\"},{\"index\":\"37\",\"value\":\"egy971\"},{\"index\":\"50\",\"value\":\"\"}],\"customMetrics\":[],\"type\":\"PAGE\",\"social\":{\"socialNetwork\":\"(not set)\",\"hasSocialSourceReferral\":\"No\",\"socialInteractionNetworkAction\":\" : \"},\"contentGroup\":{\"contentGroup1\":\"(not set)\",\"contentGroup2\":\"(not set)\",\"contentGroup3\":\"(not set)\",\"contentGroup4\":\"(not set)\",\"contentGroup5\":\"(not set)\",\"previousContentGroup1\":\"(entrance)\",\"previousContentGroup2\":\"(entrance)\",\"previousContentGroup3\":\"(entrance)\",\"previousContentGroup4\":\"(entrance)\",\"previousContentGroup5\":\"(entrance)\"},\"dataSource\":\"web\",\"publisher_infos\":[]}],\"fullVisitorId\":\"29845102206387085\",\"clientId\":\"6948854.1531708301\",\"channelGrouping\":\"Direct\",\"socialEngagementType\":\"Not Socially Engaged\"}";
		//SdkLog sdkLog = JSONObject.parseObject(js, SdkLog.class);
		
		//String js="{\"imei\":\"7d0f8d6d1d7a9c739ec540d06f8a7dc1\",\"aid\":\"com.smzdm.client.android\",\"lng\":\"116.380989\",\"sid\":\"15579053105286641\",\"uid\":\"\",\"tid\":\"UA-1000000-2\",\"an\":\"smzdm\",\"ltp\":0,\"ca\":\"2\",\"anid\":\"59b15a0450473a4db0d81003ab18e125\",\"exposed_id\":\"0101_1_13477191_\",\"sr\":\"1080*2029\",\"ea\":\"01\",\"uuid\":\"9da4c5aa-7ba6-4b43-a73e-6ac1cf5724ae\",\"ecp\":\"{\\\"22\\\":\\\"同步到首页-24\\\",\\\"tv\\\":\\\"n\\\",\\\"24\\\":\\\"普通\\\",\\\"25\\\":\\\"精选\\\\u0026玩模乐器热度Top8\\\",\\\"pid\\\":\\\"844e4ea347aab2b972eb2f7b69761649\\\",\\\"oos\\\":\\\"无\\\",\\\"ref\\\":\\\"手动刷新\\\",\\\"atp\\\":\\\"3\\\",\\\"sp\\\":\\\"0\\\",\\\"sit\\\":\\\"1557905370698\\\",\\\"11\\\":\\\"youhui\\\",\\\"a\\\":\\\"13477191\\\",\\\"13\\\":\\\"n\\\",\\\"35\\\":\\\"b\\\",\\\"c\\\":\\\"1\\\",\\\"14\\\":\\\"844e4ea347aab2b972eb2f7b69761649\\\",\\\"ad\\\":\\\"2\\\",\\\"tagID\\\":\\\"无\\\",\\\"p\\\":\\\"10\\\",\\\"ct\\\":\\\"24小时内-非同步到首页的国内\\/海淘数据\\\",\\\"1\\\":\\\"亚马逊中国\\\",\\\"5\\\":\\\"玩模乐器\\\",\\\"6\\\":\\\"乐器\\\",\\\"7\\\":\\\"无\\\",\\\"8\\\":\\\"无\\\",\\\"9\\\":\\\"京东生鲜\\\",\\\"40\\\":\\\"100_c,102_b,105_d,106_b,110_b,112_b,114_b,118_b,120_b,123_a,129_b,132_c,135_b,137_a,141_a,142_c,94_b\\\"}\",\"hnb\":\"138\",\"ec\":\"01\",\"type\":\"show\",\"ift\":\"0\",\"id\":1120,\"nd\":\"7d0f8d6d1d7a9c739ec540d06f8a7dc1\",\"v\":\"1.11\",\"isnp\":\"1\",\"ip\":\"36.110.48.246\",\"dt\":\"2\",\"ds\":\"app\",\"m\":\"430658f11c5d751d9afbe4eff9cf3449\",\"ch\":\"smzdm_update\",\"av\":\"9.4.5.76\",\"it\":\"1557905370699\",\"os\":\"9\",\"lat\":\"39.849736\",\"slt\":1557905372,\"did\":\"9086693d74bcc4bd6c4d5e8bfad66804\",\"nt\":\"wifi\",\"cid\":\"9086693d74bcc4bd6c4d5e8bfad66804.1557387456547\",\"mac\":\"94:87:E0:24:C1:FA\",\"dm\":\"MI 8\"}";
		//Map<String,String> sdk = JSONObject.parseObject(js, Map.class);
		//Map<String,String> ecp = JSONObject.parseObject(sdk.get("ecp"), Map.class);
		//System.out.print(ecp.get("a"));
		
		//String js="{\"imei\":\"\",\"aid\":\"com.smzdm.client.ios\",\"lng\":\"116.3807724925395\",\"sid\":\"15579727793595\",\"uid\":\"\",\"tid\":\"UA-1000000-1\",\"an\":\"smzdm\",\"ca\":\"0\",\"sr\":\"1242x2688\",\"ea\":\"01\",\"uuid\":\"AA2915F0-6B1C-4C28-943D-6B5DBDD66FEB\",\"ecp\":\"{\\\"p\\\":\\\"3\\\",\\\"sit\\\":\\\"1557972782914\\\",\\\"c\\\":\\\"1\\\",\\\"oos\\\":\\\"无\\\",\\\"25\\\":\\\"精选&值友专享\\\",\\\"40\\\":\\\"101_c,104_d,107_b,109_b,111_b,113_a,115_a,117_b,119_a,122_a,131_b,133_b,138_b,139_a,143_c,95_a,99_b\\\",\\\"11\\\":\\\"youhui\\\",\\\"tagID\\\":\\\"无\\\",\\\"1\\\":\\\"京东\\\",\\\"35\\\":\\\"a\\\",\\\"sp\\\":\\\"0\\\",\\\"ct\\\":\\\"24小时内-非同步到首页的国内\\\\\\/海淘数据\\\",\\\"13\\\":\\\"v\\\",\\\"tv\\\":\\\"v\\\",\\\"14\\\":\\\"5a201f91d763f74e5fb6e9293fbb0644\\\",\\\"atp\\\":\\\"3\\\",\\\"22\\\":\\\"非同步到首页-24\\\",\\\"5\\\":\\\"电脑数码\\\",\\\"ref\\\":\\\"启动APP\\\",\\\"6\\\":\\\"无\\\",\\\"7\\\":\\\"无\\\",\\\"pid\\\":\\\"5a201f91d763f74e5fb6e9293fbb0644\\\",\\\"ad\\\":\\\"2\\\",\\\"8\\\":\\\"无\\\",\\\"a\\\":\\\"13477291\\\",\\\"24\\\":\\\"普通\\\",\\\"9\\\":\\\"无\\\"}\",\"hnb\":\"4\",\"ec\":\"01\",\"type\":\"show\",\"ift\":\"0\",\"st\":\"iPhone\",\"v\":\"1.10\",\"isnp\":\"1\",\"ip\":\"210.12.11.20\",\"sv\":\"1.1.0\",\"dt\":\"1\",\"ds\":\"app\",\"m\":\"1cd3a91f940f3fd0598f1ab935339f48\",\"ch\":\"AppStore\",\"slt\":1557972788,\"av\":\"9.4.10.41\",\"it\":\"1557972782918\",\"cid\":\"st+HFt+MzksWL+ZZsugizmUFDRJRBHgCohhqAxWokpbGpbSitCNpfg==.1557972767279\",\"lat\":\"39.84980579706563\",\"idfa\":\"nsYea62x02FcmZsQCW343BBdK0g7znfqC\\/KdQslMF4rxOAef2uQxwA==\",\"did\":\"st+HFt+MzksWL+ZZsugizmUFDRJRBHgCohhqAxWokpbGpbSitCNpfg==\",\"dm\":\"iPhone11,6\",\"idfv\":\"oLP8XjJFhxMVIf4ePFEyurraWAcmkXxWQAAR+QOVMRuX6onga97+Qw==\",\"nt\":\"wifi\",\"os\":\"12.0\"}";
		//ImpLogEntity entity = JSONObject.parseObject(js, ImpLogEntity.class);
		//ItemEntity itemEntity = JSON.parseObject(entity.getEcp(), ItemEntity.class);
		//System.out.print(itemEntity.getItemId());
		
		//System.out.print(Pattern.compile("^推荐_").matcher("推荐_asasas").find());
		
		//GaData session = JSONObject.parseObject(js2, GaData.class);
		//System.out.print(session.getHits().get(0).getCustomDimensions());
		
		//SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");  
        //String dateString = formatter.format(new Date());
        //System.out.print(dateString);
		
		//System.out.print(System.currentTimeMillis()/(1000*3600*24)*(1000*3600*24) - TimeZone.getDefault().getRawOffset() + 86400000);
	
		//System.out.print(ReadConfig.getProperties("kafka.topic"));
		
		//System.out.print(ReadConfig.getProperties("redis.article.feature"));
		
		//String items="234891237|{key:brand_42111,imp:7,click:0},{key:cate_3255,imp:7,click:0},{key:cate_189,imp:7,click:0},{key:cate_3271,imp:7,click:0},{key:cate_177,imp:7,click:0}|2019-05-14 14:55:00";
		//Map<String,String> item = JSONObject.parseObject(items, Map.class);
		
		//String[] key = item.get("key").split("_");
		//String imp = item.get("imp");
		//String click = item.get("click");
		
		//System.out.print(imp+click);
		
		//SimpleDateFormat simpleDateFormat = new SimpleDateFormat("n=yyyyMMddHHmm");
		//String hehe = simpleDateFormat.format(new Date()); 
		//System.out.print(hehe.substring(0, hehe.length()-1));
		
		
		//String a = "2019-05-14-142";
		//Date date = simpleDateFormat.parse(a);
        //long ts = date.getTime();
        //System.out.print(hehe);
		//String msg = "u1dimp_e412798ce0e96034b16f5f07f73d7e04|brand|{\"4231\":2,\"43878\":1,\"1683\":1,\"59942\":1,\"1933\":2,\"1259\":1,\"40781\":1,\"55\":1,\"59746\":1,\"1669\":1,\"1931\":1,\"26849\":1,\"36681\":1}";
		//String[] data = msg.split("\\|");
				
		//System.out.print(TimeZone.getDefault().getRawOffset());
		
		
		
		/*String propertyItemId="1234567890";
		String imp = "1";
		String click = "2";
		String s_click = "3";
		String data="{\"key\":\""+propertyItemId+"\",\"imp\":\""+imp+"\",\"click\":\""+click+"\",\"s_click\":\""+s_click+"\"},";
		System.out.print(data);*/
		
		//System.out.print(ReadConfig.getProperties("hdfs.prefix")+"/bi/app_ga/app_user_portrait_redis");
		
		String regex = "^[1-9]{1}[0-9]*$";
		String input= "102";
		Pattern p = Pattern.compile(regex);
		Matcher m = p.matcher("");
		System.out.println(m.matches());//false
		m.reset();
		System.out.println(m.find());//true
	
		
		
		
	}
	
}
