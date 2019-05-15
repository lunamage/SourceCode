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
import java.util.regex.Pattern;

import org.apache.flink.api.java.tuple.Tuple4;

import com.alibaba.fastjson.JSONObject;
public class test {
	@SuppressWarnings("unchecked")
	
	public static void main(String[] args) throws Exception {
		//long current = System.currentTimeMillis();
        //long zero = current/(1000*3600*24)*(1000*3600*24) - TimeZone.getDefault().getRawOffset();
        //System.out.print(zero);
		
		//String js2="{\"visitNumber\":\"1\",\"visitId\":\"1531708301\",\"visitStartTime\":\"1531708301\",\"date\":\"20180716\",\"totals\":{\"visits\":\"1\",\"hits\":\"1\",\"pageviews\":\"1\",\"bounces\":\"1\",\"newVisits\":\"1\",\"sessionQualityDim\":\"0\"},\"trafficSource\":{\"referralPath\":\"(not set)\",\"campaign\":\"(not set)\",\"source\":\"(direct)\",\"medium\":\"(none)\",\"keyword\":\"(not set)\",\"adContent\":\"(not set)\",\"adwordsClickInfo\":{},\"isTrueDirect\":true},\"device\":{\"browser\":\"Safari (in-app)\",\"browserVersion\":\"(not set)\",\"browserSize\":\"380x600\",\"operatingSystem\":\"iOS\",\"operatingSystemVersion\":\"11.4\",\"isMobile\":true,\"mobileDeviceBranding\":\"Apple\",\"mobileDeviceModel\":\"iPhone\",\"mobileInputSelector\":\"touchscreen\",\"mobileDeviceInfo\":\"Apple iPhone\",\"mobileDeviceMarketingName\":\"(not set)\",\"flashVersion\":\"(not set)\",\"javaEnabled\":false,\"language\":\"zh-cn\",\"screenColors\":\"32-bit\",\"screenResolution\":\"375x667\",\"deviceCategory\":\"mobile\"},\"geoNetwork\":{\"continent\":\"Asia\",\"subContinent\":\"Eastern Asia\",\"country\":\"China\",\"region\":\"Hunan\",\"metro\":\"(not set)\",\"city\":\"Changsha\",\"cityId\":\"1003541\",\"networkDomain\":\"unknown.unknown\",\"latitude\":\"28.2282\",\"longitude\":\"112.9388\",\"networkLocation\":\"chinanet hunan province network\"},\"customDimensions\":[{\"index\":\"8\",\"value\":\"Mozilla/5.0 (iPhone; CPU iPhone OS 11_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15F79\"}],\"hits\":[{\"hitNumber\":\"1\",\"time\":\"0\",\"hour\":\"10\",\"minute\":\"31\",\"isInteraction\":true,\"isEntrance\":true,\"isExit\":true,\"referer\":\"https://m.smzdm.com/fenlei/sushenku/\",\"page\":{\"pagePath\":\"/wiki.m.smzdm.com/p/egy971/\",\"hostname\":\"wiki.m.smzdm.com\",\"pageTitle\":\"MAIDENFORM Flexees 女士塑身无痕打底裤【价格 测评 怎么样】_什么值得买\",\"pagePathLevel1\":\"/wiki.m.smzdm.com/\",\"pagePathLevel2\":\"/p/\",\"pagePathLevel3\":\"/egy971/\",\"pagePathLevel4\":\"/\"},\"appInfo\":{\"screenName\":\"wiki.m.smzdm.com/wiki.m.smzdm.com/p/egy971/\",\"landingScreenName\":\"wiki.m.smzdm.com/wiki.m.smzdm.com/p/egy971/\",\"exitScreenName\":\"wiki.m.smzdm.com/wiki.m.smzdm.com/p/egy971/\",\"screenDepth\":\"0\"},\"exceptionInfo\":{\"isFatal\":true},\"product\":[{\"productSKU\":\"2403061\",\"v2ProductName\":\"MAIDENFORM Flexees  女士塑身无痕打底裤\",\"v2ProductCategory\":\"塑身裤/无/无/无\",\"productVariant\":\"(not set)\",\"productBrand\":\"MAIDENFORM\",\"productPrice\":\"159000000\",\"localProductPrice\":\"0\",\"customDimensions\":[],\"customMetrics\":[],\"productListName\":\"WAP\",\"productListPosition\":\"0\",\"productCouponCode\":\"(not set)\"}],\"promotion\":[],\"eCommerceAction\":{\"action_type\":\"2\",\"step\":\"1\"},\"experiment\":[],\"customVariables\":[],\"customDimensions\":[{\"index\":\"1\",\"value\":\"无\"},{\"index\":\"2\",\"value\":\"无\"},{\"index\":\"3\",\"value\":\"MAIDENFORM\"},{\"index\":\"4\",\"value\":\"无\"},{\"index\":\"5\",\"value\":\"无\"},{\"index\":\"6\",\"value\":\"塑身裤\"},{\"index\":\"7\",\"value\":\"无\"},{\"index\":\"8\",\"value\":\"Mozilla/5.0 (iPhone; CPU iPhone OS 11_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15F79\"},{\"index\":\"13\",\"value\":\"wiki\"},{\"index\":\"35\",\"value\":\"无\"},{\"index\":\"37\",\"value\":\"egy971\"},{\"index\":\"50\",\"value\":\"\"}],\"customMetrics\":[],\"type\":\"PAGE\",\"social\":{\"socialNetwork\":\"(not set)\",\"hasSocialSourceReferral\":\"No\",\"socialInteractionNetworkAction\":\" : \"},\"contentGroup\":{\"contentGroup1\":\"(not set)\",\"contentGroup2\":\"(not set)\",\"contentGroup3\":\"(not set)\",\"contentGroup4\":\"(not set)\",\"contentGroup5\":\"(not set)\",\"previousContentGroup1\":\"(entrance)\",\"previousContentGroup2\":\"(entrance)\",\"previousContentGroup3\":\"(entrance)\",\"previousContentGroup4\":\"(entrance)\",\"previousContentGroup5\":\"(entrance)\"},\"dataSource\":\"web\",\"publisher_infos\":[]}],\"fullVisitorId\":\"29845102206387085\",\"clientId\":\"6948854.1531708301\",\"channelGrouping\":\"Direct\",\"socialEngagementType\":\"Not Socially Engaged\"}";
		//SdkLog sdkLog = JSONObject.parseObject(js, SdkLog.class);
		
		//String js="{\"imei\":\"123\",\"aid\":\"com.smzdm.client.ios\",\"lng\":\"117.1219331955941\",\"sid\":\"15535788371060\",\"uid\":\"234891237\",\"tid\":\"UA-1000000-1\",\"an\":\"smzdm\",\"ca\":\"0\",\"sr\":\"1125x2436\",\"uuid\":\"39BEE7FF-AC4E-4048-9CDD-124676B0B73E\",\"ecp\":\"{\\\"a\\\":\\\"123456\\\",\\\"17\\\":\\\"a\\\",\\\"13\\\":\\\"a\\\",\\\"21\\\":\\\"首页_推荐_feed流_a_首页强制置顶\\\",\\\"20\\\":\\\"3\\\",\\\"11\\\":\\\"youhui\\\"}\",\"idfa\":\"1MgqjeT41eINeS+IAsBZE+kJt5H9fZBdkvcqjYaDTdSfofPO6MzmDg==\",\"type\":\"screenview\",\"ift\":\"0\",\"st\":\"iPhone\",\"sn\":\"iPhone\\/3\\/12967582\\/\",\"v\":\"1.10\",\"isnp\":\"1\",\"ip\":\"117.50.12.220\",\"sv\":\"1.1.0\",\"dt\":\"1\",\"ds\":\"app\",\"ch\":\"AppStore\",\"slt\":1553579109,\"av\":\"9.3.15\",\"it\":\"1553579081445\",\"cid\":\"VkF\\/KKe4KaP\\/oEDFDjcvOpRVnmK8FdfiyHmZNngjnTL1CSKIgp9s7g==.1552356749870\",\"lat\":\"39.09510244436872\",\"dm\":\"iPhone10,3\",\"did\":\"VkF\\/KKe4KaP\\/oEDFDjcvOpRVnmK8FdfiyHmZNngjnTL1CSKIgp9s7g==\",\"hnb\":\"54\",\"os\":\"12.1\",\"nt\":\"wifi\",\"idfv\":\"eUJ9EhWsh\\/SvKKbrkXrSYrHZhS0mpnkOgomb9kJfDgMkwUIWzBkUpg==\",\"ec\":\"01\"}";
		//Map<String,String> sdk = JSONObject.parseObject(js, Map.class);
		//Map<String,String> ecp = JSONObject.parseObject(sdk.get("ecp"), Map.class);
		//System.out.print(ecp.get("20"));
		
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
		String msg = "u1dimp_e412798ce0e96034b16f5f07f73d7e04|brand|{\"4231\":2,\"43878\":1,\"1683\":1,\"59942\":1,\"1933\":2,\"1259\":1,\"40781\":1,\"55\":1,\"59746\":1,\"1669\":1,\"1931\":1,\"26849\":1,\"36681\":1}";
		String[] data = msg.split("\\|");
				
		System.out.print(TimeZone.getDefault().getRawOffset());
		
		
		

        
		
		
		
	}
	
}
