package test;


import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.apache.flink.shaded.guava18.com.google.common.base.Strings;

import java.util.TimeZone;

import com.alibaba.fastjson.JSONObject;

import utils.DateUtils;
import utils.ReadConfig;
import utils.Utils;

public class MessageSplitterTest {
	
	public static void main(String[] args) throws Exception {
		//System.out.println(System.getProperty("file.encoding"));
		Map<String, String> queryClickMapTmp = new HashMap<>();
		Map<String, Integer> queryClickRankMap = new HashMap<>();
		queryClickMapTmp.put("1_1", "1");
		queryClickMapTmp.put("1_2", "2");
		queryClickMapTmp.put("1_3", "3");
		queryClickMapTmp.put("1_4", "4");
		queryClickMapTmp.put("1_5", "5");
		
		queryClickRankMap = Utils.mapOrder(queryClickMapTmp);
		
		for(Map.Entry<String, Integer> entry : queryClickRankMap.entrySet()){
		    String mapKey = entry.getKey();
		    Integer mapValue = entry.getValue();
		    System.out.println(mapKey+":"+mapValue);
		}
   

  
	
	}
}