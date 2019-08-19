package test;


import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
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
import java.util.TimeZone;

import com.alibaba.fastjson.JSONObject;

import utils.DateUtils;
import utils.ReadConfig;
import utils.Utils;

public class MessageSplitterTest {
	
	public static void main(String[] args) throws Exception {
    
	    Map<String, String> map = new HashMap<String, String>();
        map.put("c", "0.123");
        map.put("a", "0.122");
        map.put("b", "0.122");
        map.put("d", "0.122");
        map.put("e", "0.122");
        map.put("f", "0.121");
        map.put("g", "0.121");
        
        //HashMap<String,Integer> m = new HashMap<>();
		//List<Integer> t = new ArrayList<>();
        
        //HashMap<String, Double> newmap = new HashMap<String, Double>();
        
        //newmap = Utils.mapRatio(map);

        HashMap<String,Integer> t = Utils.mapOrder(map);
        for(Map.Entry<String, Integer> entry : t.entrySet()){
            String mapKey = entry.getKey();
            Integer mapValue = entry.getValue();
            System.out.println(mapKey+":"+mapValue);
        }
        
     String a = "通用_0";
     
     System.out.println(a);
     
     
	 if(a.matches("^1|2|5|21$")) {
		 System.out.println("123");
	 }
	 
	
	}	
}
