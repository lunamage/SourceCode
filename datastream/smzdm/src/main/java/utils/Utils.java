package utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import com.alibaba.fastjson.JSONObject;

/**
 * @author: liuchen
 * @date: 2019/1/16
 * @time: 2:35 PM
 * @Description:
 */
public class Utils {

    private final static Pattern pattern = Pattern.compile("^\\d*[1-9]\\d*$");

    public final static String IMP = "imp";
    public final static String CLICK = "click";

    // 品类
    public final static String CATE = "cate";
    // 品牌
    public final static String BRAND = "brand";

    public static boolean isNumeric(String string) {
        return string != null && pattern.matcher(string).matches();
    }
    
    //推荐商城范围
	public static boolean isContainsMall(String content){
		Set<String> set = new HashSet<String>();
		set.add("183");//京东商城
		set.add("3949");//海淘全球
		set.add("247");//天猫
		set.add("2537");//天猫超市
		set.add("243");//淘宝
		set.add("8645");//拼多多
		set.add("239");//苏宁
		set.add("3981");//中亚
		set.add("5108");//网易考拉
		set.add("8912");//小米有品
		set.add("167");//国美
		
		return set.contains(content);
	}
	
	//输入搜索位置  返回修正值
	public static Double getQueryPositionValue(String key) throws NumberFormatException, IOException{
		InputStream inputStream = ReadConfig.class.getResourceAsStream("/queryPositionConfig.txt");
    	String line=null;
    	HashMap<String,Double> map=new HashMap<String,Double>();
    	BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
    	
    	while ((line = br.readLine()) != null) {
    		String[] m = line.split(",");
    		map.put(m[0], Double.valueOf(m[1]));
    	}    	
		return map.get(key);
	}
	
	//map order
	public static HashMap<String,Integer> mapOrder(Map map){
		HashMap<String,Integer> m = new HashMap<>();
		
	    List<Map.Entry<String,String>> list = new ArrayList<Map.Entry<String,String>>(map.entrySet());
        Collections.sort(list,new Comparator<Map.Entry<String,String>>() {
            //升序排序
            public int compare(Entry<String, String> o1,Entry<String, String> o2) {
                return Double.valueOf(o2.getValue()).compareTo(Double.valueOf(o1.getValue()));
            }
        });
        
        Integer i=0;
        String value=null;
        for(Map.Entry<String,String> mapping:list){ 
        	if(!mapping.getValue().equals(value)) {
        		   i+=1;
        	   }
               value=mapping.getValue();
               m.put(mapping.getKey(), i);
          } 
		
		return m;
	}
	
	//mapRatio
	public static HashMap<String,Double> mapRatio(Map<String, String> map){
	    Integer total = 0;
	        
        for(String value : map.values()){
        	total += Integer.valueOf(value);
        }
	        
	    HashMap<String, Double> newmap = new HashMap<String, Double>();
	    DecimalFormat df = new DecimalFormat("0.0000");
	        
        for(Map.Entry<String, String> entry : map.entrySet()){
            String mapKey = entry.getKey();
            Integer mapValue = Integer.valueOf(entry.getValue());
            newmap.put(mapKey, Double.valueOf(df.format((float) mapValue/total)));
        }
		
		return newmap;
	}
	
    public static boolean isJson(String content){
	  try {
		  JSONObject.parseObject(content);
		  return true;
	  	} catch (Exception e) {
	  		return false;
	  	}
	}
}
