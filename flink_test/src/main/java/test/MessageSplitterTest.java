package test;


import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
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
		
		String a ="手机";
		
		System.out.print(Utils.getMD5(a));
    
		
	}	
}
