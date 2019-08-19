package test;

import java.text.DecimalFormat;

import utils.Utils;

public class dataStreamTest {
	
    @SuppressWarnings("resource")
	public static void main(String[] args) throws Exception {
    	
    	//System.out.println(Utils.getQueryPositionValue(null));
    	
    	DecimalFormat df = new DecimalFormat("#.00");
        String correctSl = String.valueOf(df.format(121.2323232));
        System.out.println(correctSl);
    	
     }
}



