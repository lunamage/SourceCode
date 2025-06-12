package smzdm.utils;

import java.util.regex.Pattern;

/**
 * 通用工具类
 */
public class Utils {
    
    private static final Pattern NUMERIC_PATTERN = Pattern.compile("^\\d*[1-9]\\d*$");
    
    public static final String IMP = "imp";
    public static final String CLICK = "click";
    public static final String CATE = "cate";
    public static final String BRAND = "brand";
    
    public static boolean isNumeric(String string) {
        return string != null && NUMERIC_PATTERN.matcher(string).matches();
    }
}
