package utils;

import java.util.regex.Pattern;

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
}
