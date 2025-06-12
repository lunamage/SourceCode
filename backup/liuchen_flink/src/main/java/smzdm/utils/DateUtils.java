package smzdm.utils;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 日期工具类
 */
public class DateUtils {
    
    public static final String YYYYMMDD_HMS = "yyyy-MM-dd HH:mm:ss";
    public static final String YYYYMMDDHMS = "yyyyMMddHHmmss";

    /**
     * 格式化日期
     */
    public static String formatDate(Date date, String pattern) {
        if (date == null || pattern == null) {
            return null;
        }
        SimpleDateFormat sdf = new SimpleDateFormat(pattern);
        return sdf.format(date);
    }

    /**
     * 格式化日期（默认格式）
     */
    public static String formatDate(Date date) {
        return formatDate(date, YYYYMMDD_HMS);
    }
}
