package utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;


public class ReadConfig {

	public static String getProperties(String keyWord){
        Properties prop = new Properties();
        String value = null;
        try {
            InputStream inputStream = ReadConfig.class.getResourceAsStream("/config.properties");
            prop.load(inputStream);
            value = prop.getProperty(keyWord);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return value;
    }
	
	
}
