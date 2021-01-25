package utils;

import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class PropertiesUtil {
    public static Properties load(String propertieName) throws IOException {
        Properties prop = new Properties();
        prop.load(new InputStreamReader(Thread.currentThread().getContextClassLoader().getResourceAsStream(propertieName), StandardCharsets.UTF_8));
        return prop;

        /**
         * 如何使用
         * Properties property = PropertiesUtil.load("config.properties");
         * String server = property.getProperty("mysql.server");
         */
    }
}
