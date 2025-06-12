package smzdm.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Flink应用配置管理
 */
public class FlinkConfiguration {
    
    private static final Logger logger = LoggerFactory.getLogger(FlinkConfiguration.class);
    private final Properties properties;

    public FlinkConfiguration(String configPath) throws IOException {
        this.properties = loadProperties(configPath);
        validateConfiguration();
    }

    public FlinkConfiguration(Properties properties) {
        this.properties = properties;
        validateConfiguration();
    }

    private Properties loadProperties(String configPath) throws IOException {
        Properties props = new Properties();
        try (InputStream inputStream = getInputStream(configPath)) {
            props.load(inputStream);
            logger.info("Configuration loaded from: {}", configPath);
        }
        return props;
    }

    private InputStream getInputStream(String configPath) throws IOException {
        InputStream inputStream = getClass().getClassLoader().getResourceAsStream(configPath);
        if (inputStream == null) {
            throw new IOException("Configuration file not found: " + configPath);
        }
        return inputStream;
    }

    private void validateConfiguration() {
        String[] requiredKeys = {
            "bootstrap.servers", "group.id", "kafka.topic",
            "redis.article.detail", "redis.article.feature"
        };
        
        for (String key : requiredKeys) {
            String value = properties.getProperty(key);
            if (value == null || value.trim().isEmpty()) {
                throw new IllegalArgumentException("Required configuration missing: " + key);
            }
        }
    }

    // 基础getter方法
    public String getKafkaBootstrapServers() {
        return properties.getProperty("bootstrap.servers");
    }

    public String getKafkaGroupId() {
        return properties.getProperty("group.id");
    }

    public String getKafkaTopic() {
        return properties.getProperty("kafka.topic");
    }

    public int getWindowSizeMinutes() {
        return getIntProperty("flink.timeWindow.minutes.size", 5);
    }

    public String getTaskGroup() {
        return properties.getProperty("flink.task.group", "flink-stream-test");
    }

    public String getRedisArticleDetailHost() {
        return properties.getProperty("redis.article.detail");
    }

    public String getRedisArticleFeatureHost() {
        return properties.getProperty("redis.article.feature");
    }

    public int getRedisPort() {
        return getIntProperty("redis.port", 6379);
    }

    public Properties getProperties() {
        return new Properties(properties);
    }

    // 工具方法
    public int getIntProperty(String key, int defaultValue) {
        String value = properties.getProperty(key);
        if (value == null) return defaultValue;
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            logger.warn("Invalid integer value for {}: {}, using default: {}", key, value, defaultValue);
            return defaultValue;
        }
    }

    public long getLongProperty(String key, long defaultValue) {
        String value = properties.getProperty(key);
        if (value == null) return defaultValue;
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            logger.warn("Invalid long value for {}: {}, using default: {}", key, value, defaultValue);
            return defaultValue;
        }
    }

    public boolean getBooleanProperty(String key, boolean defaultValue) {
        String value = properties.getProperty(key);
        if (value == null) return defaultValue;
        return Boolean.parseBoolean(value.trim());
    }
} 