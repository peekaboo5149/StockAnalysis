package org.nerds;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Utils {
    public static final String TOPIC = "trade_topic";
    private static final Properties properties = new Properties();
    private static final Logger logger = LoggerFactory.getLogger(Utils.class);

    static {
        try {
            InputStream is = Utils.class.getClassLoader().getResourceAsStream("credential.properties");
            properties.load(is);
        } catch (IOException e) {
            logger.error("", e);
        }
    }

    public static String getEndpoint() throws IOException {
        return getProperty("endpoint", "Server Endpoint is null");
    }

    private static String getProperty(String key, String errorMessage) {
        final String endpoint = properties.getProperty(key);
        if (endpoint == null || endpoint.isEmpty()) throw new RuntimeException(errorMessage);
        return endpoint;
    }

    public static String getBrokerString() {
        return getProperty("broker", "Broker String not provided");
    }
}
