package org.nerds;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Utils {
    public static final String TOPIC = "trade_topic";

    public static String getEndpoint() throws IOException {

        InputStream is = Utils.class.getClassLoader().getResourceAsStream("credential.properties");
        Properties properties = new Properties();
        properties.load(is);
        final String endpoint = properties.getProperty("endpoint");
        if (endpoint == null || endpoint.isEmpty()) throw new RuntimeException("Server Enpoint is null");
        return endpoint;
    }
}
