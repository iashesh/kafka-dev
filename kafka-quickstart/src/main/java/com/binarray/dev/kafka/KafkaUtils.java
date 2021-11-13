package com.binarray.dev.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

/**
 * Utility class to be used by twitter producer and consumer.
 *
 * @author Ashesh
 */
public class KafkaUtils {
    private static final Logger logger = LoggerFactory.getLogger(KafkaUtils.class);

    public static Properties loadConfig(String configFile) {
        var twitterConfig = new Properties();
        try(var inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(configFile)) {
            twitterConfig.load(inputStream);
        } catch (IOException ioEx) {
            logger.error("Error loading properties.", ioEx);
        }
        return twitterConfig;
    }

}
