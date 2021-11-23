package com.binarray.dev.kafka.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

/**
 * Utility class to be used for common file operations.
 *
 * @author Ashesh
 */
public class FileUtils {
    private static final Logger logger = LoggerFactory.getLogger(FileUtils.class);

    public static Properties loadProperties(String configFile) {
        var configProps = new Properties();
        try(var inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(configFile)) {
            configProps.load(inputStream);
        } catch (IOException ioEx) {
            logger.error("Error loading properties.", ioEx);
        }
        return configProps;
    }

}
