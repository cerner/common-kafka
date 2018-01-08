package com.cerner.common.kafka.connect;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Class used to determine the version of the project
 */
// This is used in our connectors/sinks to display the version of the connector
public class Version {

    private static final Logger LOGGER = LoggerFactory.getLogger(Version.class);

    private static String version = "unknown";

    static {
        try {
            Properties props = new Properties();
            try (InputStream stream = Version.class.getResourceAsStream("/common-kafka-connect.properties")) {
                props.load(stream);
            }
            version = props.getProperty("version", version).trim();
        } catch (IOException | RuntimeException e) {
            LOGGER.warn("Error while loading version:", e);
        }
    }

    /**
     * @return the version of the project
     */
    public static String getVersion() {
        return version;
    }
}
