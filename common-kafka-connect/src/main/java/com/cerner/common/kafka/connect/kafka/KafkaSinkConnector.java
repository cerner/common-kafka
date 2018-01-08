package com.cerner.common.kafka.connect.kafka;

import com.cerner.common.kafka.connect.Version;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * A Kafka sink connector which forwards the configured data to another Kafka cluster
 */
public class KafkaSinkConnector extends SinkConnector {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSinkConnector.class);

    public static final String PRODUCER_PREFIX = "producer.";

    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
                    "Bootstrap brokers for mirror'ed cluster");

    // Visible for testing
    protected Map<String, String> config;

    @Override
    public String version() {
        return Version.getVersion();
    }

    @Override
    public void start(Map<String, String> taskConfig) {
        config = new HashMap<>();

        taskConfig.forEach((key, value) -> {
            if (key.startsWith(PRODUCER_PREFIX)) {
                config.put(key.substring(PRODUCER_PREFIX.length()), value);
            }
        });

        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, taskConfig.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        config = Collections.unmodifiableMap(config);

        LOGGER.debug("Using {} for config", config);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return KafkaSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int totalConfigs) {
        return IntStream.range(0, totalConfigs).mapToObj(i -> config).collect(Collectors.toList());
    }

    @Override
    public void stop() {
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }
}
