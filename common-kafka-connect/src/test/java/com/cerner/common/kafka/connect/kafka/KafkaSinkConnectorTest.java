package com.cerner.common.kafka.connect.kafka;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class KafkaSinkConnectorTest {

    private Map<String, String> startConfig;
    private Map<String, String> taskConfig;
    private KafkaSinkConnector connector;

    @Before
    public void before() {
        connector = new KafkaSinkConnector();

        startConfig = new HashMap<>();
        // This should be included because its required
        startConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "broker01:9092");

        // These should be included because it has the producer prefix
        startConfig.put(KafkaSinkConnector.PRODUCER_PREFIX + ProducerConfig.BATCH_SIZE_CONFIG, "123");
        startConfig.put(KafkaSinkConnector.PRODUCER_PREFIX + ProducerConfig.BUFFER_MEMORY_CONFIG, "321");

        // This should be ignored
        startConfig.put("other.config", "some-value");

        taskConfig = new HashMap<>();
        taskConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, startConfig.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        taskConfig.put(ProducerConfig.BATCH_SIZE_CONFIG, "123");
        taskConfig.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "321");
    }

    @Test
    public void start() {
        connector.start(startConfig);

        assertThat(connector.config, is(taskConfig));
    }

    @Test
    public void taskConfigs() {
        // Configures the connector
        connector.start(startConfig);

        assertThat(connector.taskConfigs(3), is(Arrays.asList(taskConfig, taskConfig, taskConfig)));
    }
}
