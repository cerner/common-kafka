package com.cerner.common.kafka.consumer;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ProcessingConfigTest {

    private Properties properties;
    private ProcessingConfig config;

    @BeforeEach
    public void before() {
        properties = new Properties();
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.EARLIEST.toString().toLowerCase());
        config = new ProcessingConfig(properties);
    }

    @Test
    public void constructor_nullProperties() {
        assertThrows(IllegalArgumentException.class,
                () -> new ProcessingConfig(null));
    }

    @Test
    public void constructor_offsetResetStrategyNotProvided() {
        properties.remove(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG);
        assertThrows(IllegalArgumentException.class,
                () -> new ProcessingConfig(properties));
    }

    @Test
    public void constructor_offsetResetStrategySetToNone() {
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
        assertThrows(IllegalArgumentException.class,
                () -> new ProcessingConfig(properties));
    }

    @Test
    public void constructor_commitInitialOffsetNotBoolean() {
        properties.setProperty(ProcessingConfig.COMMIT_INITIAL_OFFSET_PROPERTY, "not_boolean");
        assertThrows(IllegalArgumentException.class,
                () -> new ProcessingConfig(properties));
    }

    @Test
    public void constructor_commitSizeEqualsZero() {
        properties.setProperty(ProcessingConfig.COMMIT_SIZE_THRESHOLD_PROPERTY, "0");
        assertThrows(IllegalArgumentException.class,
                () -> new ProcessingConfig(properties));
    }

    @Test
    public void constructor_commitSizeLessThanZero() {
        properties.setProperty(ProcessingConfig.COMMIT_SIZE_THRESHOLD_PROPERTY, "-1");
        assertThrows(IllegalArgumentException.class,
                () -> new ProcessingConfig(properties));
    }

    @Test
    public void constructor_commitSizeNotANumber() {
        properties.setProperty(ProcessingConfig.COMMIT_SIZE_THRESHOLD_PROPERTY, "notANumber");
        assertThrows(IllegalArgumentException.class,
                () -> new ProcessingConfig(properties));
    }

    @Test
    public void constructor_commitTimeLessThanZero() {
        properties.setProperty(ProcessingConfig.COMMIT_TIME_THRESHOLD_PROPERTY, "-1");
        assertThrows(IllegalArgumentException.class,
                () -> new ProcessingConfig(properties));
    }

    @Test
    public void constructor_commitTimeNotANumber() {
        properties.setProperty(ProcessingConfig.COMMIT_TIME_THRESHOLD_PROPERTY, "notANumber");
        assertThrows(IllegalArgumentException.class,
                () -> new ProcessingConfig(properties));
    }

    @Test
    public void constructor_pauseThresholdLessThanZero() {
        properties.setProperty(ProcessingConfig.FAIL_THRESHOLD_PROPERTY, "-1");
        assertThrows(IllegalArgumentException.class,
                () -> new ProcessingConfig(properties));
    }

    @Test
    public void constructor_pauseThresholdGreaterThanOne() {
        properties.setProperty(ProcessingConfig.FAIL_THRESHOLD_PROPERTY, "2");
        assertThrows(IllegalArgumentException.class,
                () -> new ProcessingConfig(properties));
    }

    @Test
    public void constructor_pauseThresholdEqualsZero() {
        properties.setProperty(ProcessingConfig.FAIL_THRESHOLD_PROPERTY, "0");
        config = new ProcessingConfig(properties);
        assertThat(config.getFailThreshold(), is(0.0));
    }

    @Test
    public void constructor_pauseThresholdEqualsOne() {
        properties.setProperty(ProcessingConfig.FAIL_THRESHOLD_PROPERTY, "1");
        config = new ProcessingConfig(properties);
        assertThat(config.getFailThreshold(), is(1.0));
    }

    @Test
    public void constructor_pauseThresholdNotANumber() {
        properties.setProperty(ProcessingConfig.FAIL_THRESHOLD_PROPERTY, "notANumber");
        assertThrows(IllegalArgumentException.class,
                () -> new ProcessingConfig(properties));
    }

    @Test
    public void constructor_pauseSampleSizeEqualsZero() {
        properties.setProperty(ProcessingConfig.FAIL_SAMPLE_SIZE_PROPERTY, "0");
        assertThrows(IllegalArgumentException.class,
                () -> new ProcessingConfig(properties));
    }

    @Test
    public void constructor_pauseSampleSizeLessThanZero() {
        properties.setProperty(ProcessingConfig.FAIL_SAMPLE_SIZE_PROPERTY, "-1");
        assertThrows(IllegalArgumentException.class,
                () -> new ProcessingConfig(properties));
    }

    @Test
    public void constructor_pauseSampleSizeNotANumber() {
        properties.setProperty(ProcessingConfig.FAIL_SAMPLE_SIZE_PROPERTY, "notANumber");
        assertThrows(IllegalArgumentException.class,
                () -> new ProcessingConfig(properties));
    }

    @Test
    public void constructor_pauseTimeLessThanZero() {
        properties.setProperty(ProcessingConfig.FAIL_PAUSE_TIME_PROPERTY, "-1");
        assertThrows(IllegalArgumentException.class,
                () -> new ProcessingConfig(properties));
    }

    @Test
    public void constructor_pauseTimeNotANumber() {
        properties.setProperty(ProcessingConfig.FAIL_PAUSE_TIME_PROPERTY, "notANumber");
        assertThrows(IllegalArgumentException.class,
                () -> new ProcessingConfig(properties));
    }

    @Test
    public void constructor_maxPollIntervalNotANumber() {
        properties.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "notANumber");
        assertThrows(IllegalArgumentException.class,
                () -> new ProcessingConfig(properties));
    }

    @Test
    public void constructor_defaults() {
        assertThat(config.getCommitInitialOffset(), is(true));
        assertThat(config.getCommitSizeThreshold(), is(Long.parseLong(ProcessingConfig.COMMIT_SIZE_THRESHOLD_DEFAULT)));
        assertThat(config.getCommitTimeThreshold(), is(Long.parseLong(ProcessingConfig.COMMIT_TIME_THRESHOLD_DEFAULT)));
        assertThat(config.getFailPauseTime(), is(Long.parseLong(ProcessingConfig.FAIL_PAUSE_TIME_DEFAULT)));
        assertThat(config.getFailSampleSize(), is(Integer.parseInt(ProcessingConfig.FAIL_SAMPLE_SIZE_DEFAULT)));
        assertThat(config.getFailThreshold(), is(Double.parseDouble(ProcessingConfig.FAIL_THRESHOLD_DEFAULT)));
        assertThat(config.getOffsetResetStrategy(), is(OffsetResetStrategy.EARLIEST));
        assertThat(config.getMaxPollInterval(), is(300000L));

        // config properties should at least contain everything from properties
        for(Map.Entry<Object, Object> entry : properties.entrySet()) {
            assertThat(config.getProperties().get(entry.getKey()), is(entry.getValue()));
        }

        assertThat(config.getProperties().getProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG), is(Boolean.FALSE.toString()));
    }

    @Test
    public void constructor_customProperties() {
        properties.setProperty(ProcessingConfig.COMMIT_INITIAL_OFFSET_PROPERTY, String.valueOf(false));
        properties.setProperty(ProcessingConfig.COMMIT_SIZE_THRESHOLD_PROPERTY, "123");
        properties.setProperty(ProcessingConfig.COMMIT_TIME_THRESHOLD_PROPERTY, "234");
        properties.setProperty(ProcessingConfig.FAIL_PAUSE_TIME_PROPERTY, "456");
        properties.setProperty(ProcessingConfig.FAIL_SAMPLE_SIZE_PROPERTY, "567");
        properties.setProperty(ProcessingConfig.FAIL_THRESHOLD_PROPERTY, "0.1");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.LATEST.toString().toLowerCase());
        properties.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "10000");

        config = new ProcessingConfig(properties);

        assertThat(config.getCommitInitialOffset(), is(false));
        assertThat(config.getCommitSizeThreshold(), is(123L));
        assertThat(config.getCommitTimeThreshold(), is(234L));
        assertThat(config.getFailPauseTime(), is(456L));
        assertThat(config.getFailSampleSize(), is(567));
        assertThat(config.getFailThreshold(), is(0.1));
        assertThat(config.getOffsetResetStrategy(), is(OffsetResetStrategy.LATEST));
        assertThat(config.getMaxPollInterval(), is(10000L));

        // config properties should at least contain everything from properties
        for(Map.Entry<Object, Object> entry : properties.entrySet()) {
            assertThat(config.getProperties().get(entry.getKey()), is(entry.getValue()));
        }

        assertThat(config.getProperties().getProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG), is(Boolean.FALSE.toString()));
    }

}
