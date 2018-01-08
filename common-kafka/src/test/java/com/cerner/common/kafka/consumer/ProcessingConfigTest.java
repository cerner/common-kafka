package com.cerner.common.kafka.consumer;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.junit.Before;
import org.junit.Test;

public class ProcessingConfigTest {

    private Properties properties;
    private ProcessingConfig config;

    @Before
    public void before() {
        properties = new Properties();
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.EARLIEST.toString().toLowerCase());
        config = new ProcessingConfig(properties);
    }

    @Test(expected = IllegalArgumentException.class)
    public void constructor_nullProperties() throws IOException {
        new ProcessingConfig(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void constructor_offsetResetStrategyNotProvided() throws IOException {
        properties.remove(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG);
        new ProcessingConfig(properties);
    }

    @Test(expected = IllegalArgumentException.class)
    public void constructor_offsetResetStrategySetToNone() throws IOException {
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
        new ProcessingConfig(properties);
    }

    @Test(expected = IllegalArgumentException.class)
    public void constructor_commitInitialOffsetNotBoolean() throws IOException {
        properties.setProperty(ProcessingConfig.COMMIT_INITIAL_OFFSET_PROPERTY, "not_boolean");
        new ProcessingConfig(properties);
    }

    @Test(expected = IllegalArgumentException.class)
    public void constructor_commitSizeEqualsZero() throws IOException {
        properties.setProperty(ProcessingConfig.COMMIT_SIZE_THRESHOLD_PROPERTY, "0");
        new ProcessingConfig(properties);
    }

    @Test(expected = IllegalArgumentException.class)
    public void constructor_commitSizeLessThanZero() throws IOException {
        properties.setProperty(ProcessingConfig.COMMIT_SIZE_THRESHOLD_PROPERTY, "-1");
        new ProcessingConfig(properties);
    }

    @Test(expected = IllegalArgumentException.class)
    public void constructor_commitSizeNotANumber() throws IOException {
        properties.setProperty(ProcessingConfig.COMMIT_SIZE_THRESHOLD_PROPERTY, "notANumber");
        new ProcessingConfig(properties);
    }

    @Test (expected = IllegalArgumentException.class)
    public void constructor_commitTimeLessThanZero() throws IOException {
        properties.setProperty(ProcessingConfig.COMMIT_TIME_THRESHOLD_PROPERTY, "-1");
        new ProcessingConfig(properties);
    }

    @Test (expected = IllegalArgumentException.class)
    public void constructor_commitTimeNotANumber() throws IOException {
        properties.setProperty(ProcessingConfig.COMMIT_TIME_THRESHOLD_PROPERTY, "notANumber");
        new ProcessingConfig(properties);
    }

    @Test(expected = IllegalArgumentException.class)
    public void constructor_pauseThresholdLessThanZero() throws IOException {
        properties.setProperty(ProcessingConfig.FAIL_THRESHOLD_PROPERTY, "-1");
        new ProcessingConfig(properties);
    }

    @Test(expected = IllegalArgumentException.class)
    public void constructor_pauseThresholdGreaterThanOne() throws IOException {
        properties.setProperty(ProcessingConfig.FAIL_THRESHOLD_PROPERTY, "2");
        new ProcessingConfig(properties);
    }

    @Test
    public void constructor_pauseThresholdEqualsZero() throws IOException {
        properties.setProperty(ProcessingConfig.FAIL_THRESHOLD_PROPERTY, "0");
        config = new ProcessingConfig(properties);
        assertThat(config.getFailThreshold(), is(0.0));
    }

    @Test
    public void constructor_pauseThresholdEqualsOne() throws IOException {
        properties.setProperty(ProcessingConfig.FAIL_THRESHOLD_PROPERTY, "1");
        config = new ProcessingConfig(properties);
        assertThat(config.getFailThreshold(), is(1.0));
    }

    @Test (expected = IllegalArgumentException.class)
    public void constructor_pauseThresholdNotANumber() throws IOException {
        properties.setProperty(ProcessingConfig.FAIL_THRESHOLD_PROPERTY, "notANumber");
        new ProcessingConfig(properties);
    }

    @Test (expected = IllegalArgumentException.class)
    public void constructor_pauseSampleSizeEqualsZero() throws IOException {
        properties.setProperty(ProcessingConfig.FAIL_SAMPLE_SIZE_PROPERTY, "0");
        new ProcessingConfig(properties);
    }

    @Test (expected = IllegalArgumentException.class)
    public void constructor_pauseSampleSizeLessThanZero() throws IOException {
        properties.setProperty(ProcessingConfig.FAIL_SAMPLE_SIZE_PROPERTY, "-1");
        new ProcessingConfig(properties);
    }

    @Test (expected = IllegalArgumentException.class)
    public void constructor_pauseSampleSizeNotANumber() throws IOException {
        properties.setProperty(ProcessingConfig.FAIL_SAMPLE_SIZE_PROPERTY, "notANumber");
        new ProcessingConfig(properties);
    }

    @Test(expected = IllegalArgumentException.class)
    public void constructor_pauseTimeLessThanZero() throws IOException {
        properties.setProperty(ProcessingConfig.FAIL_PAUSE_TIME_PROPERTY, "-1");
        new ProcessingConfig(properties);
    }

    @Test(expected = IllegalArgumentException.class)
    public void constructor_pauseTimeNotANumber() throws IOException {
        properties.setProperty(ProcessingConfig.FAIL_PAUSE_TIME_PROPERTY, "notANumber");
        new ProcessingConfig(properties);
    }

    @Test(expected = IllegalArgumentException.class)
    public void constructor_maxPollIntervalNotANumber() throws IOException {
        properties.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "notANumber");
        new ProcessingConfig(properties);
    }

    @Test
    public void constructor_defaults() throws IOException {
        assertTrue(config.getCommitInitialOffset());
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
    public void constructor_customProperties() throws IOException {
        properties.setProperty(ProcessingConfig.COMMIT_INITIAL_OFFSET_PROPERTY, String.valueOf(false));
        properties.setProperty(ProcessingConfig.COMMIT_SIZE_THRESHOLD_PROPERTY, "123");
        properties.setProperty(ProcessingConfig.COMMIT_TIME_THRESHOLD_PROPERTY, "234");
        properties.setProperty(ProcessingConfig.FAIL_PAUSE_TIME_PROPERTY, "456");
        properties.setProperty(ProcessingConfig.FAIL_SAMPLE_SIZE_PROPERTY, "567");
        properties.setProperty(ProcessingConfig.FAIL_THRESHOLD_PROPERTY, "0.1");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.LATEST.toString().toLowerCase());
        properties.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "10000");

        config = new ProcessingConfig(properties);

        assertFalse(config.getCommitInitialOffset());
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
