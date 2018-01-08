package com.cerner.common.kafka.consumer;

import java.util.Locale;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Configuration object used for Kafka
 */
public class ProcessingConfig {

    /**
     * Logger
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(ProcessingConfig.class);

    /**
     * Commit the initial offset for a new consumer
     */
    public static final String COMMIT_INITIAL_OFFSET_PROPERTY = "processing.kafka.commit.initial.offset";

    /**
     * The default value for the {@link #COMMIT_INITIAL_OFFSET_PROPERTY}.
     */
    public static final String COMMIT_INITIAL_OFFSET_DEFAULT = String.valueOf(true);

    /**
     * The amount of time in milliseconds to wait before committing eligible offsets
     */
    public static final String COMMIT_TIME_THRESHOLD_PROPERTY = "processing.kafka.commit.time.threshold";

    /**
     * The default amount of time to wait before committing offsets (60s)
     */
    public static final String COMMIT_TIME_THRESHOLD_DEFAULT = Long.toString(60000L);

    /**
     * The maximum number of messages that can be processed but not committed per partition. In other words this will
     * trigger a commit if a partition has the configured value or more messages processed but uncommitted
     */
    public static final String COMMIT_SIZE_THRESHOLD_PROPERTY = "processing.kafka.commit.size.threshold";

    /**
     *  The default maximum number of messages that can be processed but not committed per partition (500L)
     */
    public static final String COMMIT_SIZE_THRESHOLD_DEFAULT = Long.toString(500L);

    /**
     * The threshold to pause a partition's progress when this percentage of processing or higher is failures. The value
     * should be between [0, 1].
     */
    public static final String FAIL_THRESHOLD_PROPERTY = "processing.kafka.fail.threshold";

    /**
     * The default value for the threshold to pause a partition's progress if enough failures have occurred. Default is
     * 0.5 which is equivalent to a 50% failure rate
     */
    public static final String FAIL_THRESHOLD_DEFAULT = Double.toString(0.5);

    /**
     * The number of samples to consider when calculating the percentage of recently failed records
     */
    public static final String FAIL_SAMPLE_SIZE_PROPERTY = "processing.kafka.fail.sample.size";

    /**
     * The default value for the fail sample size (25 results)
     */
    public static final String FAIL_SAMPLE_SIZE_DEFAULT = Integer.toString(25);

    /**
     * The amount of time in ms to pause a partition when the fail threshold has been met
     */
    public static final String FAIL_PAUSE_TIME_PROPERTY = "processing.kafka.fail.pause.time";

    /**
     * The default amount of time to pause a partition when the fail threshold has been met (10s)
     */
    public static final String FAIL_PAUSE_TIME_DEFAULT = Long.toString(10000);

    /**
     * If initial offsets for a new consumer should be committed
     */
    private final boolean commitInitialOffset;

    /**
     * The amount of time in milliseconds to wait before committing offsets
     */
    protected final long commitTimeThreshold;

    /**
     * The number of messages to wait before committing offsets
     */
    protected final long commitSizeThreshold;

    /**
     * The offset reset strategy for the consumer
     */
    private final OffsetResetStrategy offsetResetStrategy;

    /**
     * The percentage of failures allowed before pausing a partition
     */
    private final double failThreshold;

    /**
     * The number of results to consider when calculating the percentage of successful processed records
     */
    private final int failSampleSize;

    /**
     * The amount of time to pause a partition for
     */
    private final long failPauseTime;

    /**
     * The consumer's max poll interval
     */
    private final long maxPollInterval;

    /**
     * The raw properties configured
     */
    private final Properties properties;

    /**
     * Creates a new processing config object
     *
     * <p>
     * NOTE: You must provide a value for {@link ConsumerConfig#AUTO_OFFSET_RESET_CONFIG} in addition to the requirements
     * of {@link org.apache.kafka.clients.consumer.KafkaConsumer}
     * </p>
     *
     * @param properties
     *          the configuration used by the consumer
     * @throws IllegalArgumentException
     *          <ul>
     *              <li>properties is {@code null}</li>
     *              <li>a value for {@link ConsumerConfig#AUTO_OFFSET_RESET_CONFIG} was not provided</li>
     *              <li>the value for {@link ConsumerConfig#AUTO_OFFSET_RESET_CONFIG} is 'none'</li>
     *              <li>if the value of {@link ConsumerConfig#AUTO_OFFSET_RESET_CONFIG} was invalid</li>
     *              <li>if the value of {@link ConsumerConfig#MAX_POLL_INTERVAL_MS_CONFIG} was invalid</li>
     *              <li>{@link #COMMIT_TIME_THRESHOLD_PROPERTY} is &lt; 0</li>
     *              <li>{@link #COMMIT_SIZE_THRESHOLD_PROPERTY} is &le; 0</li>
     *              <li>{@link #FAIL_THRESHOLD_PROPERTY} is &lt; 0 or &gt; 1</li>
     *              <li>{@link #FAIL_SAMPLE_SIZE_PROPERTY} is &le; 0</li>
     *              <li>{@link #FAIL_PAUSE_TIME_PROPERTY} is &lt; 0</li>
     *              <li>if any of the numeric properties are not valid numbers</li>
     *          </ul>
     */
    public ProcessingConfig(Properties properties) {
        if (properties == null)
            throw new IllegalArgumentException("properties cannot be null");

        Properties configProperties = new Properties();
        configProperties.putAll(properties);

        // This cannot be enabled otherwise it will break the guarantees of this processing consumer
        configProperties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        this.properties = configProperties;
        this.commitInitialOffset = parseBoolean(COMMIT_INITIAL_OFFSET_PROPERTY,
                properties.getProperty(COMMIT_INITIAL_OFFSET_PROPERTY, COMMIT_INITIAL_OFFSET_DEFAULT));
        this.commitTimeThreshold = parseLong(COMMIT_TIME_THRESHOLD_PROPERTY,
                properties.getProperty(COMMIT_TIME_THRESHOLD_PROPERTY, COMMIT_TIME_THRESHOLD_DEFAULT));
        this.commitSizeThreshold = parseLong(COMMIT_SIZE_THRESHOLD_PROPERTY,
                properties.getProperty(COMMIT_SIZE_THRESHOLD_PROPERTY, COMMIT_SIZE_THRESHOLD_DEFAULT));
        this.failThreshold = parseDouble(FAIL_THRESHOLD_PROPERTY, properties.getProperty(FAIL_THRESHOLD_PROPERTY,
                FAIL_THRESHOLD_DEFAULT));
        this.failSampleSize = parseInt(FAIL_SAMPLE_SIZE_PROPERTY, properties.getProperty(FAIL_SAMPLE_SIZE_PROPERTY,
                FAIL_SAMPLE_SIZE_DEFAULT));
        this.failPauseTime = parseLong(FAIL_PAUSE_TIME_PROPERTY, properties.getProperty(FAIL_PAUSE_TIME_PROPERTY,
                FAIL_PAUSE_TIME_DEFAULT));
        this.maxPollInterval = parseLong(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, properties.getProperty(
                ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "300000"));

        if (commitTimeThreshold < 0L)
            throw new IllegalArgumentException(COMMIT_TIME_THRESHOLD_PROPERTY + " cannot be < 0. Value: " +
                    commitTimeThreshold);

        if (commitSizeThreshold <= 0L)
            throw new IllegalArgumentException(COMMIT_SIZE_THRESHOLD_PROPERTY + " cannot be <= 0. Value: " +
                    commitSizeThreshold);

        if (failThreshold < 0)
            throw new IllegalArgumentException(FAIL_THRESHOLD_PROPERTY + " cannot be < 0. Value: " + failThreshold);

        if (failThreshold > 1)
            throw new IllegalArgumentException(FAIL_THRESHOLD_PROPERTY + " cannot be > 1. Value: " + failThreshold);

        if (failSampleSize <= 0)
            throw new IllegalArgumentException(FAIL_SAMPLE_SIZE_PROPERTY + " cannot be <= 0. Value: " + failSampleSize);

        if (failPauseTime < 0)
            throw new IllegalArgumentException(FAIL_PAUSE_TIME_PROPERTY + " cannot be < 0. Value: " + failPauseTime);

        String offsetResetStrategy = properties.getProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG);

        if (offsetResetStrategy == null)
            throw new IllegalArgumentException("A value for " + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG +
                    " must be provided in the config");

        this.offsetResetStrategy = OffsetResetStrategy.valueOf(offsetResetStrategy.toUpperCase(Locale.ENGLISH));

        if (this.offsetResetStrategy.equals(OffsetResetStrategy.NONE))
            throw new IllegalArgumentException("The offset reset strategy 'none' is not valid. Must be either 'earliest'"
                    +  " or 'latest'");

        LOGGER.debug("Config {}", toString());
    }

    private static double parseDouble(String property, String value) {
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Unable to parse config [" + property + "] as value [" + value +
                    "] was not a valid number", e);
        }
    }

    private static long parseLong(String property, String value) {
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Unable to parse config [" + property + "] as value [" + value +
                    "] was not a valid number", e);
        }
    }

    private static int parseInt(String property, String value) {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Unable to parse config [" + property + "] as value [" + value +
                    "] was not a valid number", e);
        }
    }

    private static boolean parseBoolean(String property, String value) {
        if (Boolean.TRUE.toString().equalsIgnoreCase(value) || Boolean.FALSE.toString().equalsIgnoreCase(value)) {
            return Boolean.parseBoolean(value);
        }

        throw new IllegalArgumentException("Unable to parse config [" + property + "] as value [" + value + "] was not a valid "
                + "boolean");
    }

    /**
     * If initial offsets for a new consumer should be committed
     *
     * @return {@code true} if initial offsets for a new consumer should be committed, {@code false} otherwise
     */
    public boolean getCommitInitialOffset() {
        return commitInitialOffset;
    }

    /**
     * The threshold for when we should attempt to commit if enough time has passed since our last commit (in ms)
     *
     * @return threshold for when we should attempt to commit if enough time has passed since our last commit (in ms)
     */
    public long getCommitTimeThreshold() {
        return commitTimeThreshold;
    }

    /**
     * The threshold per partition for when we should attempt to commit if we have at least this many commits eligible
     * for commit
     *
     * @return The threshold per partition for when we should attempt to commit if we have at least this many commits
     * eligible for commit
     */
    public long getCommitSizeThreshold() {
        return commitSizeThreshold;
    }

    /**
     * The offset strategy used by the consumer
     *
     * @return offset strategy used by the consumer
     */
    public OffsetResetStrategy getOffsetResetStrategy() {
        return offsetResetStrategy;
    }

    /**
     * The failure threshold in percent [0, 1] in which if our processing failures go above that percentage we will pause
     * processing for that partition for a time
     *
     * @return the failure threshold in percent [0, 1]
     */
    public double getFailThreshold() {
        return failThreshold;
    }

    /**
     * The sample size used in calculating the percentage of processing failures
     *
     * @return sample size used in calculating the percentage of processing failures
     */
    public int getFailSampleSize() {
        return failSampleSize;
    }

    /**
     * The amount of time in ms we should pause a partition for if it has reached the {@link #getFailThreshold()}
     *
     * @return The amount of time in ms we should pause a partition for
     */
    public long getFailPauseTime() {
        return failPauseTime;
    }

    /**
     * The consumer's max poll interval
     *
     * @return the consumer's max poll interval
     */
    public long getMaxPollInterval() {
        return maxPollInterval;
    }

    /**
     * The properties used to configure the consumer
     *
     * @return properties used to configure the consumer
     */
    public Properties getProperties() {
        return properties;
    }

    /**
     * Returns the Kafka consumer group id
     *
     * @return the Kafka consumer group id
     */
    public String getGroupId() {
        return properties.getProperty(ConsumerConfig.GROUP_ID_CONFIG);
    }

    @Override
    public String toString() {
        return "ProcessingConfig{" +
                "commitTimeThreshold=" + commitTimeThreshold +
                ", commitSizeThreshold=" + commitSizeThreshold +
                ", offsetResetStrategy=" + offsetResetStrategy +
                ", failThreshold=" + failThreshold +
                ", failSampleSize=" + failSampleSize +
                ", failPauseTime=" + failPauseTime +
                ", properties=" + properties +
                '}';
    }
}
