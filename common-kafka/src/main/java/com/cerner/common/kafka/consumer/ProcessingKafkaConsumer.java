package com.cerner.common.kafka.consumer;

import com.cerner.common.kafka.metrics.MeterPool;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Meter;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * <p>
 * A class that wraps Kafka's {@link Consumer} and provides methods for describing message processing. The class handles
 * committing successfully processed messages and resetting the consumer during processing failures to provide at least
 * once processing.
 * </p>
 *
 * <h3>Usage</h3>
 *
 * <pre>
 * // Build a new consumer with your config
 * ProcessingKafkaConsumer consumer = new ProcessingKafkaConsumer(new ProcessingConfig(properties));
 *
 * // Subscribe to your topics
 * consumer.subscribe(..);
 *
 * // Read the next record (1000L is the maximum amount of time to wait for a record in milliseconds)
 * ConsumerRecord record = consumer.nextRecord(1000L);
 *
 * // Reading a record may return null in some cases if there are no records to read from Kafka
 * if (record != null) {
 *
 *     // Process the record
 *     boolean processed = process(record);
 *
 *     if (processed) {
 *         // Ack records that were successfully processed
 *         consumer.ack(record);
 *     }
 *     else {
 *         // or fail records that were unsuccessful
 *         consumer.fail(record);
 *     }
 * }
 *
 * // Close the consumer when finished
 * consumer.close();
 * </pre>
 *
 * <p>
 * All records should be {@link #ack(TopicPartition, long) acked} or {@link #fail(TopicPartition, long) failed} within a
 * reasonable amount of time. If a record is stuck in a pending state while other records continue to be processed
 * successfully this can block the consumer from committing its offsets.
 * </p>
 *
 * <p>
 * This class is not thread safe
 * </p>
 *
 * @param <K> the class that represents the key in the Kafka message
 * @param <V> the class that represents the value in the Kafka message
 * @author Bryan Baugher
 */
public class ProcessingKafkaConsumer<K, V> implements Closeable {

    /**
     * Logger
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(ProcessingKafkaConsumer.class);

    /**
     * Counter used for tracking rebalances. Re-balances are normally infrequent so a counter is used instead of meter
     */
    static final Counter REBALANCE_COUNTER = Metrics.newCounter(ProcessingKafkaConsumer.class, "rebalances");

    /**
     * Meter used for tracking acks
     */
    static final Meter ACK_METER = Metrics.newMeter(ProcessingKafkaConsumer.class, "acks", "acks",
            TimeUnit.SECONDS);

    /**
     * Meter used for tracking fails
     */
    static final Meter FAIL_METER = Metrics.newMeter(ProcessingKafkaConsumer.class, "fails", "fails",
            TimeUnit.SECONDS);

    /**
     * Meter used for tracking commits
     */
    static final Meter COMMIT_METER = Metrics.newMeter(ProcessingKafkaConsumer.class, "commits", "commits",
            TimeUnit.SECONDS);

    /**
     * Meter used for tracking commits scoped by consumer group, topic and partition
     */
    static final MeterPool PARTITION_COMMIT_METER = new MeterPool(ProcessingKafkaConsumer.class,"partition-commits");

    /**
     * A histogram of the difference between when a message was written and when it was read by the consumer #nextRecord()
     */
    static final Histogram READ_LATENCY = Metrics.newHistogram(ProcessingKafkaConsumer.class, "read-latency");

    /**
     * A histogram of the number of messages read from poll
     */
    static final Histogram POLL_MESSAGES = Metrics.newHistogram(ProcessingKafkaConsumer.class, "poll-messages");

    /**
     * A histogram of the amount of time between Consumer#poll(long) calls
     */
    static final Histogram POLL_LATENCY = Metrics.newHistogram(ProcessingKafkaConsumer.class, "poll-latency");

    /**
     * The Kafka consumer used to read messages from
     */
    protected final Consumer<K, V> consumer;

    /**
     * The config provided for this consumer
     */
    protected final ProcessingConfig config;

    /**
     * The re-balance listener for our consumer
     */
    protected final ConsumerRebalanceListener rebalanceListener = getRebalanceListener();

    /**
     * The last time we checked to see if we could un-pause any partitions
     */
    protected long lastUnpauseCheckTime = System.currentTimeMillis();

    /**
     * The last time a commit occurred
     */
    protected long lastCommitTime = System.currentTimeMillis();

    /**
     * Maintains the state of all the partitions our consumer is assigned to
     */
    protected final Map<TopicPartition, ProcessingPartition<K, V>> partitions = Collections.synchronizedMap(new LinkedHashMap<>());

    /**
     * The partition to process from next
     */
    protected int partitionsToProcessIndex = 0;

    /**
     * Indicates if we are allowed to commit now or should wait until next time we poll()
     */
    protected volatile boolean pauseCommit = false;

    /**
     * The last time Consumer#poll(Duration) was called
     */
    protected long lastPollTime = -1L;

    /**
     * Number of times Consumer#poll(Duration) was called
     */
    private long pollCount = 0L;

    /**
     * Creates a new {@link ProcessingKafkaConsumer} with the given configuration using the {@link KafkaConsumer}
     *
     * @param config the configuration used by the consumer
     * @throws IllegalArgumentException if config is {@code null}
     * @throws KafkaException if there is an issue creating the {@link KafkaConsumer}
     */
    public ProcessingKafkaConsumer(ProcessingConfig config) {
        this(config, new KafkaConsumer<>(extractProperties(config)));
    }

    /**
     * Creates a new {@link ProcessingKafkaConsumer} with the given configuration and deserializers using the {@link KafkaConsumer}.
     *
     * @param config the configuration used by the consumer
     * @param keyDeserializer The deserializer instance for key. When passing the deserializer instance
     *          directly, the {@link Deserializer#configure(Map, boolean)} method will not be called. If null,
     *          then it will fall back to the {@code key.deserializer} config property
     * @param valueDeserializer The deserializer instance for value. When passing the deserializer instance
     *          directly, the {@link Deserializer#configure(Map, boolean)} method will not be called. If null,
     *          then it will fall back to the {@code value.deserializer} config property
     * @throws IllegalArgumentException if config is {@code null}
     * @throws KafkaException if there is an issue creating the {@link KafkaConsumer}
     */
    public ProcessingKafkaConsumer(ProcessingConfig config, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
        this(config, new KafkaConsumer<>(extractProperties(config), keyDeserializer, valueDeserializer));
    }

    private static Properties extractProperties(ProcessingConfig config) {
        if (config == null)
            throw new IllegalArgumentException("config cannot be null");

        return config.getProperties();
    }

    /**
     * <p>
     * Creates a new {@link ProcessingKafkaConsumer} with the given configuration and consumer
     * </p>
     *
     * <p>
     * NOTE: The consumer will still be managed by this object and closed during {@link #close()}
     * </p>
     *
     * @param config   the configuration for the processing consumer
     * @param consumer the Kafka consumer used to read messages
     * @throws IllegalArgumentException if config or consumer is {@code null}
     */
    ProcessingKafkaConsumer(ProcessingConfig config, Consumer<K, V> consumer) {
        if (config == null)
            throw new IllegalArgumentException("config cannot be null");
        if (consumer == null)
            throw new IllegalArgumentException("consumer cannot be null");

        this.consumer = consumer;
        this.config = config;
    }

    /**
     * Returns the next record to be consumed or an empty optional if no record is available. This method may block up to the
     * TIMEOUT value (in milliseconds).
     *
     * @param timeout The polling TIMEOUT.
     * @return the next record to be consumed or an empty optional if no record is available
     *
     * @throws KafkaException
     *          if there is an issue reading the next record from Kafka
     * @throws IllegalStateException
     *          if an internal initialization issue occurs
     */
    public Optional<ConsumerRecord<K, V>> nextRecord(final long timeout) {
        // Un-pause any paused partitions that have been suspended long enough
        maybeUnpausePartitions();

        // Try to commit eligible offsets if enough time has passed since last commit
        maybeCommitOffsetsForTime();

        // Select all partitions which have records to be processed
        List<ProcessingPartition<K, V>> partitionsToProcess = partitions.values().stream()
                .filter(ProcessingPartition::hasNextRecord)
                .collect(Collectors.toList());

        // If there are no partitions to process we need to fetch more data
        if (partitionsToProcess.isEmpty()) {
            LOGGER.debug("Polling kafka for more data");

            partitionsToProcessIndex = 0;
            ConsumerRecords<K, V> records = pollRecords(timeout);

            // If we paused commits un-pause them now as we should have re-joined the group if necessary
            pauseCommit = false;

            // Add records to each partition
            records.partitions().forEach(topicPartition -> {
                ProcessingPartition<K, V> processingPartition = partitions.get(topicPartition);

                if (processingPartition == null) {
                    LOGGER.debug("Read a record for which we don't have a processing partition for [{}]. Adding partition",
                            topicPartition);
                    processingPartition = buildPartition(topicPartition, config, consumer);
                    partitions.put(topicPartition, processingPartition);
                }

                // Add records to partition
                processingPartition.load(records.records(topicPartition));
            });

            POLL_MESSAGES.update(records.count());

            // Reload our partitions to process
            partitionsToProcess = partitions.values().stream()
                    .filter(ProcessingPartition::hasNextRecord)
                    .collect(Collectors.toList());
        }

        ConsumerRecord<K, V> record = null;

        // Loop until we either run out of records or find a valid record to process
        while (!partitionsToProcess.isEmpty() && record == null) {
            // If the number of partitions to process has changed outside of this code (i.e. partition became paused or
            // re-assigned) verify our index is still valid otherwise start back at zero
            if (partitionsToProcessIndex >= partitionsToProcess.size())
                partitionsToProcessIndex = 0;

            ProcessingPartition<K, V> processingPartition = partitionsToProcess.get(partitionsToProcessIndex);

            if (processingPartition.hasNextRecord()) {
                LOGGER.debug("Pulling record from partition [{}]", processingPartition.getTopicPartition());
                record = processingPartition.nextRecord();
            }
            else {
                LOGGER.debug("No more messages in partition [{}] removing from partitions to process",
                        processingPartition.getTopicPartition());
                partitionsToProcess.remove(partitionsToProcessIndex);
            }

            // Signal to process the next partition and wrap back to 0 if we go over
            if (!partitionsToProcess.isEmpty())
                partitionsToProcessIndex = (partitionsToProcessIndex + 1) % partitionsToProcess.size();
        }

        if (record != null)
            // Difference between when we read the value and when the record was written
            READ_LATENCY.update(System.currentTimeMillis() - record.timestamp());

        return Optional.ofNullable(record);
    }

    private ConsumerRecords<K, V> pollRecords(long timeout) {
        try {

            long currentTime = System.currentTimeMillis();

            // Exclude the first poll from the latency timer, since it takes approximately max.poll.interval.ms as the
            // consumer group is stabilizing.
            if (lastPollTime != -1L && pollCount > 1L) {
                long pollLatency = currentTime - lastPollTime;

                POLL_LATENCY.update(pollLatency);

                if (pollLatency > config.getMaxPollInterval() && LOGGER.isWarnEnabled()) {
                    LOGGER.warn("{}ms has elapsed since last #poll(). This is greater than max.poll.interval.ms {}. If this " +
                            "continues you may need to increase max.poll.interval.ms", pollLatency, config.getMaxPollInterval());
                }
            }

            lastPollTime = currentTime;
            ++pollCount;

            return consumer.poll(Duration.ofMillis(timeout));
        } catch (IllegalStateException e) {
            // The Kafka consumer will throw this exception if the consumer is not currently subscribed to any topics. Return an
            // empty record collection after verifying that is in fact the case, otherwise rethrow the exception.
            if (consumer.subscription().isEmpty()) {
                LOGGER.debug("Consumer with no subscriptions polled for records.");
                return ConsumerRecords.empty();
            } else {
                throw e;
            }
        }
    }

    /**
     * Un-pauses any partitions that have been suspended the paused time amount
     */
    private void maybeUnpausePartitions() {
        long currentTime = System.currentTimeMillis();

        LOGGER.debug("Checking for un-pause-able partitions. Current time [{}]", currentTime);

        // Only check to see if we need to un-paused partitions every so often
        if (currentTime < lastUnpauseCheckTime + config.getFailPauseTime()) {
            LOGGER.debug("Not checking paused partitions as not enough time [{}] has past. Waiting till [{}]",
                    currentTime, (lastUnpauseCheckTime + config.getFailPauseTime()));
            return;
        }

        LOGGER.debug("Enough time has passed since last un-pause check [{}]. Possibly un-pausing partitions",
                lastUnpauseCheckTime);

        // Signal to partitions to consider un-pausing
        LOGGER.debug("Size of partitions [{}]", partitions.size());
        partitions.values().forEach(p -> p.maybeUnpause(currentTime));

        lastUnpauseCheckTime = currentTime;
    }

    /**
     * Acknowledge the given record/message has been processed successfully
     *
     * @param record the record that was successfully processed
     * @return {@code true} if the consumer had a pending message for that partition/offset and {@code false} otherwise
     * @throws IllegalArgumentException if record is {@code null}
     */
    public boolean ack(ConsumerRecord<K, V> record) {
        if (record == null)
            throw new IllegalArgumentException("record cannot be null");

        return ack(new TopicPartition(record.topic(), record.partition()), record.offset());
    }

    /**
     * Acknowledge the given record/message has been processed successfully
     *
     * @param topicPartition the topic partition of the message that was processed successfully
     * @param offset         the offset of the message that was successfully processed
     * @return {@code true} if the consumer had a pending message for that partition/offset and {@code false} otherwise
     * @throws IllegalArgumentException if topicPartition is {@code null}
     * @throws KafkaException if there is an issue committing offsets to Kafka
     */
    public boolean ack(TopicPartition topicPartition, long offset) {
        if (topicPartition == null)
            throw new IllegalArgumentException("topicPartition cannot be null");
        LOGGER.debug("acking for topic partition [{}] with offset [{}]", topicPartition, offset);

        ProcessingPartition<K, V> processingPartition = partitions.get(topicPartition);

        if (processingPartition == null) {
            LOGGER.debug("Cannot ack as we are not assigned to process data for partition [{}] offset [{}]",
                    topicPartition, offset);
            return false;
        }

        boolean result = processingPartition.ack(offset);

        if (result)
            ACK_METER.mark();

        // Try to commit eligible offsets if the size or time thresholds have been met
        if (!maybeCommitOffsetsForTime())
            // Only check size if we didn't commit for time
            maybeCommitOffsetsForSize(processingPartition);

        return result;
    }

    /**
     * Informs the processing consumer that the message failed to be processed and resets the consumer accordingly to
     * re-process the message
     *
     * @param record the message that failed to be processed
     * @return {@code true} if the consumer had a pending message for that partition/offset and {@code false} otherwise
     * @throws IllegalArgumentException if record is {@code null}
     * @throws KafkaException if there is an issue rewinding the consumer to re-read the failed record
     */
    public boolean fail(ConsumerRecord<K, V> record) {
        if (record == null)
            throw new IllegalArgumentException("record cannot be null");

        return fail(new TopicPartition(record.topic(), record.partition()), record.offset());
    }

    /**
     * Informs the processing consumer that the message failed to be processed and resets the consumer accordingly to
     * re-process the message
     *
     * @param topicPartition the topic partition of the message that was failed to be processed
     * @param offset         the offset of the message that was failed to be processed
     * @return {@code true} if the consumer had a pending message for that partition/offset and {@code false} otherwise
     * @throws IllegalArgumentException if topicPartition is {@code null}
     * @throws KafkaException if there is an issue rewinding the consumer to re-read the failed record
     */
    public boolean fail(TopicPartition topicPartition, long offset) {
        if (topicPartition == null)
            throw new IllegalArgumentException("topicPartition cannot be null");
        LOGGER.debug("failing for topic partition [{}] with offset [{}]", topicPartition, offset);

        ProcessingPartition<K, V> processingPartition = partitions.get(topicPartition);

        if (processingPartition == null) {
            LOGGER.debug("Cannot fail as we are not assigned to process data for partition [{}] offset [{}]",
                    topicPartition, offset);
            return false;
        }

        boolean result = processingPartition.fail(offset);

        if (result)
            FAIL_METER.mark();

        return result;
    }

    /**
     * Reset the consumer to start consuming from the specified offset values. Offsets for partitions not currently
     * assigned to this consumer will be ignored. Note that the partition assignment can be empty immediately after
     * {@link #subscribe(Collection) subscribing to topics} prior to a {@link #nextRecord(long) poll of sufficient
     * duration}, or when partitions are in the process of getting reassigned.
     *
     * @param offsets the offsets to reset the consumer to
     * @throws IllegalArgumentException if {@code offsets} is {@code null}
     * @throws KafkaException if there is an issue resetting the offsets
     */
    public synchronized void resetOffsets(Map<TopicPartition, Long> offsets) {
        if (offsets == null)
            throw new IllegalArgumentException("offsets cannot be null");

        // Inquire the partitions that are assigned to this consumer currently.
        Set<TopicPartition> assignedPartitions = consumer.assignment();
        LOGGER.debug("resetting offsets for assigned partitions {}", assignedPartitions);

        // Close and remove all assigned partitions, they will be regenerated as new messages are read.
        partitions.values().stream()
                .filter(partition -> assignedPartitions.contains(partition.getTopicPartition()))
                .forEach(partition -> IOUtils.closeQuietly(partition));
        partitions.keySet().removeAll(assignedPartitions);

        // Commit offsets for consumer's assigned partitions.
        Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = offsets.entrySet().stream()
                .filter(e -> assignedPartitions.contains(e.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, e -> new OffsetAndMetadata(e.getValue())));
        commitOffsets(offsetsToCommit);

        // Seek consumer to the offsets that were just committed for its assigned partitions.
        offsetsToCommit.entrySet().stream().forEach(e -> consumer.seek(e.getKey(), e.getValue().offset()));
    }

    /**
     * Commits all committable offsets
     *
     * @throws KafkaException
     *          if there is an issue committing offsets to Kafka
     */
    public synchronized void commitOffsets() {
        if (pauseCommit) {
            LOGGER.debug("Commits are paused until we poll() again");
            return;
        }

        LOGGER.debug("committing offsets");
        try {
            commitOffsets(getCommittableOffsets());
        } catch(CommitFailedException e) {
            LOGGER.debug("Failed to commit offsets, pausing commits until next poll", e);
            pauseCommit = true;
            throw e;
        }
    }

    /**
     * Commits the given offsets and removes offsets from completed
     *
     * @param offsetsToCommit
     *          offsets to commit per partition
     *
     * @throws KafkaException
     *          if there is an issue committing offsets to Kafka
     */
    protected void commitOffsets(Map<TopicPartition, OffsetAndMetadata> offsetsToCommit) {
        if (!offsetsToCommit.isEmpty()) {
            if (LOGGER.isDebugEnabled())
                LOGGER.debug("Syncing commits {}", Arrays.toString(offsetsToCommit.entrySet().toArray()));

            consumer.commitSync(offsetsToCommit);
            COMMIT_METER.mark();

            // Update all partitions that we've committed these values
            offsetsToCommit.entrySet().forEach(e -> {
                ProcessingPartition<K, V> partition = partitions.get(e.getKey());
                if (partition != null) {
                    partition.committedOffset(e.getValue().offset());
                    PARTITION_COMMIT_METER.getMeter(getPartitionConsumerScope(e.getKey())).mark();
                }
            });
        } else {
            LOGGER.debug("No offsets to commit");
        }

        // Update the last commit time
        lastCommitTime = System.currentTimeMillis();
    }

    /**
     * Generates the scope for partition commit metric.
     * E.g: {consumer_group}.{topic}.{partition}
     *
     * @param topicPartition {@link TopicPartition} used to generate scope for partition commit metric.
     * @return scope for partition commit metric
     */
    protected String getPartitionConsumerScope(TopicPartition topicPartition) {
        StringBuilder scopeBuilder = new StringBuilder(config.getGroupId())
                .append(".")
                .append(topicPartition.topic())
                .append(".")
                .append(topicPartition.partition());
        return scopeBuilder.toString();
    }

    /**
     * Commits offsets if we meet the time threshold
     *
     * @return {@code true} if we did decide to commit offsets
     *
     * @throws KafkaException
     *          if there is an issue committing offsets to Kafka
     */
    protected boolean maybeCommitOffsetsForTime() {

        if (pauseCommit) {
            LOGGER.debug("Commits are paused, not attempting to commit offsets for time");
            return false;
        }

        long currentTime = System.currentTimeMillis();

        if (lastCommitTime + config.getCommitTimeThreshold() <= currentTime) {
            if (LOGGER.isDebugEnabled())
                LOGGER.debug("Committing offsets due to time. Last commit time [{}], commit time threshold [{}], " +
                        "current time [{}]", new Object[]{lastCommitTime, config.getCommitTimeThreshold(), currentTime});

            commitOffsets();
            return true;
        }

        return false;
    }

    /**
     * Commit offsets if we meet the size threshold
     *
     * @param processingPartition partition to consider committing our processing offsets for
     *
     * @throws KafkaException
     *          if there is an issue committing offsets to Kafka
     */
    protected void maybeCommitOffsetsForSize(ProcessingPartition<K, V> processingPartition) {

        if (pauseCommit) {
            LOGGER.debug("Commits are paused, not attempting to commit offsets for size");
            return;
        }

        long committableOffsets = processingPartition.getCommittableOffsetsSize();
        if (config.getCommitSizeThreshold() <= committableOffsets) {

            if (LOGGER.isDebugEnabled())
                LOGGER.debug("Committing offsets due to committable offsets [{}] >= commit size threshold [{}] for partition [{}]",
                        new Object[] { committableOffsets, config.getCommitSizeThreshold(),
                                processingPartition.getTopicPartition() });

            commitOffsets();
        }
    }

    /**
     * Returns a map of topic partition to the offset that can be committed
     *
     * @return a map of topic partition to the offset that can be committed
     */
    protected Map<TopicPartition, OffsetAndMetadata> getCommittableOffsets() {
        return partitions.entrySet().stream()
                .filter(e -> e.getValue().getCommittableOffset() != null)
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getCommittableOffset()));
    }

    // Used for unit testing
    protected ProcessingPartition<K, V> buildPartition(TopicPartition topicPartition, ProcessingConfig processingConfig,
                                                       Consumer<K, V> consumer) {
        return new ProcessingPartition<>(topicPartition, processingConfig, consumer);
    }

    /**
     * <p>
     * Subscribes the consumer to the collection of topics. This call is not additive and will replace any existing
     * subscription.
     * </p>
     *
     * <p>
     * NOTE: This should be called prior to calling {@link #nextRecord(long)} in order to set the appropriate topics to read
     * from.
     * </p>
     *
     * @param topics the topics to subscribe to
     */
    public void subscribe(Collection<String> topics) {
        // This method does not throw a KafkaException
        consumer.subscribe(topics, rebalanceListener);
    }

    /**
     * <p>
     * Subscribes the consumer to the provided topic name regular expression pattern. This call is not additive and will replace
     * any existing subscription.
     * </p>
     *
     * <p>
     * NOTE: This should be called prior to calling {@link #nextRecord(long)} in order to set the appropriate topics to read
     * from.
     * </p>
     *
     * @param pattern the topic name regular expression pattern to subscribe to
     */
    public void subscribe(Pattern pattern) {
        // This method does not throw a KafkaException
        consumer.subscribe(pattern, rebalanceListener);
    }

    /**
     * <p>
     * Returns the {@link Consumer} used by this class
     * </p>
     *
     * <p>
     * It is perfectly safe to call any 'read' methods on the consumer but should be noted that methods that change the
     * consumer's state may cause issues for the {@link ProcessingKafkaConsumer}.
     * </p>
     *
     * @return the {@link Consumer} used by this class
     */
    public Consumer<K, V> getConsumer() {
        return consumer;
    }

    @Override
    public void close() throws IOException {
        // Close all partitions
        closeAllPartitionsQuitely();

        try {
            commitOffsets();
        } finally {
            IOUtils.closeQuietly(consumer);
        }
    }

    class ProcessingRebalanceListener implements ConsumerRebalanceListener {

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitionsCurrentlyAssigned) {
            try {
                // Commit all offsets during a re-balance
                commitOffsets();
            } catch(KafkaException e) {
                LOGGER.error("Failed to commit offsets during re-balance", e);
            }
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitionsAssigned) {
            if (LOGGER.isDebugEnabled())
                LOGGER.debug("Re-assigned to partitions {}", Arrays.toString(partitionsAssigned.toArray()));

            // Increase rebalance counter
            REBALANCE_COUNTER.inc();

            LOGGER.info("Rebalance [{}] is in progress. Old partitions: {} New Partitions: {}", REBALANCE_COUNTER.count(),
                    partitions.keySet(), partitionsAssigned);

            /**
             * We must reset the local partition state (cached offsets) for all partitions,
             * even if the last assignment we saw also included these partitions,
             * because we may have been disconnected and missed an entire group generation
             * during which another consumer might have been assigned the partition and committed offsets.
             */
            closeAllPartitionsQuitely();

            partitions.clear();

            try {
                partitionsAssigned
                        .forEach(tp -> partitions.put(tp, buildPartition(tp, config, consumer)));
            } catch (IllegalStateException e) {
                LOGGER.error("Failed to initialize processing partition", e);
            }
        }
    }

    ConsumerRebalanceListener getRebalanceListener() {
        return new ProcessingRebalanceListener();
    }

    private void closeAllPartitionsQuitely(){
        partitions.values().forEach(partition -> IOUtils.closeQuietly(partition));
    }
}

