package com.cerner.common.kafka.consumer;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Meter;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

/**
 * Represents a Kafka partition and maintains state about the records that have been processed successfully to ensure
 * at least once processing of every message
 *
 * @param <K> the class that represents the key in the Kafka message
 * @param <V> the class that represents the value in the Kafka message
 *
 * @author Bryan Baugher
 */
public class ProcessingPartition<K, V> implements Closeable {

    /**
     * Logger
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(ProcessingPartition.class);

    /**
     * A counter that tracks current number of paused partitions
     */
    static final Counter PAUSED_PARTITIONS = Metrics.newCounter(ProcessingPartition.class, "paused-partitions");

    /**
     * A meter that tracks partition pauses rates/history
     */
    static final Meter PAUSED_METER = Metrics.newMeter(ProcessingPartition.class, "pauses", "pauses", TimeUnit.SECONDS);

    /**
     * A histogram of the number of messages committed per partition for each commit request
     */
    static final Histogram PARTITION_COMMITTED_OFFSETS = Metrics.newHistogram(ProcessingPartition.class,
            "partition-committed-offsets");

    /**
     * A histogram of the difference between consumer {@link #nextRecord() reading} a message  and {@link #ack(long) acking} it
     */
    static final Histogram PROCESSING_LATENCY = Metrics.newHistogram(ProcessingPartition.class, "processing-latency");

    /**
     * The partition
     */
    private final TopicPartition topicPartition;

    /**
     * The config associated to the consumer
     */
    private final ProcessingConfig config;

    /**
     * The consumer for this partition
     */
    private final Consumer<K, V> consumer;

    /**
     * The set of offsets that have been {@link #nextRecord() read} but not {@link #ack(long) acked}
     */
    protected final Map<Long, Long> pendingOffsets = new HashMap<>();

    /**
     * The set of offsets that have been {@link #ack(long) acked} but not committed
     */
    protected final SortedSet<Long> completedOffsets = new TreeSet<>();

    /**
     * The time in epoch when the partition can be un-paused
     */
    protected long pausedTillTime = 0L;

    /**
     * The cached number of recent failures
     */
    protected int recentFailedResults;

    /**
     * The list of the most recent processing results (true=success, false=failure)
     */
    protected LinkedList<Boolean> recentProcessingResults;

    /**
     * The offset of the record we should read next
     */
    protected long offsetPosition = 0L;

    /**
     * The offset that could be committed. Can be null if there is nothing to commit
     */
    protected Long committableOffset = null;

    /**
     * The offset for the last committed value. Maybe be null if we haven't committed anything yet
     */
    protected long lastCommittedOffset;

    /**
     * If the partition is in a paused state
     */
    protected boolean paused = false;

    /**
     * The iterator of records to be read
     */
    protected Iterator<ConsumerRecord<K, V>> records = Collections.emptyIterator();

    /**
     * Creates a processing partition
     *
     * @param topicPartition
     *          the Kafka partition this represents
     * @param config
     *          the config used for the consumer
     * @param consumer
     *          the consumer used to read values from Kafka
     * @throws IllegalStateException
     *          if the processing partition could not be properly initialized
     */
    public ProcessingPartition(TopicPartition topicPartition, ProcessingConfig config, Consumer<K, V> consumer) {
        this.topicPartition = topicPartition;
        this.config = config;
        this.consumer = consumer;

        // Start results with 100% successful
        resetResults();

        lastCommittedOffset = getLastCommittedOffset();

        // Consumer fetch cursors might be ahead of last committed offset
        resetToCommitted();
    }

    /**
     * Resets the results to be 100% successful
     */
    private void resetResults() {
        recentProcessingResults = new LinkedList<>(Collections.nCopies(config.getFailSampleSize(), true));
        recentFailedResults = 0; // no failures
    }

    /**
     * Mark the record as having been processed successfully
     *
     * @param offset
     *          the offset of the record
     * @return
     *          {@code true} if the offset was pending (read from {@link #nextRecord()} but not {@link #ack(long) acked}
     *          or {@link #fail(long) failed})
     */
    public boolean ack(long offset) {
        Long messageReadTime = pendingOffsets.remove(offset);

        if (messageReadTime == null) {
            LOGGER.debug("Ack for record on topic partition [{}] with offset [{}] is invalid as that offset is not " +
                    "pending. Not committing", topicPartition, offset);
            return false;
        }

        PROCESSING_LATENCY.update(System.currentTimeMillis() - messageReadTime);

        LOGGER.debug("Acking record with offset [{}] for partition [{}]", offset, topicPartition);

        completedOffsets.add(offset);

        maybeUpdateCommitableOffset(offset);

        // Add a success
        addResult(true);

        return true;
    }

    /**
     * Mark the record as having been processed unsuccessfully
     *
     * @param offset
     *          the offset of the record
     * @return
     *          {@code true} if the offset was pending (read from {@link #nextRecord()} but not {@link #ack(long) acked}
     *          or {@link #fail(long) failed})
     *
     * @throws KafkaException
     *          if there is an issue rewinding the consumer to re-read the failed record
     */
    public boolean fail(long offset) {
        if (pendingOffsets.remove(offset) == null) {
            LOGGER.debug("Fail for record on topic partition [{}] with offset [{}] is invalid as that offset is not " +
                    "pending. Not resetting consumer", topicPartition, offset);
            return false;
        }

        LOGGER.debug("Failing record for partition [{}] and offset [{}]", topicPartition, offset);

        // If our offset position is after the current offset then reset otherwise we should already be set to
        // re-read this again
        // The offset may be below our current offset position if we recently reset the position due to another failure
        if (offsetPosition > offset) {
            if (LOGGER.isDebugEnabled())
                LOGGER.debug("Failed record with partition [{}] and offset [{}] has offset before our current offset " +
                        "position [{}], resetting consumer", topicPartition, offset, offsetPosition);
            resetToCommitted();

            // Wipe records in order to keep our offset position correct (i.e. we don't read records ahead of this in our
            // cache). This is fine since we did a rewind we will re-read these anyway
            records = Collections.<ConsumerRecord<K, V>> emptyList().iterator();
        }

        if (!paused) {
            // Add a failure
            addResult(false);

            double failurePercent = (double) recentFailedResults / config.getFailSampleSize();

            LOGGER.debug("Failed processing for partition [{}] at [{}%]", topicPartition, failurePercent * 100);

            if (failurePercent >= config.getFailThreshold()) {
                paused = true;
                pausedTillTime = System.currentTimeMillis() + config.getFailPauseTime();

                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("Failed processing [{}%] above threshold [{}%]. Pausing partition [{}] till [{}]", new Object[] {
                        failurePercent * 100, config.getFailThreshold() * 100, topicPartition, new Date(pausedTillTime) });
                }

                // This method does not throw a KafkaException
                consumer.pause(Collections.singleton(topicPartition));

                PAUSED_METER.mark();
                PAUSED_PARTITIONS.inc();
            }
        }

        return true;
    }

    /**
     * Adds the processing result to our list of successful results
     *
     * @param successfulResult
     *          if the result was successful {@code true} or not
     */
    private void addResult(boolean successfulResult) {
        LOGGER.debug("Adding result [{}] to recent results for partition [{}]", successfulResult, topicPartition);

        recentProcessingResults.addFirst(successfulResult);

        // If the added result is a failure (false) add one to recent failures
        if (!successfulResult)
            recentFailedResults++;

        // Remove the last value to keep our result sample to the same size
        boolean removedResult = recentProcessingResults.pollLast();

        LOGGER.debug("Removed result [{}] for partition [{}]", removedResult, topicPartition);

        // If the removed result was a failure (false) subtract 1 from recent failures
        if (!removedResult)
            recentFailedResults--;

        LOGGER.debug("Recent failed results at [{}] for partition [{}]", recentFailedResults, topicPartition);
    }

    /**
     * Check if the partition should be un-paused
     *
     * @param currentTime
     *          the current time since epoch
     */
    public void maybeUnpause(long currentTime) {
        if (!paused) {
            LOGGER.debug("Partition [{}] not paused. Nothing to do", topicPartition);
            return;
        }

        if (currentTime >= pausedTillTime) {
            if(LOGGER.isInfoEnabled()){
                LOGGER.info("Unpausing partition [{}] as the current time [{}] is >= paused time [{}]",
                        new Object[] { topicPartition, new Date(currentTime), new Date(pausedTillTime) });
            }

            // This method does not throw a KafkaException
            consumer.resume(Collections.singleton(topicPartition));

            PAUSED_PARTITIONS.dec();
            paused = false;

            // Reset successful results to 100% successful
            resetResults();
        }
        else{
            if (LOGGER.isDebugEnabled())
                LOGGER.debug("Not unpausing partition [{}] as the current time [{}] is < paused time [{}]",
                    topicPartition, currentTime, pausedTillTime);
        }
    }

    /**
     * Returns the offset to use as the partition's last committed offset. This value indicates where we will start
     * reading on the partition and is used to determine what offset is eligible for committing and where to rewind to
     * re-read a message. The returned offset is ensured to be committed, if {@link ProcessingConfig#getCommitInitialOffset()
     * allowed} by the configuration.
     *
     * @return the offset to use as the partition's last committed offset
     *
     * @throws IllegalStateException if an error occurs looking up the consumer's committed offset or broker offset values
     */
    protected long getLastCommittedOffset() {
        OffsetAndMetadata lastCommittedOffset;
        try {
            lastCommittedOffset = consumer.committed(topicPartition);
        } catch (KafkaException e) {
            throw new IllegalStateException("Unable to retrieve committed offset for topic partition [" + topicPartition + "]", e);
        }

        // If we don't have a committed offset use the reset offset
        if (lastCommittedOffset == null) {
            LOGGER.debug("No offset committed for partition [{}]", topicPartition);
            return getCommittedResetOffset();
        }

        long offset = lastCommittedOffset.offset();

        LOGGER.debug("Last committed offset for partition [{}] is [{}]", topicPartition, offset);

        long startOffset = getEarliestOffset();

        // Verify our committed offset is not before the earliest offset
        if (offset < startOffset) {
            // If our offset is before the start offset this likely means processing was stopped/stalled for so long
            // messages fell off the queue and we failed to process them
            if (LOGGER.isErrorEnabled())
                LOGGER.error("Committed offset [{}] is before earliest offset [{}] partition [{}]. This likely indicates"
                        + " that processing missed messages", offset, startOffset, topicPartition);

            // If it's not in range use the reset offset as that's where we will be starting from
            return getCommittedResetOffset();
        }

        long endOffset = getLatestOffset();

        // Verify our committed offset is not after the latest offset
        if (offset > endOffset) {
            if (LOGGER.isWarnEnabled())
                LOGGER.warn("Committed offset [{}] is after latest offset [{}] for partition [{}]. This could indicate " +
                        "a bug in the ProcessingConsumer, a topic being re-created or something else updating offsets",
                        offset, endOffset, topicPartition);

            // If it's not in range use the reset offset as that's where we will be starting from
            return getCommittedResetOffset();
        }

        if (LOGGER.isDebugEnabled())
            LOGGER.debug("Using committed offset [{}] for partition [{}] as it is in range of start [{}] / end [{}] broker offsets",
                offset, topicPartition, startOffset, endOffset);

        return offset;
    }

    /**
     * Returns the reset offset used in situations where the consumer has no committed offset for a partition, or its committed
     * offset is out of range. The returned offset is ensured to be committed, if {@link ProcessingConfig#getCommitInitialOffset()
     * allowed} by the configuration.
     *
     * @return the reset offset
     */
    private long getCommittedResetOffset() {
        // Get the reset offset
        long resetOffset = getResetOffset();

        LOGGER.debug("Using reset offset [{}] for partition [{}] as last committed offset", resetOffset, topicPartition);

        // Consumer doesn't have an offset so try to commit the offset. This can be helpful for monitoring in case
        // there are no messages in the queue or processing is failing
        if (config.getCommitInitialOffset()) {
            try {
                consumer.commitSync(Collections.singletonMap(topicPartition, new OffsetAndMetadata(resetOffset)));
            } catch (KafkaException e) {
                LOGGER.warn("Unable to commit reset offset {} during initialization of partition {} for group {}", resetOffset,
                        topicPartition, config.getGroupId(), e);
            }
        }

        return resetOffset;
    }

    /**
     * Returns the offset that would be used for the partition based on the consumer's configured offset reset strategy
     *
     * @return the offset that would be used for the partition based on the consumer's configured offset reset strategy
     */
    protected long getResetOffset() {
        OffsetResetStrategy strategy = config.getOffsetResetStrategy();

        if (strategy == OffsetResetStrategy.EARLIEST) {
            LOGGER.debug("Looking up offset for partition [{}] using earliest reset", topicPartition);
            return getEarliestOffset();
        }
        else if (strategy == OffsetResetStrategy.LATEST) {
            LOGGER.debug("Looking up offset for partition [{}] using latest reset", topicPartition);
            return getLatestOffset();
        }
        else {
            throw new IllegalStateException("Unable to reset partition to previously committed offset as there is no"
                    + " offset for partition [" + topicPartition + "] and offset reset strategy [" + strategy + "] is unknown");
        }
    }

    /**
     * Returns the earliest offset for the partition
     *
     * @return the earliest offset for the partition
     *
     * @throws IllegalStateException if the earliest offset could not be looked up
     */
    protected long getEarliestOffset() {
        Map<TopicPartition, Long> offsets;

        try {
            offsets = consumer.beginningOffsets(Collections.singleton(topicPartition));
        } catch (TimeoutException | InterruptException e) {
            throw new IllegalStateException("Unable to look up earliest offset for topic partition [" + topicPartition + "]", e);
        }

        if (!offsets.containsKey(topicPartition))
            throw new IllegalStateException("Unable to look up earliest offset for topic partition [" + topicPartition + "]");

        Long offset = offsets.get(topicPartition);

        if (offset == null)
            throw new IllegalStateException("Unable to look up earliest offset for topic partition [" + topicPartition + "]");

        return offset;
    }

    /**
     * Returns the latest offset for the partition
     *
     * @return the latest offset for the partition
     *
     * @throws IllegalStateException if the latest offset could not be looked up
     */
    protected long getLatestOffset() {
        Map<TopicPartition, Long> offsets;

        try {
            offsets = consumer.endOffsets(Collections.singleton(topicPartition));
        } catch (TimeoutException | InterruptException e) {
            throw new IllegalStateException("Unable to look up latest offset for topic partition [" + topicPartition + "]", e);
        }

        if (!offsets.containsKey(topicPartition))
            throw new IllegalStateException("Unable to look up latest offset for topic partition [" + topicPartition + "]");

        Long offset = offsets.get(topicPartition);

        if (offset == null)
            throw new IllegalStateException("Unable to look up latest offset for topic partition [" + topicPartition + "]");

        return offset;
    }

    /**
     * Reset the partition to the last committed value
     *
     * @throws KafkaException if the consumer position cannot be set
     */
    private void resetToCommitted() {
        LOGGER.debug("seeking [{}] to last committed offset [{}]", topicPartition, lastCommittedOffset);

        consumer.seek(topicPartition, lastCommittedOffset);

        // Update our current position
        offsetPosition = lastCommittedOffset;
    }

    private void maybeUpdateCommitableOffset(long ackedOffset) {
        Long offset = committableOffset;

        if (offset == null) {
            // If we don't have a committable offset use the last committed offset
            offset = lastCommittedOffset;

            LOGGER.debug("No committable offset yet for partition [{}]. Starting from last committed offset [{}]",
                    topicPartition, offset);
        }

        // If the latest acked record matches our current committable offset we can bump the committable offset up
        // commitableOffset represents the record Kafka should read next (i.e. latest committed record + 1)
        if (offset.equals(ackedOffset)) {
            if (LOGGER.isDebugEnabled())
                LOGGER.debug("Next committable offset [{}] matches recently acked record for partition [{}]. Increasing" +
                    " committable offset", offset, topicPartition);

            offset++;

            // Since we bumped the committable offset up 1 we need to check if this now frees us to move the committable
            // offset higher due to previously completed work
            Iterator<Long> eligibleOffsets = completedOffsets.tailSet(offset).iterator();

            while(eligibleOffsets.hasNext() && eligibleOffsets.next().equals(offset))
                offset++;

            LOGGER.debug("Bumped committable offset to [{}] for partition [{}]", offset, topicPartition);

            committableOffset = offset;
        }
    }

    /**
     * The committable offset for this partition or {@code null} if there is nothing to commit
     *
     * @return committable offset for this partition or {@code null} if there is nothing to commit
     */
    public OffsetAndMetadata getCommittableOffset() {
        if (committableOffset == null)
            return null;

        return new OffsetAndMetadata(committableOffset);
    }

    /**
     * Return the total number of committable records for this partition
     *
     * @return the total number of committable records for this partition
     */
    public long getCommittableOffsetsSize() {
        // If we don't have a committable offset yet there is nothing to commit
        if (committableOffset == null)
            return 0;

        return committableOffset - lastCommittedOffset;
    }

    /**
     * Tells the partition that the following offset was committed to Kafka
     *
     * @param committedOffset
     *      the offset committed to Kafka for the partition
     */
    public void committedOffset(long committedOffset) {
        LOGGER.debug("Offset [{}] has been committed for partition [{}]. Removing all completed offsets below commit value",
                committedOffset, topicPartition);

        lastCommittedOffset = committedOffset;

        // Reset committable offset to nothing since we just committed
        committableOffset = null;

        // Remove all completed offsets less than the committed offset
        completedOffsets.removeIf(o -> o < committedOffset);

        PARTITION_COMMITTED_OFFSETS.update(committedOffset - lastCommittedOffset);
    }

    /**
     * Add the given records to the partition to be read
     *
     * @param records
     *          the records to be read next
     */
    public void load(List<ConsumerRecord<K, V>> records) {
        List<ConsumerRecord<K, V>> recordsToAdd = new ArrayList<>();
        LOGGER.debug("Loading records for partition [{}]", topicPartition);

        if (hasNextRecord()) {
            LOGGER.debug("Combining existing records with added records for partition [{}]", topicPartition);
            this.records.forEachRemaining(recordsToAdd::add);
        }

        recordsToAdd.addAll(records);

        this.records = recordsToAdd.iterator();
    }

    /**
     * Read the next record
     *
     * @return
     *          the next record or {@code null} if there is no record to be read
     */
    public ConsumerRecord<K, V> nextRecord() {
        LOGGER.debug("Reading next record for partition [{}]", topicPartition);
        ConsumerRecord<K, V> record = null;

        while(record == null && hasNextRecord()) {
            record = records.next();

            // Update our current position to the next record we should read
            offsetPosition = record.offset() + 1;

            // We may re-read some messages so we should skip any that are already completed (and not committed),
            // currently pending, or committed
            if (completedOffsets.contains(record.offset())) {
                LOGGER.debug("Skipping record for partition [{}] with offset [{}] as it is completed",
                        topicPartition, record.offset());
                record = null;
            }
            else if (pendingOffsets.containsKey(record.offset())) {
                LOGGER.debug("Skipping record for partition [{}] with offset [{}] as it is pending",
                        topicPartition, record.offset());
                record = null;
            }
            else if (record.offset() < lastCommittedOffset) {
                LOGGER.debug("Skipping record for partition [{}] with offset [{}] as it has already been committed",
                        topicPartition, record.offset());
                record = null;
            }
        }

        if (record == null) {
            LOGGER.debug("No record to return for partition [{}]", topicPartition);
            return null;
        }

        LOGGER.debug("Adding record [{}] to pending for partition [{}]", record.offset(), topicPartition);

        // Add to pending
        pendingOffsets.put(record.offset(), System.currentTimeMillis());

        return record;
    }

    /**
     * Returns {@code true} if there is another record to be read
     *
     * @return
     *      {@code true} if there is another record to be read
     */
    public boolean hasNextRecord() {
        return !paused && records.hasNext();
    }

    /**
     * Returns the partition associated to this object
     *
     * @return the partition associated to this object
     */
    public TopicPartition getTopicPartition() {
        return topicPartition;
    }

    @Override
    public void close() throws IOException {
        // This method does not throw a KafkaException
        if (consumer.paused().contains(topicPartition))
            PAUSED_PARTITIONS.dec();
    }
}
