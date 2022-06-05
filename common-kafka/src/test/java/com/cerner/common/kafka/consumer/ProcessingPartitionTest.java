package com.cerner.common.kafka.consumer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.log4j.spi.RootLogger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
@SuppressWarnings("unchecked")
public class ProcessingPartitionTest {

    private TestLogAppender logAppender;

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Mock
    Consumer<String, String> consumer;

    Properties properties;
    ProcessingConfig config;
    TopicPartition topicPartition;
    MockProcessingPartition<String, String> partition;

    @Before
    public void before() {
        properties = new Properties();
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-group");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.EARLIEST.toString().toLowerCase());

        config = new ProcessingConfig(properties);
        topicPartition = new TopicPartition("topic", 1);

        when(consumer.committed(topicPartition)).thenReturn(new OffsetAndMetadata(0L));

        partition = new MockProcessingPartition<>(topicPartition, config, consumer);

        logAppender = new TestLogAppender();
        RootLogger.getRootLogger().addAppender(logAppender);
    }

    @After
    public void after() {
        RootLogger.getRootLogger().removeAppender(logAppender);
    }

    @Test
    public void load() {
        partition.load(Arrays.asList(record(1L), record(2L), record(3L)));

        assertThat(partition.records.hasNext(), is(true));
        assertRecordsAreEqual(partition.records.next(), record(1L));

        assertThat(partition.records.hasNext(), is(true));
        assertRecordsAreEqual(partition.records.next(), record(2L));

        assertThat(partition.records.hasNext(), is(true));
        assertRecordsAreEqual(partition.records.next(), record(3L));

        assertThat(partition.records.hasNext(), is(false));
    }

    @Test
    public void load_withExistingRecords() {
        partition.load(Arrays.asList(record(1L)));
        partition.load(Arrays.asList(record(2L)));

        assertThat(partition.records.hasNext(), is(true));
        assertRecordsAreEqual(partition.records.next(), record(1L));

        assertThat(partition.records.hasNext(), is(true));
        assertRecordsAreEqual(partition.records.next(), record(2L));

        assertThat(partition.records.hasNext(), is(false));
    }

    @Test
    public void ack_recordNotPending() {
        long previousProcessingLatencyCount = ProcessingPartition.PROCESSING_LATENCY.count();

        assertThat(partition.ack(1L), is(false));

        assertThat(ProcessingPartition.PROCESSING_LATENCY.count(), is(previousProcessingLatencyCount));
    }

    @Test
    public void ack_recordPending() {
        long previousProcessingLatencyCount = ProcessingPartition.PROCESSING_LATENCY.count();

        // Add record to partition to be processed
        partition.load(Arrays.asList(record(0L)));

        // Read record which should now be pending
        assertRecordsAreEqual(partition.nextRecord(), record(0L));
        assertThat(partition.pendingOffsets.keySet(), contains(0L));

        // Ack record
        assertThat(partition.ack(0L), is(true));

        assertThat(ProcessingPartition.PROCESSING_LATENCY.count(), is(previousProcessingLatencyCount + 1));

        // Record should no longer be pending but completed
        assertThat(partition.pendingOffsets.keySet(), empty());
        assertThat(partition.completedOffsets, contains(0L));
        assertThat(partition.committableOffset, is(1L));
        assertThat(partition.lastCommittedOffset, is(0L));
        assertThat(partition.getCommittableOffsetsSize(), is(1L));
    }

    @Test
    public void nextRecord_manyRecords() {
        partition.load(Arrays.asList(record(1L), record(2L), record(3L)));

        assertThat(partition.hasNextRecord(), is(true));
        assertRecordsAreEqual(partition.nextRecord(), record(1L));

        assertThat(partition.pendingOffsets.keySet(), contains(1L));
        assertThat(partition.offsetPosition, is(2L));

        assertThat(partition.hasNextRecord(), is(true));
        assertRecordsAreEqual(partition.nextRecord(), record(2L));

        assertThat(partition.pendingOffsets.keySet(), containsInAnyOrder(1L, 2L));
        assertThat(partition.offsetPosition, is(3L));

        assertThat(partition.hasNextRecord(), is(true));
        assertRecordsAreEqual(partition.nextRecord(), record(3L));

        assertThat(partition.pendingOffsets.keySet(), containsInAnyOrder(1L, 2L, 3L));
        assertThat(partition.offsetPosition, is(4L));

        assertThat(partition.hasNextRecord(), is(false));
        assertThat(partition.nextRecord(), is(nullValue()));

        assertThat(partition.pendingOffsets.keySet(), containsInAnyOrder(1L, 2L, 3L));
        assertThat(partition.offsetPosition, is(4L));

        assertThat(partition.committableOffset, is(nullValue()));
        assertThat(partition.getCommittableOffsetsSize(), is(0L));
    }

    @Test
    public void fail_notPending() {
        assertThat(partition.fail(1L), is(false));
        verify(consumer, never()).seek(topicPartition, 0);
        verify(consumer, never()).pause(Collections.singleton(topicPartition));
    }

    @Test
    public void fail_withCommitForPartition() {
        // Add record to partition to be processed
        partition.load(Arrays.asList(record(0L)));

        // Read record which should now be pending
        assertRecordsAreEqual(partition.nextRecord(), record(0L));
        assertThat(partition.pendingOffsets.keySet(), contains(0L));

        // Fail record
        assertThat(partition.fail(0L), is(true));

        // Record should no longer be pending and should not be completed
        assertThat(partition.pendingOffsets.keySet(), empty());
        assertThat(partition.completedOffsets, empty());

        // Our consumer had a commit for this partition so it should have used to re-wind
        verify(consumer).seek(topicPartition, 0L);

        // We have not had enough failures to cause us to pause
        verify(consumer, never()).pause(Collections.singleton(topicPartition));
    }

    @Test
    public void fail_offsetPositionBeforeFailedRecord() {

        // Add record to partition to be processed, give it a high offset
        partition.load(Arrays.asList(record(1L), record(2L)));

        // Read record which should now be pending
        assertRecordsAreEqual(partition.nextRecord(), record(1L));
        assertThat(partition.pendingOffsets.keySet(), contains(1L));
        assertThat(partition.offsetPosition, is(2L));

        // Read second record
        assertRecordsAreEqual(partition.nextRecord(), record(2L));
        assertThat(partition.pendingOffsets.keySet(), containsInAnyOrder(1L, 2L));
        assertThat(partition.offsetPosition, is(3L));

        // Fail record first record
        assertThat(partition.fail(1L), is(true));

        // Record should no longer be pending and should not be completed
        assertThat(partition.pendingOffsets.keySet(), contains(2L));
        assertThat(partition.completedOffsets, empty());

        // Our consumer had a commit for this partition so it should have used to re-wind
        verify(consumer).seek(topicPartition, 0L);
        assertThat(partition.offsetPosition, is(0L));

        // Fail record second record
        assertThat(partition.fail(2L), is(true));

        // Record should no longer be pending and should not be completed
        assertThat(partition.pendingOffsets.keySet(), empty());
        assertThat(partition.completedOffsets, empty());

        // Consumer should have only done 1 seek since it did a rewind to a previous offset so we are set to re-process
        // the second record too and don't need to rewind again
        verify(consumer).seek(topicPartition, 0L);
        assertThat(partition.offsetPosition, is(0L));
    }

    @Test
    public void nextRecord_skipPendingAndCompletedRecords() {
        partition.load(Arrays.asList(record(0L), record(1L), record(2L)));

        // Read all records
        assertRecordsAreEqual(partition.nextRecord(), record(0L));
        assertRecordsAreEqual(partition.nextRecord(), record(1L));
        assertRecordsAreEqual(partition.nextRecord(), record(2L));

        assertThat(partition.hasNextRecord(), is(false));
        assertThat(partition.offsetPosition, is(3L));
        assertThat(partition.pendingOffsets.keySet(), containsInAnyOrder(0L, 1L, 2L));
        assertThat(partition.completedOffsets, empty());

        // Ack an early record
        assertThat(partition.ack(0L), is(true));

        assertThat(partition.pendingOffsets.keySet(), containsInAnyOrder(1L, 2L));
        assertThat(partition.completedOffsets, contains(0L));

        assertThat(partition.committableOffset, is(1L));
        assertThat(partition.lastCommittedOffset, is(0L));
        assertThat(partition.getCommittableOffsetsSize(), is(1L));

        // Fail the latest record
        assertThat(partition.fail(2L), is(true));

        assertThat(partition.pendingOffsets.keySet(), contains(1L));
        assertThat(partition.completedOffsets, contains(0L));

        assertThat(partition.committableOffset, is(1L));
        assertThat(partition.lastCommittedOffset, is(0L));
        assertThat(partition.getCommittableOffsetsSize(), is(1L));

        verify(consumer).seek(topicPartition, 0L);
        assertThat(partition.offsetPosition, is(0L));

        // Since we seeked to the beginning assume we re-read those same messages starting from 0
        partition.load(Arrays.asList(record(0L), record(1L), record(2L)));

        // nextRecord() should skip record 0 (completed) and record 1 (pending) as we've processed them
        assertRecordsAreEqual(partition.nextRecord(), record(2L));

        assertThat(partition.hasNextRecord(), is(false));
        assertThat(partition.offsetPosition, is(3L));
        assertThat(partition.pendingOffsets.keySet(), containsInAnyOrder(1L, 2L));
        assertThat(partition.completedOffsets, contains(0L));

        assertThat(partition.committableOffset, is(1L));
        assertThat(partition.lastCommittedOffset, is(0L));
        assertThat(partition.getCommittableOffsetsSize(), is(1L));
    }

    @Test
    public void commitableOffsets() {
        // Load some data
        partition.load(Arrays.asList(record(0L), record(1L), record(2L), record(3L), record(4L)));

        // We haven't processed anything so nothing should be commitable
        assertThat(partition.getCommittableOffset(), is(nullValue()));

        // Read records
        assertRecordsAreEqual(partition.nextRecord(), record(0L));
        assertRecordsAreEqual(partition.nextRecord(), record(1L));
        assertRecordsAreEqual(partition.nextRecord(), record(2L));
        assertRecordsAreEqual(partition.nextRecord(), record(3L));
        assertRecordsAreEqual(partition.nextRecord(), record(4L));

        // We haven't successfully processed anything so nothing should be commitable
        assertThat(partition.getCommittableOffset(), is(nullValue()));

        // Ack only some records
        assertThat(partition.ack(2L), is(true));
        assertThat(partition.ack(4L), is(true));

        // Nothing is committable since 0 is still pending
        assertThat(partition.getCommittableOffset(), is(nullValue()));
        assertThat(partition.lastCommittedOffset, is(0L));
        assertThat(partition.getCommittableOffsetsSize(), is(0L));

        assertThat(partition.ack(0L), is(true));

        // Only 0 is committable since 1 is pending (we commit 1 more than the latest committable record)
        assertThat(partition.getCommittableOffset(), is(new OffsetAndMetadata(1L)));
        assertThat(partition.getCommittableOffsetsSize(), is(1L));

        assertThat(partition.fail(1L), is(true));

        // Only 0 is committable since 1 has not been processed
        assertThat(partition.getCommittableOffset(), is(new OffsetAndMetadata(1L)));
        assertThat(partition.getCommittableOffsetsSize(), is(1L));

        // Add record 1 into queue (as if we re-read it)
        partition.load(Arrays.asList(record(1L)));
        assertRecordsAreEqual(partition.nextRecord(), record(1L));

        // Only 0 is committable since 1 is still pending
        assertThat(partition.getCommittableOffset(), is(new OffsetAndMetadata(1L)));
        assertThat(partition.getCommittableOffsetsSize(), is(1L));

        assertThat(partition.ack(1L), is(true));

        // Now that we acked 1 we can commit up to 2
        assertThat(partition.getCommittableOffset(), is(new OffsetAndMetadata(3L)));
        assertThat(partition.getCommittableOffsetsSize(), is(3L));

        // Fail a record (cause a rewind to re-read failed record)
        assertThat(partition.fail(3L), is(true));

        // Even though we failed a record we can still commit up to 2
        assertThat(partition.getCommittableOffset(), is(new OffsetAndMetadata(3L)));
        assertThat(partition.getCommittableOffsetsSize(), is(3L));
    }

    @Test
    public void pause_thresholdMet() {
        long previousPausedPartitions = ProcessingPartition.PAUSED_PARTITIONS.count();
        long previousPausedMeterCount = ProcessingPartition.PAUSED_METER.count();

        // Set sample size really low
        properties.setProperty(ProcessingConfig.FAIL_SAMPLE_SIZE_PROPERTY, "3");

        partition = new MockProcessingPartition<>(topicPartition, new ProcessingConfig(properties), consumer);

        // Add some records
        partition.load(Arrays.asList(record(0L), record(1L), record(2L)));

        assertRecordsAreEqual(partition.nextRecord(), record(0L));
        assertRecordsAreEqual(partition.nextRecord(), record(1L));

        // No failures yet so should not be paused
        assertThat(partition.paused, is(false));
        verify(consumer, never()).pause(Collections.singleton(topicPartition));

        assertThat(partition.fail(0L), is(true));
        verify(consumer).seek(topicPartition, 0L);

        // Although we failed we haven't hit our threshold yet (>= 50% of 3)
        assertThat(partition.paused, is(false));
        verify(consumer, never()).pause(Collections.singleton(topicPartition));

        long currentTime = System.currentTimeMillis();

        // This should put us over our threshold and pause our partition
        assertThat(partition.fail(1L), is(true));

        assertThat(partition.paused, is(true));
        verify(consumer).pause(Collections.singleton(topicPartition));
        assertThat(ProcessingPartition.PAUSED_PARTITIONS.count(), is(previousPausedPartitions + 1));
        assertThat(ProcessingPartition.PAUSED_METER.count(), is(previousPausedMeterCount + 1));

        assertThat(partition.pausedTillTime, is(greaterThanOrEqualTo(currentTime + config.getFailPauseTime())));

        // We should have wiped our records (record 2) which will be re-read when we seeked previously
        assertThat(partition.records.hasNext(), is(false));
    }

    @Test
    public void pause_moreRecordsLoaded() throws InterruptedException {
        pause_thresholdMet();

        assertThat(partition.paused, is(true));

        // Add more records (lets pretend the consumer had these queued up to give to us before we paused)
        partition.load(Arrays.asList(record(2L), record(3L)));

        // We should not be able to read those records since we are paused
        assertThat(partition.hasNextRecord(), is(false));
        assertThat(partition.nextRecord(), is(nullValue()));

        // Un-pause the partition
        partition.maybeUnpause(System.currentTimeMillis() + config.getFailPauseTime());
        assertThat(partition.paused, is(false));

        // We should now be able to read those records
        assertThat(partition.hasNextRecord(), is(true));
        assertRecordsAreEqual(partition.nextRecord(), record(2L));

        assertThat(partition.hasNextRecord(), is(true));
        assertRecordsAreEqual(partition.nextRecord(), record(3L));

        assertThat(partition.hasNextRecord(), is(false));
    }

    @Test
    public void pause_thenClosePartition() throws IOException {
        when(consumer.paused()).thenReturn(Collections.singleton(topicPartition));

        long previousPausedPartitions = ProcessingPartition.PAUSED_PARTITIONS.count();

        pause_thresholdMet();

        assertThat(ProcessingPartition.PAUSED_PARTITIONS.count(), is(previousPausedPartitions + 1));

        partition.close();

        assertThat(ProcessingPartition.PAUSED_PARTITIONS.count(), is(previousPausedPartitions));
    }

    @Test
    public void maybeUnpause_notPaused() {
        partition.maybeUnpause(System.currentTimeMillis());

        assertThat(partition.paused, is(false));
        verify(consumer, never()).resume(Collections.singleton(topicPartition));
    }

    @Test
    public void maybeUnpause_paused() {
        long previousPausedPartitions = ProcessingPartition.PAUSED_PARTITIONS.count();
        long currentTime = System.currentTimeMillis();

        partition.paused = true;
        partition.recentFailedResults = config.getFailSampleSize();
        partition.recentProcessingResults = new LinkedList<>(Collections.nCopies(config.getFailSampleSize(), false));

        // Set to pause till pause time from current time
        partition.pausedTillTime = currentTime + config.getFailPauseTime();

        // This shouldn't un-pause us since not enough time has passed
        partition.maybeUnpause(System.currentTimeMillis());

        assertThat(partition.paused, is(true));
        verify(consumer, never()).resume(Collections.singleton(topicPartition));

        // This time is far enough in the future to allow us to un-pause
        partition.maybeUnpause(currentTime + config.getFailPauseTime());

        // Verify everything was reset
        assertThat(partition.paused, is(false));
        assertThat(ProcessingPartition.PAUSED_PARTITIONS.count(), is(previousPausedPartitions - 1));
        verify(consumer).resume(Collections.singleton(topicPartition));
        assertThat(partition.recentFailedResults, is(0));
        assertThat(partition.recentProcessingResults,
                is(new LinkedList<>(Collections.nCopies(config.getFailSampleSize(), true))));
    }

    @Test
    public void getLastCommittedOffset_noCommittedOffset() {
        when(consumer.committed(topicPartition)).thenReturn(null);

        // We set the reset strategy to earliest so this should give earliest offset
        assertThat(partition.getLastCommittedOffset(), is(partition.getEarliestOffset()));
        verify(consumer).commitSync(Collections.singletonMap(topicPartition, any(OffsetAndMetadata.class)));
    }

    @Test
    public void getLastCommittedOffset_noCommittedOffset_initialCommitDisabled() {
        properties.put(ProcessingConfig.COMMIT_INITIAL_OFFSET_PROPERTY, String.valueOf(false));

        config = new ProcessingConfig(properties);

        partition = new MockProcessingPartition<>(topicPartition, config, consumer);

        when(consumer.committed(topicPartition)).thenReturn(null);

        // We set the reset strategy to earliest so this should give earliest offset
        assertThat(partition.getLastCommittedOffset(), is(partition.getEarliestOffset()));
        verify(consumer, never()).commitSync(any(Map.class));
    }

    @Test
    public void getLastCommittedOffset_committedOffsetBeforeEarliest() {
        when(consumer.committed(topicPartition)).thenReturn(new OffsetAndMetadata(0L));

        // Increase earliest broker offset to be above committed
        partition.earliestBrokerOffset = 1L;
        partition.latestBrokerOffset = 100L;

        // Since our offset is out of range we will fall back to the offset based on our reset strategy
        // We set the reset strategy to earliest so this should give earliest offset
        assertThat(partition.getLastCommittedOffset(), is(1L));
        verify(consumer).commitSync(Collections.singletonMap(topicPartition, any(OffsetAndMetadata.class)));
    }

    @Test
    public void getLastCommittedOffset_committedOffsetEqualsEarliest() {
        when(consumer.committed(topicPartition)).thenReturn(new OffsetAndMetadata(0L));
        partition.earliestBrokerOffset = 0L;
        partition.latestBrokerOffset = 100L;

        assertThat(partition.getLastCommittedOffset(), is(0L));
        verify(consumer, never()).commitSync(any(Map.class));
    }

    @Test
    public void getLastCommittedOffset_committedOffsetAfterLatest() {
        when(consumer.committed(topicPartition)).thenReturn(new OffsetAndMetadata(200L));

        partition.earliestBrokerOffset = 25L;

        // Decrease latest broker offset to be below committed
        partition.latestBrokerOffset = 100L;

        // Since our offset is out of range we will fall back to the offset based on our reset strategy
        // We set the reset strategy to earliest so this should give earliest offset
        assertThat(partition.getLastCommittedOffset(), is(25L));
        verify(consumer).commitSync(Collections.singletonMap(topicPartition, any(OffsetAndMetadata.class)));
    }

    @Test
    public void getLastCommittedOffset_committedOffsetEqualsLatest() {
        when(consumer.committed(topicPartition)).thenReturn(new OffsetAndMetadata(123L));
        partition.earliestBrokerOffset = 10L;
        partition.latestBrokerOffset = 123L;

        assertThat(partition.getLastCommittedOffset(), is(123L));
        verify(consumer, never()).commitSync(any(Map.class));
    }

    @Test
    public void getLastCommittedOffset_committedOffset() {
        when(consumer.committed(topicPartition)).thenReturn(new OffsetAndMetadata(123L));
        partition.earliestBrokerOffset = 100L;
        partition.latestBrokerOffset = 250L;

        assertThat(partition.getLastCommittedOffset(), is(123L));
        verify(consumer, never()).commitSync(any(Map.class));
    }

    @Test
    public void getLastCommittedOffset_noConsumerCommit_failedToCommit() {
        when(consumer.committed(topicPartition)).thenReturn(null);
        partition.earliestBrokerOffset = 123L;
        doThrow(new KafkaException("unable to commit")).when(consumer).commitSync(anyMap());

        partition.getLastCommittedOffset();
        verify(consumer).commitSync(Collections.singletonMap(topicPartition, new OffsetAndMetadata(123L)));
        assertThat(logAppender.getLogs(),
                hasItem("Unable to commit reset offset 123 during initialization of partition " + topicPartition +
                        " for group my-group"));
    }

    @Test
    public void committedOffset() {
        long previousCommittedOffsets = ProcessingPartition.PARTITION_COMMITTED_OFFSETS.count();

        partition.lastCommittedOffset = 0L;
        partition.committedOffset(123L);

        assertThat(ProcessingPartition.PARTITION_COMMITTED_OFFSETS.count(), is(previousCommittedOffsets + 1L));
    }

    @Test
    public void getResetOffset_configuredToEarliest() {
        properties = new Properties();
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.EARLIEST.toString().toLowerCase());

        config = new ProcessingConfig(properties);

        partition = new MockProcessingPartition<>(topicPartition, config, consumer);

        assertThat(partition.getResetOffset(), is(partition.getEarliestOffset()));
    }

    @Test
    public void getResetOffset_configuredToLatest() {
        properties = new Properties();
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.LATEST.toString().toLowerCase());

        config = new ProcessingConfig(properties);

        partition = new MockProcessingPartition<>(topicPartition, config, consumer);

        assertThat(partition.getResetOffset(), is(partition.getLatestOffset()));
    }

    private void assertRecordsAreEqual(ConsumerRecord<String, String> record1, ConsumerRecord<String, String> record2) {
        assertThat("Record [" + record1 + "] topic does not match record [" + record2 + "]", record1.topic(),
                is(record2.topic()));
        assertThat("Record [" + record1 + "] partition does not match record [" + record2 + "]", record1.partition(),
                is(record2.partition()));
        assertThat("Record [" + record1 + "] offset does not match record [" + record2 + "]", record1.offset(),
                is(record2.offset()));
        assertThat("Record [" + record1 + "] key does not match record [" + record2 + "]", record1.key(),
                is(record2.key()));
        assertThat("Record [" + record1 + "] value does not match record [" + record2 + "]", record1.value(),
                is(record2.value()));
    }

    private ConsumerRecord<String, String> record(long offset) {
        return new ConsumerRecord<>(topicPartition.topic(), topicPartition.partition(), offset, "key" +
                offset, "value" + offset);
    }

    // Used to override behavior to query brokers about earliest/latest offsets
    public static class MockProcessingPartition<K, V> extends ProcessingPartition<K, V> {

        public Long earliestBrokerOffset;
        public Long latestBrokerOffset;

        public MockProcessingPartition(TopicPartition topicPartition, ProcessingConfig config, Consumer<K, V> consumer) {
            super(topicPartition, config, consumer);
        }

        @Override
        protected long getEarliestOffset() {
            return earliestBrokerOffset == null ? 0L : earliestBrokerOffset;
        }

        @Override
        protected long getLatestOffset() {
            return latestBrokerOffset == null ? Long.MAX_VALUE : latestBrokerOffset;
        }
    }

    private static class TestLogAppender extends AppenderSkeleton {
        private final List<LoggingEvent> log = new ArrayList<>();

        @Override
        public boolean requiresLayout() {
            return false;
        }

        @Override
        protected void append(final LoggingEvent loggingEvent) {
            log.add(loggingEvent);
        }

        @Override
        public void close() {
        }

        public List<String> getLogs() {
            return log.stream().map(LoggingEvent::getRenderedMessage).collect(Collectors.toList());
        }
    }
}
