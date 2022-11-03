package com.cerner.common.kafka.consumer;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.cerner.common.kafka.consumer.ProcessingPartitionTest.MockProcessingPartition;

@RunWith(MockitoJUnitRunner.class)
@SuppressWarnings("unchecked")
public class ProcessingKafkaConsumerTest {

    @Mock
    Consumer<String, String> consumer;
    @Captor
    ArgumentCaptor<Map<TopicPartition, OffsetAndMetadata>> commitCaptor;

    ProcessingKafkaConsumer<String, String> processingConsumer;

    private static final long POLL_TIME=100L;

    String topic = "topic";
    int partition = 1;
    long offset = 123L;
    String key = "key";
    String value = "value";
    TopicPartition topicPartition;
    Collection<TopicPartition> topicPartitions;

    Properties properties;
    ProcessingConfig config;

    ConsumerRecord<String, String> record1;
    ConsumerRecord<String, String> record2;
    ConsumerRecord<String, String> record3;
    ConsumerRecord<String, String> record4;
    ConsumerRecord<String, String> record5;
    ConsumerRecord<String, String> record6;

    @Before
    public void before() {
        topicPartition = new TopicPartition(topic, partition);
        topicPartitions = Collections.singleton(topicPartition);

        // Single record - for simple use cases
        record1 = new ConsumerRecord<>(topic, partition, offset, key, value);
        ConsumerRecords<String, String> firstSet = new ConsumerRecords<>(Collections.singletonMap(topicPartition,
                Arrays.asList(record1)));

        // More records
        record2 = new ConsumerRecord<>(topic, partition, offset + 1, key + 1, value + 1);
        record3 = new ConsumerRecord<>(topic, partition, offset + 2, key + 2, value + 2);
        ConsumerRecords<String, String> secondSet = new ConsumerRecords<>(Collections.singletonMap(topicPartition,
                Arrays.asList(record2, record3)));

        // Records with different topics/partitions
        record4 = new ConsumerRecord<>(topic + 1, partition + 1, offset + 3, key + 3, value + 3);
        record5 = new ConsumerRecord<>(topic + 2, partition + 2, offset + 4, key + 4, value + 4);
        record6 = new ConsumerRecord<>(topic + 3, partition + 3, offset + 5, key + 5, value + 5);

        Map<TopicPartition, List<ConsumerRecord<String, String>>> thirdSetMap = new HashMap<>();
        thirdSetMap.put(new TopicPartition(record4.topic(), record4.partition()), Arrays.asList(record4));
        thirdSetMap.put(new TopicPartition(record5.topic(), record5.partition()), Arrays.asList(record5));
        thirdSetMap.put(new TopicPartition(record6.topic(), record6.partition()), Arrays.asList(record6));
        ConsumerRecords<String, String> thirdSet = new ConsumerRecords<>(thirdSetMap);

        when(consumer.poll(any(Duration.class))).thenReturn(firstSet).thenReturn(secondSet).thenReturn(thirdSet);
        when(consumer.committed(topicPartition)).thenReturn(new OffsetAndMetadata(offset));
        when(consumer.committed(new TopicPartition(record4.topic(), record4.partition())))
                .thenReturn(new OffsetAndMetadata(record4.offset()));
        when(consumer.committed(new TopicPartition(record5.topic(), record5.partition())))
                .thenReturn(new OffsetAndMetadata(record5.offset()));
        when(consumer.committed(new TopicPartition(record6.topic(), record6.partition())))
                .thenReturn(new OffsetAndMetadata(record6.offset()));

        properties = new Properties();

        // Config needed by KafkaConsumer when we create it
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:123");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "group");

        // KafkaConsumer is very picky and wants this to be lower case. This is also need by the processing consumer
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.EARLIEST.toString().toLowerCase());

        // Build the consumer
        rebuildConsumer();
    }

    @Test(expected = IllegalArgumentException.class)
    public void constructorWithConsumer_nullConfig() {
        new ProcessingKafkaConsumer(null, consumer);
    }

    @Test(expected = IllegalArgumentException.class)
    public void constructor_nullConsumer() {
        new ProcessingKafkaConsumer(config, null);
    }

    @Test
    public void constructorWithoutConsumer() throws IOException {
        try (ProcessingKafkaConsumer<String, String> processingKafkaConsumer = new ProcessingKafkaConsumer<>(config)) {
            assertThat(processingKafkaConsumer.getConsumer(), is(instanceOf(KafkaConsumer.class)));
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void constructorWithoutConsumer_nullConfig() {
            new ProcessingKafkaConsumer(null);
    }

    @Test
    public void constructorWithConsumer() {
        assertThat(processingConsumer.getConsumer(), is(consumer));
    }

    @Test
    public void constructorWithDeserializers_nullConfig() {
        Deserializer<String> keyDeserializer = new StringDeserializer();
        Deserializer<String> valueDeserializer = new StringDeserializer();
        try {
            new ProcessingKafkaConsumer(null, keyDeserializer, valueDeserializer);
            Assert.fail("Expected IllegalArgumentException to be thrown");
        } catch (IllegalArgumentException e) {
        }
    }

    @Test
    public void constructorWithDeserializers() throws IOException {
        Deserializer<String> keyDeserializer = new StringDeserializer();
        Deserializer<String> valueDeserializer = new StringDeserializer();
        try (ProcessingKafkaConsumer<String, String> processingKafkaConsumer =
                 new ProcessingKafkaConsumer<>(config, keyDeserializer, valueDeserializer)) {
            assertThat(processingKafkaConsumer.getConsumer(), is(instanceOf(KafkaConsumer.class)));
        }
    }


    @Test
    public void nextRecord() {
        long previousReadLatencyCount = ProcessingKafkaConsumer.READ_LATENCY.count();
        long previousPollMessagesCount = ProcessingKafkaConsumer.POLL_MESSAGES.count();
        long previousPollLatencyCount = ProcessingKafkaConsumer.POLL_LATENCY.count();

        assertThat(nextRecordIsPresent(), is(record1));
        verify(consumer).poll(Duration.ofMillis(POLL_TIME));
        assertThat(ProcessingKafkaConsumer.READ_LATENCY.count(), is(previousReadLatencyCount + 1));
        assertThat(ProcessingKafkaConsumer.POLL_MESSAGES.count(), is(previousPollMessagesCount + 1));

        // Since we only called poll once we can't determine a latency (difference between polls)
        assertThat(ProcessingKafkaConsumer.POLL_LATENCY.count(), is(previousPollLatencyCount));
    }

    @Test
    public void nextRecord_noPartitionsWithMessagesToRead() {
        // Read all available records from first batch #poll()
        nextRecord();

        // This should cause us to do another #poll() and read the next record
        assertThat(nextRecordIsPresent(), is(record2));
        verify(consumer, times(2)).poll(Duration.ofMillis(POLL_TIME));
    }

    @Test
    public void nextRecord_multipleReads() {
        long previousPollLatencyCount = ProcessingKafkaConsumer.POLL_LATENCY.count();

        assertThat(nextRecordIsPresent(), is(record1));
        verify(consumer).poll(Duration.ofMillis(POLL_TIME));

        // Since we only called poll once we can't determine a latency (difference between polls)
        assertThat(ProcessingKafkaConsumer.POLL_LATENCY.count(), is(previousPollLatencyCount));

        // Reading again should #poll() and read the next set of messages
        assertThat(nextRecordIsPresent(), is(record2));
        assertThat(nextRecordIsPresent(), is(record3));

        // Since we have only called #poll() twice we should not yet have a latency measurement
        assertThat(ProcessingKafkaConsumer.POLL_LATENCY.count(), is(previousPollLatencyCount));

        verify(consumer, times(2)).poll(Duration.ofMillis(POLL_TIME));

        // There is only 2 message in the second set so this should read the 3rd batch
        assertThat(nextRecordIsPresent(), is(record4));
        verify(consumer, times(3)).poll(Duration.ofMillis(POLL_TIME));

        // This tracks the time from the start of the 2nd poll to the start of the 3rd poll
        assertThat(ProcessingKafkaConsumer.POLL_LATENCY.count(), is(previousPollLatencyCount + 1));
    }

    @Test
    public void nextRecord_fairProcessing() {
        TopicPartition topic1Partition1 = new TopicPartition("topic1", 1);
        TopicPartition topic1Partition2 = new TopicPartition("topic1", 2);
        TopicPartition topic2Partition1 = new TopicPartition("topic2", 1);
        TopicPartition topic2Partition2 = new TopicPartition("topic2", 2);

        when(consumer.committed(topic1Partition1)).thenReturn(new OffsetAndMetadata(0L));
        when(consumer.committed(topic1Partition2)).thenReturn(new OffsetAndMetadata(0L));
        when(consumer.committed(topic2Partition1)).thenReturn(new OffsetAndMetadata(0L));
        when(consumer.committed(topic2Partition2)).thenReturn(new OffsetAndMetadata(0L));

        List<TopicPartition> topicPartitions = Arrays.asList(topic1Partition1, topic1Partition2, topic2Partition1,
                topic2Partition2);

        Map<TopicPartition, List<ConsumerRecord<String, String>>> recordsMap = new HashMap<>();

        // Provide 3 records per partition
        topicPartitions.forEach(tp -> {
            List<ConsumerRecord<String, String>> records = new ArrayList<>();
            for(int message=0; message<3; message++) {
                records.add(new ConsumerRecord<>(tp.topic(), tp.partition(), message, "key", tp.toString() + " Message: " + message));
            }
            recordsMap.put(tp, records);
        });

        // Setup consumer to read these records
        ConsumerRecords<String, String> records = new ConsumerRecords<>(recordsMap);
        when(consumer.poll(any(Duration.class))).thenReturn(records);

        rebuildConsumer();
        processingConsumer.rebalanceListener.onPartitionsRevoked(topicPartitions);
        processingConsumer.rebalanceListener.onPartitionsAssigned(topicPartitions);

        // We are asserting that we will read message 0 for each partition before reading 1 and then 2
        for(int message = 0; message < 3; message++) {

            // Handle any partition ordering
            final int messageInt = message;
            Collection<String> values = topicPartitions.stream().map(tp -> tp.toString() + " Message: " + messageInt)
                    .collect(Collectors.toList());

            for(int partition = 0; partition < topicPartitions.size(); partition++) {
                String value = nextRecordIsPresent().value();
                assertThat("Expected to remove [" + value + "] but it was not part of values [" + values + "]",
                        values.remove(value), is(true));
            }

            assertThat(values, empty());
        }

        // We should have read all records
        Optional<ConsumerRecord<String, String>> optional = processingConsumer.nextRecord(POLL_TIME);
        assertThat("expected optional consumer record to not be present", optional.isPresent(), is(false));
    }

    @Test
    public void nextRecord_noSubscriptions() {
        when(consumer.poll(any(Duration.class))).thenThrow(new IllegalStateException());
        when(consumer.subscription()).thenReturn(Collections.emptySet());
        assertThat(processingConsumer.nextRecord(POLL_TIME), is(Optional.empty()));
    }

    @Test(expected = IllegalStateException.class)
    public void nextRecord_unexpectedException() {
        when(consumer.poll(any(Duration.class))).thenThrow(new IllegalStateException());
        when(consumer.subscription()).thenReturn(Collections.singleton("some.topic"));
        processingConsumer.nextRecord(POLL_TIME);
    }

    @Test(expected = IllegalArgumentException.class)
    public void ack_nullTopicPartition() {
        processingConsumer.ack(null, 0);
    }

    @Test
    public void ack() {
        long previousAckCount = ProcessingKafkaConsumer.ACK_METER.count();

        Optional<ConsumerRecord<String, String>> optional = processingConsumer.nextRecord(POLL_TIME);
        assertThat("optional is not present", optional.isPresent(), is(true));

        assertThat(processingConsumer.ack(topicPartition, offset), is(true));
        assertThat(processingConsumer.getCommittableOffsets().get(topicPartition), is(new OffsetAndMetadata(offset + 1)));
        assertThat(ProcessingKafkaConsumer.ACK_METER.count(), is(previousAckCount + 1));
    }

    @Test
    public void ack_topicPartitionIsNotPending() {
        assertThat(processingConsumer.ack(topicPartition, offset), is(false));
        assertThat(processingConsumer.getCommittableOffsets().isEmpty(), is(true));
    }

    @Test
    public void ack_offsetIsNotPending() {
        // Read a message
        nextRecord();

        // This offset was never read but we did read a message for the partition
        assertThat(processingConsumer.ack(topicPartition, offset + 1), is(false));

        assertThat(processingConsumer.getCommittableOffsets().isEmpty(), is(true));
    }

    @Test
    public void ack_invalidTopicPartition() {
        // This offset was never read but we did read a message for the partition
        final ConsumerRecord<String, String> invalidPartition = new ConsumerRecord<>("notARealTopic", 0,0,"key", "value");

        assertThat(processingConsumer.ack(invalidPartition), is(false));
    }

    @Test
    public void ack_causesOffsetCommit() {
        // We are not testing the offset commit logic here, just that the ack can cause offsets to be committed so it
        // doesn't matter what property we pick here. This will simply make it always commit
        properties.setProperty(ProcessingConfig.COMMIT_TIME_THRESHOLD_PROPERTY, "0");
        rebuildConsumer();

        Optional<ConsumerRecord<String, String>> optional = processingConsumer.nextRecord(POLL_TIME);
        assertThat("optional is not present", optional.isPresent(), is(true));
        assertThat(processingConsumer.ack(topicPartition, offset), is(true));

        assertThat(processingConsumer.getCommittableOffsets().isEmpty(), is(true));

        verify(consumer).commitSync(Collections.singletonMap(topicPartition, new OffsetAndMetadata(offset + 1)));
    }

    @Test(expected = IllegalArgumentException.class)
    public void ack_topicPartitionIsNull() {
        processingConsumer.ack(null, 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void ack_topicPartitionIsNull2() {
        processingConsumer.ack(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void fail_topicPartitionIsNull() {
        processingConsumer.fail(null, 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void fail_topicPartitionIsNull2() {
        processingConsumer.fail(null);
    }

    @Test
    public void fail() {
        // seek is invoked by setup of this test class
        verify(processingConsumer.consumer).seek(topicPartition, offset);

        long previousFailCount = ProcessingKafkaConsumer.FAIL_METER.count();

        // Read a bunch of messages
        processingConsumer.nextRecord(POLL_TIME); // record 1
        processingConsumer.nextRecord(POLL_TIME); // null
        processingConsumer.nextRecord(POLL_TIME); // record 2
        processingConsumer.nextRecord(POLL_TIME); // record 3
        processingConsumer.nextRecord(POLL_TIME); // null
        processingConsumer.nextRecord(POLL_TIME); // record 4
        processingConsumer.nextRecord(POLL_TIME); // record 5
        processingConsumer.nextRecord(POLL_TIME); // record 6

        // Ack some of the messages. We should now have some acked and some pending
        processingConsumer.ack(topicPartition, record1.offset());
        processingConsumer.ack(topicPartition, record3.offset());
        processingConsumer.ack(new TopicPartition(record5.topic(), record5.partition()), record5.offset());

        // Fail record 2
        assertThat(processingConsumer.fail(topicPartition, record2.offset()), is(true));

        assertThat(ProcessingKafkaConsumer.FAIL_METER.count(), is(previousFailCount + 1));

        // Although we failed record 2, record 1 is still committable so that should still be eligible
        assertThat(processingConsumer.getCommittableOffsets().get(topicPartition), is(new OffsetAndMetadata(record1.offset() + 1)));
        assertThat(processingConsumer.getCommittableOffsets().get(new TopicPartition(record4.topic(), record4.partition())),
                is(nullValue()));
        assertThat(processingConsumer.getCommittableOffsets().get(new TopicPartition(record5.topic(), record5.partition())),
                is(new OffsetAndMetadata(record5.offset() + 1)));
        assertThat(processingConsumer.getCommittableOffsets().get(new TopicPartition(record6.topic(), record6.partition())),
                is(nullValue()));

        // Failing record 2 should rewind our consumer for that partition to the last committed offset
        verify(processingConsumer.consumer, times(2)).seek(topicPartition, offset);

        // We never committed anything
        verify(consumer, never()).commitSync(anyMap());
    }

    @Test
    public void fail_topicPartitionIsNotPending() {
        // seek is invoked by setup of this test class
        verify(consumer, times(4)).seek(any(TopicPartition.class), anyLong());

        assertThat(processingConsumer.fail(topicPartition, offset), is(false));

        // Verify we never reset our consumer
        verify(consumer, times(4)).seek(any(TopicPartition.class), anyLong());
    }

    @Test
    public void fail_offsetIsNotPending() {
        // seek is invoked by setup of this test class
        verify(consumer).seek(topicPartition, offset);
        processingConsumer.nextRecord(POLL_TIME);
        assertThat(processingConsumer.fail(topicPartition, offset + 1), is(false));

        // Verify we never reset our consumer
        verify(consumer).seek(topicPartition, offset);
    }

    @Test
    public void fail_invalidTopicPartion() {
        // This offset was never read but we did read a message for the partition
        final ConsumerRecord<String, String> invalidPartition = new ConsumerRecord<>("notARealTopic", 0, 0, "key", "value");

        assertThat(processingConsumer.fail(invalidPartition), is(false));
    }

    @Test(expected = IllegalArgumentException.class)
    public void resetOffsets_nullOffsets() {
        processingConsumer.resetOffsets(null);
    }

    @Test
    public void resetOffsets() {
        TopicPartition a0 = new TopicPartition("a", 0);
        TopicPartition a1 = new TopicPartition("a", 1);
        TopicPartition b0 = new TopicPartition("b", 0);

        Set<TopicPartition> assignment = new HashSet<>();
        assignment.addAll(Arrays.asList(a0, a1, b0));
        when(consumer.assignment()).thenReturn(assignment);

        Map<TopicPartition, Long> offsets = new HashMap<>();
        offsets.put(a0, 5L);
        offsets.put(a1, 6L);
        offsets.put(b0, 7L);

        processingConsumer.resetOffsets(offsets);

        verify(consumer).commitSync(getCommitOffsets(offsets));
        verify(consumer).seek(a0, 5L);
        verify(consumer).seek(a1, 6L);
        verify(consumer).seek(b0, 7L);
    }

    @Test
    public void resetOffsets_noAssignment() {
        // seek is invoked by setup of this test class
        verify(consumer, times(4)).seek(any(TopicPartition.class), anyLong());

        TopicPartition a0 = new TopicPartition("a", 0);
        TopicPartition a1 = new TopicPartition("a", 1);
        TopicPartition b0 = new TopicPartition("b", 0);

        when(consumer.assignment()).thenReturn(Collections.emptySet());

        Map<TopicPartition, Long> offsets = new HashMap<>();
        offsets.put(a0, 5L);
        offsets.put(a1, 6L);
        offsets.put(b0, 7L);

        processingConsumer.resetOffsets(offsets);

        verify(consumer, never()).commitSync(anyMap());
        verify(consumer, times(4)).seek(any(TopicPartition.class), anyLong());
    }

    @Test
    public void resetOffsets_noOffsets() {
        // seek is invoked by setup of this test class
        verify(consumer, times(4)).seek(any(TopicPartition.class), anyLong());

        TopicPartition a0 = new TopicPartition("a", 0);
        TopicPartition a1 = new TopicPartition("a", 1);
        TopicPartition b0 = new TopicPartition("b", 0);

        Set<TopicPartition> assignment = new HashSet<>();
        assignment.addAll(Arrays.asList(a0, a1, b0));
        when(consumer.assignment()).thenReturn(assignment);

        processingConsumer.resetOffsets(Collections.emptyMap());

        verify(consumer, never()).commitSync(anyMap());
        verify(consumer, times(4)).seek(any(TopicPartition.class), anyLong());
    }

    @Test
    public void resetOffsets_moreThanAssigned() {
        TopicPartition a0 = new TopicPartition("a", 0);
        TopicPartition a1 = new TopicPartition("a", 1);
        TopicPartition b0 = new TopicPartition("b", 0);

        Set<TopicPartition> assignment = new HashSet<>();
        assignment.add(a1);
        when(consumer.assignment()).thenReturn(assignment);

        Map<TopicPartition, Long> offsets = new HashMap<>();
        offsets.put(a0, 5L);
        offsets.put(a1, 6L);
        offsets.put(b0, 7L);

        processingConsumer.resetOffsets(offsets);

        verify(consumer).commitSync(getCommitOffsets(Collections.singletonMap(a1, 6L)));
        verify(consumer, never()).seek(eq(a0), anyLong());
        verify(consumer).seek(a1, 6L);
        verify(consumer, never()).seek(eq(b0), anyLong());
    }

    @Test
    public void resetOffsets_lessThanAssigned() {
        TopicPartition a0 = new TopicPartition("a", 0);
        TopicPartition a1 = new TopicPartition("a", 1);
        TopicPartition b0 = new TopicPartition("b", 0);

        Set<TopicPartition> assignment = new HashSet<>();
        assignment.add(b0);
        when(consumer.assignment()).thenReturn(assignment);

        Map<TopicPartition, Long> offsets = new HashMap<>();
        offsets.put(b0, 7L);

        processingConsumer.resetOffsets(offsets);

        verify(consumer).commitSync(getCommitOffsets(offsets));
        verify(consumer, never()).seek(eq(a0), anyLong());
        verify(consumer, never()).seek(eq(a1), anyLong());
        verify(consumer).seek(b0, 7L);
    }

    @Test
    public void resetOffsets_notAssigned() {
        // seek is invoked by setup of this test class
        verify(consumer, times(4)).seek(any(TopicPartition.class), anyLong());

        TopicPartition a0 = new TopicPartition("a", 0);
        TopicPartition a1 = new TopicPartition("a", 1);
        TopicPartition b0 = new TopicPartition("b", 0);

        Set<TopicPartition> assignment = new HashSet<>();
        assignment.addAll(Arrays.asList(a0, a1));
        when(consumer.assignment()).thenReturn(assignment);

        Map<TopicPartition, Long> offsets = new HashMap<>();
        offsets.put(b0, 7L);

        processingConsumer.resetOffsets(offsets);

        verify(consumer, never()).commitSync(anyMap());
        verify(consumer, times(4)).seek(any(TopicPartition.class), anyLong());
    }

    @Test
    public void resetOffsets_moreAndLessThanAssigned() {
        TopicPartition a0 = new TopicPartition("a", 0);
        TopicPartition a1 = new TopicPartition("a", 1);
        TopicPartition b0 = new TopicPartition("b", 0);

        Set<TopicPartition> assignment = new HashSet<>();
        assignment.addAll(Arrays.asList(a0, a1));
        when(consumer.assignment()).thenReturn(assignment);

        Map<TopicPartition, Long> offsets = new HashMap<>();
        offsets.put(a0, 5L);
        offsets.put(b0, 7L);

        processingConsumer.resetOffsets(offsets);

        verify(consumer).commitSync(getCommitOffsets(Collections.singletonMap(a0, 5L)));
        verify(consumer).seek(a0, 5L);
        verify(consumer, never()).seek(eq(a1), anyLong());
        verify(consumer, never()).seek(eq(b0), anyLong());
    }

    @Test
    public void resetOffsets_closesAndClearsAssignedProcessingPartitions() throws IOException {
        TopicPartition a0 = new TopicPartition("a", 0);
        TopicPartition a1 = new TopicPartition("a", 1);
        TopicPartition b0 = new TopicPartition("b", 0);

        Set<TopicPartition> assignment = new HashSet<>();
        assignment.addAll(Arrays.asList(a0, a1));
        when(consumer.assignment()).thenReturn(assignment);

        Map<TopicPartition, Long> offsets = new HashMap<>();
        offsets.put(a0, 5L);
        offsets.put(a1, 6L);
        offsets.put(b0, 7L);

        ProcessingPartition<String, String> processingPartition_a0 = getMockProcessingPartition(a0);
        ProcessingPartition<String, String> processingPartition_a1 = getMockProcessingPartition(a1);
        ProcessingPartition<String, String> processingPartition_b0 = getMockProcessingPartition(b0);

        processingConsumer.partitions.clear();
        processingConsumer.partitions.put(a0, processingPartition_a0);
        processingConsumer.partitions.put(a1, processingPartition_a1);
        processingConsumer.partitions.put(b0, processingPartition_b0);

        processingConsumer.resetOffsets(offsets);

        verify(processingPartition_a0).close();
        verify(processingPartition_a1).close();
        verify(processingPartition_b0, never()).close();

        assertThat(processingConsumer.partitions, is(Collections.singletonMap(b0, processingPartition_b0)));
    }

    @Test
    public void commitOffsets() {
        long previousCommitCount = ProcessingKafkaConsumer.COMMIT_METER.count();

        // Read a bunch of messages
        processingConsumer.nextRecord(POLL_TIME); // record 1
        processingConsumer.nextRecord(POLL_TIME); // null
        processingConsumer.nextRecord(POLL_TIME); // record 2
        processingConsumer.nextRecord(POLL_TIME); // record 3
        processingConsumer.nextRecord(POLL_TIME); // null
        processingConsumer.nextRecord(POLL_TIME); // record 4
        processingConsumer.nextRecord(POLL_TIME); // record 5
        processingConsumer.nextRecord(POLL_TIME); // record 6

        // Ack some of the messages. We should now have some acked and some pending
        processingConsumer.ack(topicPartition, record1.offset());
        processingConsumer.ack(topicPartition, record3.offset());
        processingConsumer.ack(new TopicPartition(record5.topic(), record5.partition()), record5.offset());

        processingConsumer.commitOffsets();

        assertThat(ProcessingKafkaConsumer.COMMIT_METER.count(), is(previousCommitCount + 1));

        Map<TopicPartition, OffsetAndMetadata> committedOffsets = new HashMap<>();

        // Although record 3 is completed record 2 is pending so for this partition record 1 is as high as we can commit
        committedOffsets.put(topicPartition, new OffsetAndMetadata(record1.offset() + 1));

        committedOffsets.put(new TopicPartition(record5.topic(), record5.partition()),
                new OffsetAndMetadata(record5.offset() + 1));

        verify(consumer).commitSync(committedOffsets);
        assertThat(processingConsumer.getCommittableOffsets().isEmpty(), is(true));
    }

    @Test
    public void commitOffsets_nothingCompleted() {
        // Read a bunch of messages
        processingConsumer.nextRecord(POLL_TIME); // record 1
        processingConsumer.nextRecord(POLL_TIME); // null
        processingConsumer.nextRecord(POLL_TIME); // record 2
        processingConsumer.nextRecord(POLL_TIME); // record 3
        processingConsumer.nextRecord(POLL_TIME); // null
        processingConsumer.nextRecord(POLL_TIME); // record 4
        processingConsumer.nextRecord(POLL_TIME); // record 5
        processingConsumer.nextRecord(POLL_TIME); // record 6

        processingConsumer.commitOffsets();

        // Nothing was committed
        verify(consumer, never()).commitSync(anyMap());
        assertThat(processingConsumer.getCommittableOffsets().isEmpty(), is(true));
    }

    @Test
    public void commitOffsets_earliestMessageStillPending() {
        // Read first 3 messages
        processingConsumer.nextRecord(POLL_TIME); // record 1
        processingConsumer.nextRecord(POLL_TIME); // null
        processingConsumer.nextRecord(POLL_TIME); // record 2
        processingConsumer.nextRecord(POLL_TIME); // record 3

        // Ack record 2 and 3 which are after record 1's offset but on same partition
        processingConsumer.ack(topicPartition, record2.offset());
        processingConsumer.ack(topicPartition, record3.offset());

        processingConsumer.commitOffsets();

        // Nothing should be committed since record 1 still hasn't been processed
        verify(consumer, never()).commitSync(anyMap());
        assertThat(processingConsumer.getCommittableOffsets().isEmpty(), is(true));
    }

    @Test
    public void commitOffsets_noLongerAssignedToPartition() {
        // Read a bunch of messages
        processingConsumer.nextRecord(POLL_TIME); // record 1
        processingConsumer.nextRecord(POLL_TIME); // null
        processingConsumer.nextRecord(POLL_TIME); // record 2
        processingConsumer.nextRecord(POLL_TIME); // record 3
        processingConsumer.nextRecord(POLL_TIME); // null
        processingConsumer.nextRecord(POLL_TIME); // record 4
        processingConsumer.nextRecord(POLL_TIME); // record 5
        processingConsumer.nextRecord(POLL_TIME); // record 6

        // Ack record 1 and 4
        processingConsumer.ack(topicPartition, record1.offset());
        processingConsumer.ack(new TopicPartition(record4.topic(), record4.partition()), record4.offset());

        //Should have two eligible offsets before rebalance
        assertThat(processingConsumer.getCommittableOffsets().size(), is(2));

        //Will always be called before onPartitionsAssigned
        processingConsumer.rebalanceListener.onPartitionsRevoked(Arrays.asList(topicPartition,
                new TopicPartition(record4.topic(), record4.partition()),
                new TopicPartition(record5.topic(), record5.partition()),
                new TopicPartition(record6.topic(), record6.partition())
        ));

        // Both record1 and record4 should have been committed
        verify(consumer).commitSync(commitCaptor.capture());
        Map<TopicPartition, OffsetAndMetadata> committedOffsets = commitCaptor.getValue();
        assertThat(committedOffsets.size(), is(2));
        assertThat(committedOffsets.get(new TopicPartition(record1.topic(), record1.partition())), is(new OffsetAndMetadata(record1.offset() + 1)));
        assertThat(committedOffsets.get(new TopicPartition(record4.topic(), record4.partition())), is(new OffsetAndMetadata(record4.offset() + 1)));

        // Change our assignments so we no longer are interested in record 1's partition
        processingConsumer.rebalanceListener.onPartitionsAssigned(Arrays.asList(
                new TopicPartition(record4.topic(), record4.partition()),
                new TopicPartition(record5.topic(), record5.partition()),
                new TopicPartition(record6.topic(), record6.partition())
        ));

        assertThat(processingConsumer.getCommittableOffsets().isEmpty(), is(true));

    }

    @Test
    public void commitOffsets_noChangeToAssignment() {
        // Read a bunch of messages
        processingConsumer.nextRecord(POLL_TIME); // record 1
        processingConsumer.nextRecord(POLL_TIME); // null
        processingConsumer.nextRecord(POLL_TIME); // record 2
        processingConsumer.nextRecord(POLL_TIME); // record 3
        processingConsumer.nextRecord(POLL_TIME); // null
        processingConsumer.nextRecord(POLL_TIME); // record 4
        processingConsumer.nextRecord(POLL_TIME); // record 5
        processingConsumer.nextRecord(POLL_TIME); // record 6

        // Ack record 1 and 4
        processingConsumer.ack(topicPartition, record1.offset());
        processingConsumer.ack(new TopicPartition(record4.topic(), record4.partition()), record4.offset());

        //Should have two eligible offsets before rebalance
        assertThat(processingConsumer.getCommittableOffsets().size(), is(2));

        //Will always be called before onPartitionsAssigned
        processingConsumer.rebalanceListener.onPartitionsRevoked(Arrays.asList(topicPartition,
                new TopicPartition(record4.topic(), record4.partition()),
                new TopicPartition(record5.topic(), record5.partition()),
                new TopicPartition(record6.topic(), record6.partition())
        ));

        // Both record1 and record4 should have been committed
        verify(consumer).commitSync(commitCaptor.capture());
        Map<TopicPartition, OffsetAndMetadata> committedOffsets = commitCaptor.getValue();
        assertThat(committedOffsets.size(), is(2));
        assertThat(committedOffsets.get(new TopicPartition(record1.topic(), record1.partition())), is(new OffsetAndMetadata(record1.offset() + 1)));
        assertThat(committedOffsets.get(new TopicPartition(record4.topic(), record4.partition())), is(new OffsetAndMetadata(record4.offset() + 1)));

        // Rebalance invoked, but no change in our assignments
        processingConsumer.rebalanceListener.onPartitionsAssigned(Arrays.asList(topicPartition,
                new TopicPartition(record4.topic(), record4.partition()),
                new TopicPartition(record5.topic(), record5.partition()),
                new TopicPartition(record6.topic(), record6.partition())
        ));

        assertThat(processingConsumer.getCommittableOffsets().isEmpty(), is(true));
    }

    @Test
    public void commitOffsets_missedCommitsBetweenRebalanceGenerations() {

        // Read record 1
        processingConsumer.nextRecord(POLL_TIME);

        // Ack record
        processingConsumer.ack(topicPartition, record1.offset());
        //Should have one eligible offset before rebalance
        assertThat(processingConsumer.getCommittableOffsets().size(), is(1));

        //Will always be called before onPartitionsAssigned
        processingConsumer.rebalanceListener.onPartitionsRevoked(Arrays.asList(topicPartition,
                new TopicPartition(record4.topic(), record4.partition()),
                new TopicPartition(record5.topic(), record5.partition()),
                new TopicPartition(record6.topic(), record6.partition())
        ));
        // record1 should have been committed
        verify(consumer).commitSync(commitCaptor.capture());
        Map<TopicPartition, OffsetAndMetadata> committedOffsets = commitCaptor.getValue();
        assertThat(committedOffsets.size(), is(1));
        assertThat(committedOffsets.get(new TopicPartition(record1.topic(), record1.partition())), is(new OffsetAndMetadata(record1.offset() + 1)));

        //Simulate missed generation before we rejoin consumer group
        when(consumer.poll(any(Duration.class)))
                .thenReturn(new ConsumerRecords<>(Collections.singletonMap(topicPartition, Arrays.asList(record3))));
        when(consumer.committed(topicPartition)).thenReturn(new OffsetAndMetadata(record2.offset() + 1));

        // Rejoin consumer group during subsequent rebalance
        processingConsumer.rebalanceListener.onPartitionsAssigned(Arrays.asList(topicPartition,
                new TopicPartition(record4.topic(), record4.partition()),
                new TopicPartition(record5.topic(), record5.partition()),
                new TopicPartition(record6.topic(), record6.partition())
        ));
        assertThat(processingConsumer.getCommittableOffsets().isEmpty(), is(true));

        processingConsumer.nextRecord(POLL_TIME); // record 3

        // Ack record 3
        processingConsumer.ack(new TopicPartition(record3.topic(), record3.partition()), record3.offset());

        // Record 3 offset should be committable, even though we never saw record 2
        assertThat(processingConsumer.getCommittableOffsets().size(), is(1));

        processingConsumer.commitOffsets();

        // record3 should have been committed
        verify(consumer, atLeastOnce()).commitSync(commitCaptor.capture());
        committedOffsets = commitCaptor.getValue();
        assertThat(committedOffsets.size(), is(1));
        assertThat(committedOffsets.get(new TopicPartition(record3.topic(), record3.partition())), is(new OffsetAndMetadata(record3.offset() + 1)));

        assertThat(processingConsumer.getCommittableOffsets().isEmpty(), is(true));

    }

    @Test
    public void nextRecord_maybeCommitOffsetsForTime() throws InterruptedException {
        properties.setProperty(ProcessingConfig.COMMIT_TIME_THRESHOLD_PROPERTY, "50"); // 50ms
        rebuildConsumer();

        processingConsumer.nextRecord(POLL_TIME); // record 1
        processingConsumer.ack(topicPartition, record1.offset());

        // Nothing should be committed (not enough time has passed)
        verify(consumer, never()).commitSync(anyMap());

        // Record 1 should be eligible
        assertThat(processingConsumer.getCommittableOffsets().get(topicPartition),
                is(new OffsetAndMetadata(record1.offset() + 1)));

        processingConsumer.nextRecord(POLL_TIME); // null

        // Nothing should be committed (not enough time has passed)
        verify(consumer, never()).commitSync(anyMap());

        // Record 1 should be eligible
        assertThat(processingConsumer.getCommittableOffsets().get(topicPartition),
                is(new OffsetAndMetadata(record1.offset() + 1)));

        Thread.sleep(50L);

        processingConsumer.nextRecord(POLL_TIME); // record 2

        // record 1 should be committed
        verify(consumer).commitSync(Collections.singletonMap(topicPartition,
                new OffsetAndMetadata(record1.offset() + 1)));

        assertThat(processingConsumer.getCommittableOffsets().isEmpty(), is(true));
    }

    @Test
    public void ack_maybeCommitOffsets_timeAndSizeThresholdNotMet() {
        processingConsumer.nextRecord(POLL_TIME); // record 1
        processingConsumer.ack(topicPartition, record1.offset());

        // Nothing should be committed
        verify(consumer, never()).commitSync(anyMap());

        // Record 1 should be eligible
        assertThat(processingConsumer.getCommittableOffsets().get(topicPartition),
                is(new OffsetAndMetadata(record1.offset() + 1)));
    }

    @Test
    public void ack_maybeCommitOffsets_timeThresholdMet() throws InterruptedException {
        properties.setProperty(ProcessingConfig.COMMIT_TIME_THRESHOLD_PROPERTY, "50"); // 50ms
        rebuildConsumer();

        // Read first 3 messages
        processingConsumer.nextRecord(POLL_TIME); // record 1
        processingConsumer.nextRecord(POLL_TIME); // null
        processingConsumer.nextRecord(POLL_TIME); // record 2
        processingConsumer.nextRecord(POLL_TIME); // record 3

        // Wait until time threshold has past
        Thread.sleep(50L);

        // Ack'ing the record should cause a commit
        processingConsumer.ack(topicPartition, record1.offset());

        // record 1 should be committed
        verify(consumer).commitSync(Collections.singletonMap(topicPartition,
                new OffsetAndMetadata(record1.offset() + 1)));

        assertThat(processingConsumer.getCommittableOffsets().isEmpty(), is(true));

        // Ack'ing these records should not cause a commit as we just committed and need to wait another 50ms
        processingConsumer.ack(topicPartition, record2.offset());
        processingConsumer.ack(topicPartition, record3.offset());

        // These records should not be committed not enough time has passed
        verify(consumer, never()).commitSync(Collections.singletonMap(topicPartition,
                new OffsetAndMetadata(record3.offset() + 1)));

        // Up to record 3 should be eligible
        assertThat(processingConsumer.getCommittableOffsets().get(topicPartition),
                is(new OffsetAndMetadata(record3.offset() + 1)));
    }

    @Test
    public void ack_maybeCommitOffsets_sizeThresholdMet() {
        properties.setProperty(ProcessingConfig.COMMIT_SIZE_THRESHOLD_PROPERTY, "2");
        rebuildConsumer();

        // Read 2 messages
        processingConsumer.nextRecord(POLL_TIME); // record 1
        processingConsumer.nextRecord(POLL_TIME); // null
        processingConsumer.nextRecord(POLL_TIME); // record 2

        processingConsumer.ack(topicPartition, record1.offset());

        // Ack'ing 1 message should not cause a commit
        verify(consumer, never()).commitSync(anyMap());

        // This 2nd ack should cause the commit
        processingConsumer.ack(topicPartition, record2.offset());

        // Verify the records were committed
        verify(consumer).commitSync(Collections.singletonMap(topicPartition,
                new OffsetAndMetadata(record2.offset() + 1)));

        assertThat(processingConsumer.getCommittableOffsets().isEmpty(), is(true));
    }

    @Test
    public void ack_maybeCommitOffsets_sizeThresholdIsPerPartition() {
        properties.setProperty(ProcessingConfig.COMMIT_SIZE_THRESHOLD_PROPERTY, "2");
        rebuildConsumer();

        // Read a bunch of messages
        assertThat(nextRecordIsPresent(), is(record1));
        assertThat(nextRecordIsPresent(), is(record2));
        assertThat(nextRecordIsPresent(), is(record3));
        assertThat(nextRecordIsPresent(), is(record4));
        assertThat(nextRecordIsPresent(), is(record6)); // Odd ordering issue we can't control
        assertThat(nextRecordIsPresent(), is(record5));

        assertThat(processingConsumer.ack(topicPartition, record1.offset()), is(true));

        assertThat(processingConsumer.getCommittableOffsets().get(topicPartition),
                is(new OffsetAndMetadata(record1.offset() + 1)));

        // Ack'ing 1 message should not cause a commit
        verify(consumer, never()).commitSync(anyMap());

        // This second ack should also not commit anything because its for a different partition and our threshold targets
        // the partition level
        assertThat(processingConsumer.ack(new TopicPartition(record4.topic(), record4.partition()), record4.offset()), is(true));

        assertThat(processingConsumer.getCommittableOffsets().get(new TopicPartition(record4.topic(), record4.partition())),
                is(new OffsetAndMetadata(record4.offset() + 1)));

        verify(consumer, never()).commitSync(anyMap());

        // Ack'ing this message should cause a commit since the first partition now has 2 offset completed
        assertThat(processingConsumer.ack(topicPartition, record2.offset()), is(true));

        // Verify the records were committed
        Map<TopicPartition, OffsetAndMetadata> committedOffsets = new HashMap<>();
        committedOffsets.put(topicPartition, new OffsetAndMetadata(record2.offset() + 1));
        committedOffsets.put(new TopicPartition(record4.topic(), record4.partition()),
                new OffsetAndMetadata(record4.offset() + 1));

        verify(consumer).commitSync(committedOffsets);

        assertThat(processingConsumer.getCommittableOffsets().isEmpty(), is(true));
    }

    @Test
    public void ack_maybeCommitOffsets_sizeAndTimeThresholdMet() throws InterruptedException {
        properties.setProperty(ProcessingConfig.COMMIT_TIME_THRESHOLD_PROPERTY, "50"); // 50ms
        properties.setProperty(ProcessingConfig.COMMIT_SIZE_THRESHOLD_PROPERTY, "2");
        rebuildConsumer();

        // Read 2 messages
        processingConsumer.nextRecord(POLL_TIME); // record 1
        processingConsumer.nextRecord(POLL_TIME); // null
        processingConsumer.nextRecord(POLL_TIME); // record 2

        processingConsumer.ack(topicPartition, record1.offset());

        // Ack'ing 1 message (this quickly) should not cause a commit
        verify(consumer, never()).commitSync(anyMap());

        // Wait until time threshold has past
        Thread.sleep(50L);

        // This 2nd ack should cause the commit
        processingConsumer.ack(topicPartition, record2.offset());

        // Verify the records were committed
        verify(consumer).commitSync(Collections.singletonMap(topicPartition,
                new OffsetAndMetadata(record2.offset() + 1)));

        assertThat(processingConsumer.getCommittableOffsets().isEmpty(), is(true));
    }

    @Test
    public void subscribe_topicList() {
        List<String> topics = Arrays.asList("topic1", "topic2");
        processingConsumer.subscribe(topics);
        verify(consumer).subscribe(topics, processingConsumer.rebalanceListener);
    }

    @Test
    public void subscribe_regexPattern() {
        Pattern pattern = Pattern.compile("^some-prefix\\..*");
        processingConsumer.subscribe(pattern);
        verify(consumer).subscribe(pattern, processingConsumer.rebalanceListener);
    }

    @Test
    public void fail_pausePartition() throws InterruptedException {
        properties.put(ProcessingConfig.FAIL_PAUSE_TIME_PROPERTY, Long.toString(50L));
        properties.put(ProcessingConfig.FAIL_SAMPLE_SIZE_PROPERTY, Integer.toString(3));
        rebuildConsumer();

        // Read the first record in 3 separate batches or poll()'s
        when(consumer.poll(any(Duration.class))).thenReturn(
                new ConsumerRecords<>(Collections.singletonMap(topicPartition, Arrays.asList(record1)))).thenReturn(
                new ConsumerRecords<>(Collections.singletonMap(topicPartition, Arrays.asList(record1)))).thenReturn(
                new ConsumerRecords<>(Collections.singletonMap(topicPartition, Arrays.asList(record1))));

        // Read a message
        processingConsumer.nextRecord(POLL_TIME); // record 1 - batch 1

        // Fail the message
        processingConsumer.fail(topicPartition, record1.offset());

        // This should not have paused the partition as not enough failures have occurred
        verify(consumer, never()).pause(topicPartitions);

        // Read a message
        processingConsumer.nextRecord(POLL_TIME); // null (empty iterator)
        processingConsumer.nextRecord(POLL_TIME); // record 1 - batch 2

        // Fail the message
        processingConsumer.fail(topicPartition, record1.offset());

        // This should have paused the partition as we met the threshold
        verify(consumer).pause(topicPartitions);

        // This should not resume the partition as not enough time has passed
        processingConsumer.nextRecord(POLL_TIME); // null (iterator is empty)

        verify(consumer, never()).resume(topicPartitions);

        // Wait the pause time
        Thread.sleep(50L);

        // This should un-pause the partition as enough time has passed
        processingConsumer.nextRecord(POLL_TIME); // record 1 - batch 3

        verify(consumer).resume(topicPartitions);
    }

    @Test
    public void fail_multiplePauses() throws InterruptedException {
        properties.put(ProcessingConfig.FAIL_PAUSE_TIME_PROPERTY, Long.toString(0L));
        properties.put(ProcessingConfig.FAIL_SAMPLE_SIZE_PROPERTY, Integer.toString(3));
        rebuildConsumer();

        // Read the first record in 4 separate batches or poll()'s
        when(consumer.poll(any(Duration.class))).thenReturn(
                new ConsumerRecords<>(Collections.singletonMap(topicPartition, Arrays.asList(record1)))).thenReturn(
                new ConsumerRecords<>(Collections.singletonMap(topicPartition, Arrays.asList(record1)))).thenReturn(
                new ConsumerRecords<>(Collections.singletonMap(topicPartition, Arrays.asList(record1)))).thenReturn(
                new ConsumerRecords<>(Collections.singletonMap(topicPartition, Arrays.asList(record1))));

        // Read a message
        processingConsumer.nextRecord(POLL_TIME); // record 1 - batch 1

        // Fail the message
        processingConsumer.fail(topicPartition, record1.offset());

        // This should not have paused the partition as not enough failures have occurred
        verify(consumer, never()).pause(topicPartitions);

        // Read a message
        processingConsumer.nextRecord(POLL_TIME); // null (empty iterator)
        processingConsumer.nextRecord(POLL_TIME); // record 1 - batch 2

        // Fail the message
        processingConsumer.fail(topicPartition, record1.offset());

        // This should have paused the partition as we met the threshold
        verify(consumer).pause(topicPartitions);

        processingConsumer.nextRecord(POLL_TIME); // null (iterator is empty)

        // We have no pause time so this should resume the partition
        verify(consumer).resume(topicPartitions);

        processingConsumer.nextRecord(POLL_TIME); // record 1 - batch 3

        // This should not pause the partition as our failure count was reset after being paused
        processingConsumer.fail(topicPartition, record1.offset());

        // Only 1 pause has occurred so far
        verify(consumer).pause(topicPartitions);

        processingConsumer.nextRecord(POLL_TIME); // null (empty iterator)
        processingConsumer.nextRecord(POLL_TIME); // record 1 - batch 4

        // This should pause the partition as the threshold has been met now
        processingConsumer.fail(topicPartition, record1.offset());

        // We have now paused twice
        verify(consumer, times(2)).pause(topicPartitions);

        processingConsumer.nextRecord(POLL_TIME); // null (empty iterator)

        verify(consumer, times(2)).resume(topicPartitions);
    }

    @Test
    public void rebalanceListener_onPartitionsAssigned() {
        long rebalanceCount = ProcessingKafkaConsumer.REBALANCE_COUNTER.count();
        TopicPartition newPartition = new TopicPartition("new-topic", 0);
        when(consumer.committed(newPartition)).thenReturn(new OffsetAndMetadata(0L));
        processingConsumer.rebalanceListener.onPartitionsAssigned(Arrays.asList(topicPartition, newPartition));
        assertThat(processingConsumer.partitions.keySet(), contains(topicPartition, newPartition));
        assertThat(ProcessingKafkaConsumer.REBALANCE_COUNTER.count(), is(rebalanceCount + 1));
    }

    @Test
    public void commitOffsets_pauseCommits() {
        assertThat(nextRecordIsPresent(), is(record1));
        assertThat(processingConsumer.ack(topicPartition, record1.offset()), is(true));

        assertThat(processingConsumer.getCommittableOffsets().entrySet().size(), is(1));

        processingConsumer.pauseCommit = true;

        processingConsumer.commitOffsets();

        // We should not have committed
        verify(consumer, never()).commitSync(anyMap());
    }

    @Test
    public void maybeCommitOffsetsForTime_pausedCommits() throws InterruptedException {
        properties.setProperty(ProcessingConfig.COMMIT_TIME_THRESHOLD_PROPERTY, "50"); // 50ms
        rebuildConsumer();

        assertThat(nextRecordIsPresent(), is(record1));
        assertThat(processingConsumer.ack(topicPartition, record1.offset()), is(true));
        assertThat(processingConsumer.getCommittableOffsets().entrySet().size(), is(1));

        processingConsumer.pauseCommit = true;

        // Wait till time threshold passes
        Thread.sleep(50L);

        assertThat(processingConsumer.maybeCommitOffsetsForTime(), is(false));

        // We should not have committed
        verify(consumer, never()).commitSync(anyMap());
    }

    @Test
    public void maybeCommitOffsetsForSize_pausedCommits() {
        properties.setProperty(ProcessingConfig.COMMIT_SIZE_THRESHOLD_PROPERTY, "3"); // 3 records
        rebuildConsumer();

        assertThat(nextRecordIsPresent(), is(record1));
        assertThat(processingConsumer.ack(topicPartition, record1.offset()), is(true));

        assertThat(nextRecordIsPresent(), is(record2));
        assertThat(processingConsumer.ack(topicPartition, record2.offset()), is(true));

        // We shouldn't see another poll() till after we hit size threshold
        processingConsumer.pauseCommit = true;

        assertThat(nextRecordIsPresent(), is(record3));
        assertThat(processingConsumer.ack(topicPartition, record3.offset()), is(true));

        // Although there are 3 eligible offsets to commit they are all for the same partition so expect 1
        assertThat(processingConsumer.getCommittableOffsets().entrySet().size(), is(1));

        // Since our size threshold is set to 3 we should have committed during our ack but we are paused
        verify(consumer, never()).commitSync(anyMap());
    }

    @Test
    public void nextRecord_pausedCommits() {
        processingConsumer.pauseCommit = true;

        processingConsumer.nextRecord(POLL_TIME);

        verify(consumer).poll(Duration.ofMillis(POLL_TIME));

        // Since we have polled we can commit again
        assertThat(processingConsumer.pauseCommit, is(false));
    }

    private ConsumerRecord<String, String> nextRecordIsPresent() {
        Optional<ConsumerRecord<String, String>> optional = processingConsumer.nextRecord(POLL_TIME);
        assertThat("optional is not present", optional.isPresent(), is(true));
        return optional.get();
    }

    private void rebuildConsumer() {
        config = new ProcessingConfig(properties);
        processingConsumer = new MockProcessingKafkaConsumer<>(config, consumer);

        List<TopicPartition> assignedPartitions = Arrays.asList(topicPartition,
                new TopicPartition(record4.topic(), record4.partition()),
                new TopicPartition(record5.topic(), record5.partition()),
                new TopicPartition(record6.topic(), record6.partition()));

        // Assign topics to consumer
        processingConsumer.rebalanceListener.onPartitionsRevoked(assignedPartitions);
        processingConsumer.rebalanceListener.onPartitionsAssigned(assignedPartitions);
    }

    // Converts offset map values to their committable OffsetAndMetadata form
    private static Map<TopicPartition, OffsetAndMetadata> getCommitOffsets(Map<TopicPartition, Long> offsets) {
        return offsets.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> new OffsetAndMetadata(e.getValue())));
    }

    // Creates a basic mock processing partition
    @SuppressWarnings("unchecked")
    private static <K, V> ProcessingPartition<K, V> getMockProcessingPartition(TopicPartition partition) {
        ProcessingPartition<K, V> mockProcessingPartition = mock(ProcessingPartition.class);
        when(mockProcessingPartition.getTopicPartition()).thenReturn(partition);
        return mockProcessingPartition;
    }

    // Used to provide mock processing partition
    private class MockProcessingKafkaConsumer<K, V> extends ProcessingKafkaConsumer<K, V> {

        protected MockProcessingKafkaConsumer(ProcessingConfig config, Consumer<K, V> consumer) {
            super(config, consumer);
        }

        @Override
        protected ProcessingPartition<K, V> buildPartition(TopicPartition topicPartition, ProcessingConfig processingConfig,
                                                           Consumer<K, V> consumer) {
            return new MockProcessingPartition<>(topicPartition, processingConfig, consumer);
        }
    }
}
