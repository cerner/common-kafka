package com.cerner.common.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsCollectionContaining.hasItem;
import static org.hamcrest.core.IsNot.not;
import static org.hamcrest.core.IsNull.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class ConsumerOffsetClientTest {

    @Mock
    private Consumer<Object, Object> consumer;

    @Captor
    private ArgumentCaptor<Map<TopicPartition, Long>> offsetsRequests;

    @Captor
    private ArgumentCaptor<Collection<TopicPartition>> endOffsetRequest;

    @Captor
    private ArgumentCaptor<Map<TopicPartition, OffsetAndMetadata>> commitRequest;

    private ConsumerOffsetClient client;

    @BeforeEach
    public void before() {
        client = new ConsumerOffsetClient(consumer);
    }

    @Test
    public void constructor_properties() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        ConsumerOffsetClient client = new ConsumerOffsetClient(properties);

        assertThat(client.providedByUser, is(false));
        assertThat(client.consumer, is(not(nullValue())));
    }

    @Test
    public void constructor_propertiesNull() {
        assertThrows(IllegalArgumentException.class,
                () -> new ConsumerOffsetClient((Properties) null),
                "Expected ConsumerOffsetClient constructor to throw IllegalArgumentException, but wasn't thrown");
    }

    @Test
    public void constructor_consumer() {
        ConsumerOffsetClient client = new ConsumerOffsetClient(consumer);
        assertThat(client.providedByUser, is(true));
        assertThat(client.consumer, is(consumer));
    }

    @Test
    public void constructor_consumerNull() {
        assertThrows(IllegalArgumentException.class,
                () -> new ConsumerOffsetClient((Consumer<Object, Object>) null),
                "Expected ConsumerOffsetClient constructor to throw IllegalArgumentException, but wasn't thrown");
    }

    @Test
    public void getEndOffsets() {
        Map<TopicPartition, Long> offsets = new HashMap<>();
        offsets.put(new TopicPartition("topic1", 0), 123L);
        offsets.put(new TopicPartition("topic1", 1), 234L);
        offsets.put(new TopicPartition("topic2", 0), 345L);
        offsets.put(new TopicPartition("topic2", 1), 456L);

        when(consumer.partitionsFor("topic1")).thenReturn(Arrays.asList(
                new PartitionInfo("topic1", 0, null, null, null),
                new PartitionInfo("topic1", 1, null, null, null)));
        when(consumer.partitionsFor("topic2")).thenReturn(Arrays.asList(
                new PartitionInfo("topic2", 0, null, null, null),
                new PartitionInfo("topic2", 1, null, null, null)));

        when(consumer.endOffsets(Arrays.asList(
                new TopicPartition("topic1", 0),
                new TopicPartition("topic1", 1),
                new TopicPartition("topic2", 0),
                new TopicPartition("topic2", 1)
        ))).thenReturn(offsets);

        assertThat(client.getEndOffsets(Arrays.asList("topic1", "topic2")), is(offsets));
    }

    @Test
    public void getEndOffsets_nullTopics() {
        assertThrows(IllegalArgumentException.class,
                () -> client.getEndOffsets(null),
                "Expected client.getEndOffsets to throw IllegalArgumentException, but wasn't thrown");
    }

    @Test
    public void getBeginningOffsets() {
        Map<TopicPartition, Long> offsets = new HashMap<>();
        offsets.put(new TopicPartition("topic1", 0), 123L);
        offsets.put(new TopicPartition("topic1", 1), 234L);
        offsets.put(new TopicPartition("topic2", 0), 345L);
        offsets.put(new TopicPartition("topic2", 1), 456L);

        when(consumer.partitionsFor("topic1")).thenReturn(Arrays.asList(
                new PartitionInfo("topic1", 0, null, null, null),
                new PartitionInfo("topic1", 1, null, null, null)));
        when(consumer.partitionsFor("topic2")).thenReturn(Arrays.asList(
                new PartitionInfo("topic2", 0, null, null, null),
                new PartitionInfo("topic2", 1, null, null, null)));

        when(consumer.endOffsets(Arrays.asList(
                new TopicPartition("topic1", 0),
                new TopicPartition("topic1", 1),
                new TopicPartition("topic2", 0),
                new TopicPartition("topic2", 1)
        ))).thenReturn(offsets);

        assertThat(client.getEndOffsets(Arrays.asList("topic1", "topic2")), is(offsets));
    }

    @Test
    public void getBeginningOffsets_nullTopics() {
        assertThrows(IllegalArgumentException.class,
                () -> client.getBeginningOffsets(null),
                "Expected client.getBeginningOffsets to throw IllegalArgumentException, but wasn't thrown");
    }

    @Test
    public void getCommittedOffsets() {
        Map<TopicPartition, Long> offsets = new HashMap<>();
        offsets.put(new TopicPartition("topic1", 0), 123L);
        offsets.put(new TopicPartition("topic1", 1), 234L);
        offsets.put(new TopicPartition("topic2", 0), -1L);
        offsets.put(new TopicPartition("topic2", 1), -1L);

        when(consumer.partitionsFor("topic1")).thenReturn(Arrays.asList(
                new PartitionInfo("topic1", 0, null, null, null),
                new PartitionInfo("topic1", 1, null, null, null)));
        when(consumer.partitionsFor("topic2")).thenReturn(Arrays.asList(
                new PartitionInfo("topic2", 0, null, null, null),
                new PartitionInfo("topic2", 1, null, null, null)));

        TopicPartition topic0 = new TopicPartition("topic1", 0);
        when(consumer.committed(Collections.singleton(topic0)))
                .thenReturn(Collections.singletonMap(topic0, new OffsetAndMetadata(123L)));
        TopicPartition topic1 = new TopicPartition("topic1", 1);
        when(consumer.committed(Collections.singleton(topic1)))
                .thenReturn(Collections.singletonMap(topic1, new OffsetAndMetadata(234L)));

        assertThat(client.getCommittedOffsets(Arrays.asList("topic1", "topic2")), is(offsets));
    }

    @Test
    public void getCommittedOffsets_nullTopics() {
        assertThrows(IllegalArgumentException.class,
                () -> client.getCommittedOffsets(null),
                "Expected client.getCommittedOffsets to throw IllegalArgumentException, but wasn't thrown");
    }

    @Test
    public void getCommittedOffset() {
        TopicPartition topic = new TopicPartition("topic", 0);
        when(consumer.committed(Collections.singleton(topic)))
                .thenReturn(Collections.singletonMap(topic, new OffsetAndMetadata(123L)));
        assertThat(client.getCommittedOffset(new TopicPartition("topic", 0)), is(123L));
    }

    @Test
    public void getCommittedOffset_noOffset() {
        assertThat(client.getCommittedOffset(new TopicPartition("topic", 0)), is(-1L));
    }

    @Test
    public void getCommittedOffset_nullTopicPartition() {
        assertThrows(IllegalArgumentException.class,
                () -> client.getCommittedOffset(null),
                "Expected client.getCommittedOffsets to throw IllegalArgumentException, but wasn't thrown");
    }

    @Test
    public void getOffsetForTimes_nullTopics() {
        assertThrows(IllegalArgumentException.class,
                () -> client.getOffsetsForTimes(null,1),
                "Expected client.getOffsetsForTime to throw IllegalArgumentException, but wasn't thrown");
    }

    @Test
    public void getOffsetsForTimes() {
        Map<TopicPartition, OffsetAndTimestamp> offsets = new HashMap<>();
        offsets.put(new TopicPartition("topic1", 0), new OffsetAndTimestamp(123L, 10));
        offsets.put(new TopicPartition("topic1", 1), new OffsetAndTimestamp(234L, 10));
        offsets.put(new TopicPartition("topic2", 0), new OffsetAndTimestamp(0L, 10));
        offsets.put(new TopicPartition("topic2", 1), new OffsetAndTimestamp(0L, 10));

        Map<TopicPartition, Long> longOffsets = offsets.entrySet()
                .stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().offset()));

        when(consumer.partitionsFor("topic1")).thenReturn(Arrays.asList(
                new PartitionInfo("topic1", 0, null, null, null),
                new PartitionInfo("topic1", 1, null, null, null)));
        when(consumer.partitionsFor("topic2")).thenReturn(Arrays.asList(
                new PartitionInfo("topic2", 0, null, null, null),
                new PartitionInfo("topic2", 1, null, null, null)));

        when(consumer.offsetsForTimes(any())).thenReturn(offsets);

        long time = 10L;
        assertThat(client.getOffsetsForTimes(Arrays.asList("topic1", "topic2"), time), is(longOffsets));

        //all were found so no need to look up any ending offsets
        verify(consumer, never()).endOffsets(any());

        verify(consumer).offsetsForTimes(offsetsRequests.capture());

        Map<TopicPartition, Long> requestValue = offsetsRequests.getValue();
        Set<TopicPartition> topicPartitions = requestValue.keySet();
        IntStream.range(0, 2).forEach( i -> {
                assertThat(topicPartitions, hasItem(new TopicPartition("topic1", i)));
                assertThat(topicPartitions, hasItem(new TopicPartition("topic2", i)));
            }
        );
        requestValue.values().forEach(i -> assertThat(i, is(time)));
    }

    @Test
    public void getOffsetsForTimesNullOffsets() {
        Map<TopicPartition, OffsetAndTimestamp> offsets = new HashMap<>();
        offsets.put(new TopicPartition("topic1", 0), null);
        offsets.put(new TopicPartition("topic1", 1), new OffsetAndTimestamp(234L, 10));


        when(consumer.partitionsFor("topic1")).thenReturn(Arrays.asList(
                new PartitionInfo("topic1", 0, null, null, null),
                new PartitionInfo("topic1", 1, null, null, null)));

        when(consumer.offsetsForTimes(any())).thenReturn(offsets);

        Map<TopicPartition, Long> missingOffsets = new HashMap<>();
        missingOffsets.put(new TopicPartition("topic1", 0), 234L);

        when(consumer.endOffsets(any())).thenReturn(missingOffsets);

        Map<TopicPartition, Long> longOffsets = new HashMap<>();
        longOffsets.put(new TopicPartition("topic1", 0), 234L);
        longOffsets.put(new TopicPartition("topic1", 1), 234L);

        long time = 10L;
        assertThat(client.getOffsetsForTimes(Collections.singletonList("topic1"), time), is(longOffsets));

        verify(consumer).offsetsForTimes(offsetsRequests.capture());

        Map<TopicPartition, Long> requestValue = offsetsRequests.getValue();
        Set<TopicPartition> topicPartitions = requestValue.keySet();

        assertThat(topicPartitions, hasItem(new TopicPartition("topic1", 1)));

        requestValue.values().forEach(i -> assertThat(i, is(time)));

        verify(consumer).endOffsets(endOffsetRequest.capture());

        Collection<TopicPartition> endingValue = endOffsetRequest.getValue();
        assertThat(endingValue.size(), is(1));
        assertThat(endingValue, hasItem(new TopicPartition("topic1", 0)));
    }

    @Test
    public void getOffsetsForTimesSomeMissing() {
        Map<TopicPartition, OffsetAndTimestamp> offsets = new HashMap<>();

        Map<TopicPartition, OffsetAndTimestamp> offsetsInRange = new HashMap<>();
        offsetsInRange.put(new TopicPartition("topic1", 0), new OffsetAndTimestamp(123L, 10));
        offsetsInRange.put(new TopicPartition("topic2", 1), new OffsetAndTimestamp(234L, 10));

        Map<TopicPartition, OffsetAndTimestamp> offsetsOutRange = new HashMap<>();
        offsetsOutRange.put(new TopicPartition("topic1", 1), new OffsetAndTimestamp(0L, 10));
        offsetsOutRange.put(new TopicPartition("topic2", 0), new OffsetAndTimestamp(0L, 10));

        offsets.putAll(offsetsOutRange);
        offsets.putAll(offsetsInRange);

        Map<TopicPartition, Long> longOffsetsInRange = offsetsInRange.entrySet()
                .stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().offset()));

        when(consumer.partitionsFor("topic1")).thenReturn(Arrays.asList(
                new PartitionInfo("topic1", 0, null, null, null),
                new PartitionInfo("topic1", 1, null, null, null)));
        when(consumer.partitionsFor("topic2")).thenReturn(Arrays.asList(
                new PartitionInfo("topic2", 0, null, null, null),
                new PartitionInfo("topic2", 1, null, null, null)));

        when(consumer.offsetsForTimes(any())).thenReturn(offsetsInRange);
        when(consumer.endOffsets(any())).thenReturn(longOffsetsInRange);

        long time = 10L;
        assertThat(client.getOffsetsForTimes(Arrays.asList("topic1", "topic2"), time), is(longOffsetsInRange));

        verify(consumer).offsetsForTimes(offsetsRequests.capture());

        Map<TopicPartition, Long> requestValue = offsetsRequests.getValue();
        Set<TopicPartition> topicPartitions = requestValue.keySet();
        IntStream.range(0, 2).forEach( i -> {
                    assertThat(topicPartitions, hasItem(new TopicPartition("topic1", i)));
                    assertThat(topicPartitions, hasItem(new TopicPartition("topic2", i)));
                }
        );
        requestValue.values().forEach(i -> assertThat(i, is(time)));

        verify(consumer).endOffsets(endOffsetRequest.capture());

        Collection<TopicPartition> endingValue = endOffsetRequest.getValue();
        assertThat(endingValue.size(), is(2));
        assertThat(endingValue, hasItem(new TopicPartition("topic1", 1)));
        assertThat(endingValue, hasItem(new TopicPartition("topic2", 0)));
    }

    @Test
    public void commitOffsets_null() {
        assertThrows(IllegalArgumentException.class,
                () -> client.commitOffsets(null),
                "Expected client.commitOffsets to throw IllegalArgumentException, but wasn't thrown");

    }

    @Test
    public void commitOffsets(){
        Map<TopicPartition, Long> offsets = new HashMap<>();
        offsets.put(new TopicPartition("topic1", 0), 123L);
        offsets.put(new TopicPartition("topic1", 1), 234L);
        offsets.put(new TopicPartition("topic2", 0), 0L);
        offsets.put(new TopicPartition("topic2", 1), 0L);

        client.commitOffsets(offsets);

        verify(consumer).commitSync(commitRequest.capture());

        Map<TopicPartition, OffsetAndMetadata> request = commitRequest.getValue();
        request.forEach((k, v) -> assertThat(v.offset(), is(offsets.get(k))));
    }

    @Test
    public void commitOffsets_negativeValue(){
        assertThrows(IllegalArgumentException.class,
                () -> client.commitOffsets(Collections.singletonMap(new TopicPartition("topic1", 0), -1L)),
                "Expected client.commitOffsets to throw IllegalArgumentException, but wasn't thrown");
    }

    @Test
    public void commitOffsets_nullValue(){
        assertThrows(NullPointerException.class,
                () -> client.commitOffsets(Collections.singletonMap(new TopicPartition("topic1", 0), null)),
                "Expected client.commitOffsets to throw NullPointerException, but wasn't thrown");
    }

    @Test
    public void getPartitionsFor() {
        when(consumer.partitionsFor("topic1")).thenReturn(Arrays.asList(
                new PartitionInfo("topic1", 0, null, null, null),
                new PartitionInfo("topic1", 1, null, null, null)));
        when(consumer.partitionsFor("topic2")).thenReturn(Arrays.asList(
                new PartitionInfo("topic2", 0, null, null, null),
                new PartitionInfo("topic2", 1, null, null, null)));

        assertThat(client.getPartitionsFor(Arrays.asList("topic1", "topic2")), is(Arrays.asList(
                new TopicPartition("topic1", 0),
                new TopicPartition("topic1", 1),
                new TopicPartition("topic2", 0),
                new TopicPartition("topic2", 1)
        )));
    }

    @Test
    public void getPartitionsFor_nullTopics() {

        assertThrows(IllegalArgumentException.class,
                () -> client.getPartitionsFor(null),
                "Expected client.getPartitionsFor to throw IllegalArgumentException, but wasn't thrown");
    }

    @Test
    public void close_providedByConsumer() throws IOException {
        client.close();
        verify(consumer, never()).close();
    }

    @Test
    public void close_notProvidedByConsumer() throws IOException {
        client = new ConsumerOffsetClient(consumer, false);
        client.close();
        verify(consumer).close();
    }

}
