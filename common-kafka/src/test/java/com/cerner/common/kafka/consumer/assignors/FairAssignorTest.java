package com.cerner.common.kafka.consumer.assignors;

import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.clients.consumer.internals.PartitionAssignor.Subscription;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FairAssignorTest {

    private FairAssignor assignor = new FairAssignor();

    @Test
    public void testOneConsumerNoTopic() {
        String consumerId = "consumer";

        Map<String, Integer> partitionsPerTopic = new HashMap<>();

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic,
                Collections.singletonMap(consumerId, new Subscription(Collections.emptyList())));
        assertEquals(Collections.singleton(consumerId), assignment.keySet());
        assertTrue(assignment.get(consumerId).isEmpty());
    }

    @Test
    public void testOneConsumerNonexistentTopic() {
        String topic = "topic";
        String consumerId = "consumer";

        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic, 0);

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic,
                Collections.singletonMap(consumerId, new Subscription(Collections.singletonList(topic))));

        assertEquals(Collections.singleton(consumerId), assignment.keySet());
        assertTrue(assignment.get(consumerId).isEmpty());
    }

    @Test
    public void testOneConsumerOneTopic() {
        String topic = "topic";
        String consumerId = "consumer";

        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic, 3);

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic,
                Collections.singletonMap(consumerId, new Subscription(Collections.singletonList(topic))));
        assertEquals(Arrays.asList(
                new TopicPartition(topic, 0),
                new TopicPartition(topic, 1),
                new TopicPartition(topic, 2)), assignment.get(consumerId));
    }

    @Test
    public void testOnlyAssignsPartitionsFromSubscribedTopics() {
        String topic = "topic";
        String otherTopic = "other";
        String consumerId = "consumer";

        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic, 3);
        partitionsPerTopic.put(otherTopic, 3);

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic,
                Collections.singletonMap(consumerId, new Subscription(Collections.singletonList(topic))));
        assertEquals(Arrays.asList(
                new TopicPartition(topic, 0),
                new TopicPartition(topic, 1),
                new TopicPartition(topic, 2)), assignment.get(consumerId));
    }

    @Test
    public void testSubscriptionIncludesTopicThatDoesNotExist() {
        String topic = "topic";
        String topicDoesNotExist = "doesNotExist";
        String consumerId = "consumer";

        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic, 1);

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic,
                Collections.singletonMap(consumerId,
                new Subscription(Arrays.asList(topic, topicDoesNotExist))));
        assertEquals(Arrays.asList(new TopicPartition(topic, 0)), assignment.get(consumerId));
    }

    @Test
    public void testOneConsumerMultipleTopics() {
        String topic1 = "topic1";
        String topic2 = "topic2";
        String consumerId = "consumer";

        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic1, 1);
        partitionsPerTopic.put(topic2, 2);

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic,
                Collections.singletonMap(consumerId,
                new Subscription(Arrays.asList(topic1, topic2))));
        assertEquals(Arrays.asList(
                new TopicPartition(topic2, 0),
                new TopicPartition(topic2, 1),
                new TopicPartition(topic1, 0)), assignment.get(consumerId));
    }

    @Test
    public void testTwoConsumersOneTopicOnePartition() {
        String topic = "topic";
        String consumer1 = "consumer1";
        String consumer2 = "consumer2";

        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic, 1);

        Map<String, Subscription> consumers = new HashMap<>();
        consumers.put(consumer1, new Subscription(Collections.singletonList(topic)));
        consumers.put(consumer2, new Subscription(Collections.singletonList(topic)));

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, consumers);
        assertEquals(Arrays.asList(new TopicPartition(topic, 0)), assignment.get(consumer1));
        assertEquals(Collections.<TopicPartition>emptyList(), assignment.get(consumer2));
    }

    @Test
    public void testTwoConsumersOneTopicTwoPartitions() {
        String topic = "topic";
        String consumer1 = "consumer1";
        String consumer2 = "consumer2";

        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic, 2);

        Map<String, Subscription> consumers = new HashMap<>();
        consumers.put(consumer1, new Subscription(Collections.singletonList(topic)));
        consumers.put(consumer2, new Subscription(Collections.singletonList(topic)));

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, consumers);
        assertEquals(Arrays.asList(new TopicPartition(topic, 0)), assignment.get(consumer1));
        assertEquals(Arrays.asList(new TopicPartition(topic, 1)), assignment.get(consumer2));
    }

    @Test
    public void testMultipleConsumersMixedTopics() {
        String topic1 = "topic1";
        String topic2 = "topic2";
        String consumer1 = "consumer1";
        String consumer2 = "consumer2";
        String consumer3 = "consumer3";

        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic1, 3);
        partitionsPerTopic.put(topic2, 2);

        Map<String, Subscription> consumers = new HashMap<>();
        consumers.put(consumer1, new Subscription(Collections.singletonList(topic1)));
        consumers.put(consumer2, new Subscription(Arrays.asList(topic1, topic2)));
        consumers.put(consumer3, new Subscription(Collections.singletonList(topic1)));

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, consumers);
        assertEquals(Arrays.asList(
                new TopicPartition(topic1, 0),
                new TopicPartition(topic1, 2)), assignment.get(consumer1));
        assertEquals(Arrays.asList(
                new TopicPartition(topic2, 0),
                new TopicPartition(topic2, 1)), assignment.get(consumer2));
        assertEquals(Arrays.asList(
                new TopicPartition(topic1, 1)), assignment.get(consumer3));
    }

    @Test
    public void testTwoConsumersTwoTopicsSixPartitions() {
        String topic1 = "topic1";
        String topic2 = "topic2";
        String consumer1 = "consumer1";
        String consumer2 = "consumer2";

        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic1, 3);
        partitionsPerTopic.put(topic2, 3);

        Map<String, Subscription> consumers = new HashMap<>();
        consumers.put(consumer1, new Subscription(Arrays.asList(topic1, topic2)));
        consumers.put(consumer2, new Subscription(Arrays.asList(topic1, topic2)));

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, consumers);
        assertEquals(Arrays.asList(
                new TopicPartition(topic1, 0),
                new TopicPartition(topic1, 2),
                new TopicPartition(topic2, 1)), assignment.get(consumer1));
        assertEquals(Arrays.asList(
                new TopicPartition(topic1, 1),
                new TopicPartition(topic2, 0),
                new TopicPartition(topic2, 2)), assignment.get(consumer2));
    }

    @Test
    public void testMultipleConsumersUnbalancedSubscriptions() {
        String topic1 = "topic1";
        String topic2 = "topic2";
        String topic3 = "topic3";
        String topic4 = "topic4";
        String topic5 = "topic5";
        String consumer1 = "consumer1";
        String consumer2 = "consumer2";
        String consumer3 = "consumer3";
        String consumer4 = "consumer4";
        int oddTopicPartitions = 2;
        int evenTopicPartitions = 1;

        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic1, oddTopicPartitions);
        partitionsPerTopic.put(topic2, evenTopicPartitions);
        partitionsPerTopic.put(topic3, oddTopicPartitions);
        partitionsPerTopic.put(topic4, evenTopicPartitions);
        partitionsPerTopic.put(topic5, oddTopicPartitions);

        List<String> oddTopics = Arrays.asList(topic1, topic3, topic5);
        List<String> allTopics = Arrays.asList(topic1, topic2, topic3, topic4, topic5);

        Map<String, Subscription> consumers = new HashMap<>();
        consumers.put(consumer1, new Subscription(allTopics));
        consumers.put(consumer2, new Subscription(oddTopics));
        consumers.put(consumer3, new Subscription(oddTopics));
        consumers.put(consumer4, new Subscription(allTopics));

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, consumers);
        assertEquals(Arrays.asList(
                new TopicPartition(topic2, 0),
                new TopicPartition(topic3, 0)), assignment.get(consumer1));
        assertEquals(Arrays.asList(
                new TopicPartition(topic1, 0),
                new TopicPartition(topic3, 1)), assignment.get(consumer2));
        assertEquals(Arrays.asList(
                new TopicPartition(topic1, 1),
                new TopicPartition(topic5, 0)), assignment.get(consumer3));
        assertEquals(Arrays.asList(
                new TopicPartition(topic4, 0),
                new TopicPartition(topic5, 1)), assignment.get(consumer4));
    }
}
