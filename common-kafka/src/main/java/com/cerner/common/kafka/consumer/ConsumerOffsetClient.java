package com.cerner.common.kafka.consumer;

import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.BytesDeserializer;

import java.io.Closeable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Simple API to fetch offsets from Kafka
 */
public class ConsumerOffsetClient implements Closeable {

    protected final boolean providedByUser;
    protected final Consumer<Object, Object> consumer;

    /**
     * Constructs the consumer offset client
     *
     * @param properties
     *      the properties used to connect to Kafka. The {@link ConsumerConfig#BOOTSTRAP_SERVERS_CONFIG} must be provided while
     *      {@link ConsumerConfig#GROUP_ID_CONFIG} is optional and a random group will be provided if not supplied
     * @throws org.apache.kafka.common.KafkaException
     *      if there is an issue creating the client
     */
    public ConsumerOffsetClient(Properties properties) {
        this(getConsumer(properties), false);
    }

    /**
     * Constructs the consumer offset client
     *
     * @param consumer
     *      the consumer used to interact with Kafka. It is up to the user to maintain the lifecycle of this consumer
     */
    public ConsumerOffsetClient(Consumer<Object, Object> consumer) {
        this(consumer, true);
    }

    ConsumerOffsetClient(Consumer<Object, Object> consumer, boolean providedByUser) {
        if (consumer == null)
            throw new IllegalArgumentException("consumer cannot be null");

        this.consumer = consumer;
        this.providedByUser = providedByUser;
    }

    private static Consumer<Object, Object> getConsumer(Properties properties) {
        if (properties == null)
            throw new IllegalArgumentException("properties cannot be null");

        Properties consumerProperties = new Properties();
        consumerProperties.putAll(properties);

        // Ensure the serializer configuration is set though its not needed
        consumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, BytesDeserializer.class.getName());
        consumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, BytesDeserializer.class.getName());

        String group = consumerProperties.getProperty(ConsumerConfig.GROUP_ID_CONFIG);

        // Add some random consumer group name to avoid any issues
        if (group == null)
            consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-consumer-offset-client-" + UUID.randomUUID());

        return new KafkaConsumer<>(consumerProperties);
    }

    /**
     * Returns the end/latests offsets for the provided topics
     *
     * @param topics
     *      collection of Kafka topics
     * @return the end/latests offsets for the provided topics
     * @throws org.apache.kafka.common.KafkaException
     *      if there is an issue fetching the end offsets
     * @throws IllegalArgumentException
     *      if topics is null
     */
    public Map<TopicPartition, Long> getEndOffsets(Collection<String> topics) {
        if (topics == null)
            throw new IllegalArgumentException("topics cannot be null");

        Collection<TopicPartition> partitions = getPartitionsFor(topics);
        return consumer.endOffsets(partitions);
    }

    /**
     * Returns the beginning/earliest offsets for the provided topics
     *
     * @param topics
     *      collection of Kafka topics
     * @return the beginning/earliest offsets for the provided topics
     * @throws org.apache.kafka.common.KafkaException
     *      if there is an issue fetching the beginning offsets
     * @throws IllegalArgumentException
     *      if topics is null
     */
    public Map<TopicPartition, Long> getBeginningOffsets(Collection<String> topics) {
        if (topics == null)
            throw new IllegalArgumentException("topics cannot be null");

        Collection<TopicPartition> partitions = getPartitionsFor(topics);
        return consumer.beginningOffsets(partitions);
    }

    /**
     * Returns the offsets for the provided topics at the specified {@code time}.  If no offset for a topic or partition
     * is available at the specified {@code time} then the {@link #getEndOffsets(Collection) latest} offsets
     * for that partition are returned.
     *
     * @param topics
     *      collection of Kafka topics
     * @param time the specific time at which to retrieve offsets
     * @return the offsets for the provided topics at the specified time.
     * @throws org.apache.kafka.common.KafkaException
     *      if there is an issue fetching the offsets
     * @throws IllegalArgumentException
     *      if topics is null
     */
    public Map<TopicPartition, Long> getOffsetsForTimes(Collection<String> topics, long time) {
        if (topics == null)
            throw new IllegalArgumentException("topics cannot be null");

        Collection<TopicPartition> partitions = getPartitionsFor(topics);

        //Find all the offsets at a specified time.
        Map<TopicPartition, Long> topicTimes = getPartitionsFor(topics)
                .stream().collect(Collectors.toMap(Function.identity(), s -> time));
        Map<TopicPartition, OffsetAndTimestamp> foundOffsets = consumer.offsetsForTimes(topicTimes);


        //merge the offsets together into a single collection.
        Map<TopicPartition, Long> offsets = new HashMap<>();
        offsets.putAll(foundOffsets.entrySet()
                .stream()
                //Filter the null offsets.
                .filter(e -> e.getValue() !=null)
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().offset())));

        //if some partitions do not have offsets at the specified time, find the latest offsets of the partitions for that time.
        List<TopicPartition> missingPartitions = partitions.stream()
                .filter(t -> !offsets.containsKey(t)).collect(Collectors.toList());
        if(!missingPartitions.isEmpty()) {
            Map<TopicPartition, Long> missingOffsets = consumer.endOffsets(missingPartitions);
            offsets.putAll(missingOffsets);
        }

        return offsets;
    }

    /**
     * Returns the committed offsets for the consumer group and the provided topics or -1 if no offset is found
     *
     * @param topics
     *      collection of Kafka topics
     * @return the committed offsets for the consumer group and the provided topics or -1 if no offset is found
     * @throws org.apache.kafka.common.KafkaException
     *      if there is an issue fetching the committed offsets
     * @throws IllegalArgumentException
     *      if topics is null
     */
    public Map<TopicPartition, Long> getCommittedOffsets(Collection<String> topics) {
        if (topics == null)
            throw new IllegalArgumentException("topics cannot be null");

        Collection<TopicPartition> partitions = getPartitionsFor(topics);
        Map<TopicPartition, Long> offsets = new HashMap<>();

        partitions.forEach(topicPartition -> {
            offsets.put(topicPartition, getCommittedOffset(topicPartition));
        });

        return offsets;
    }

    /**
     * Commits the {@code offsets}.
     *
     * @param offsets the offsets to commit.
     * @throws IllegalArgumentException if the {@code offsets} are {@code null} or contains a negative value.
     * @throws NullPointerException if the {@code offsets} contains a {@code null} value
     * @throws org.apache.kafka.common.KafkaException
     *      if there is an issue committing the offsets
     */
    public void commitOffsets(Map<TopicPartition, Long> offsets){
        if(offsets == null)
            throw new IllegalArgumentException("offsets cannot be null");

        Map<TopicPartition, OffsetAndMetadata> offsetsToWrite = offsets.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> new OffsetAndMetadata(e.getValue())));

        consumer.commitSync(offsetsToWrite);
    }

    /**
     * Returns the committed offset or -1 for the consumer group and the given topic partition
     *
     * @param topicPartition
     *      a topic partition
     * @return the committed offset or -1 for the consumer group and the given topic partition
     * @throws org.apache.kafka.common.KafkaException
     *      if there is an issue fetching the committed offset
     * @throws IllegalArgumentException
     *      if topicPartition is null
     */
    public long getCommittedOffset(TopicPartition topicPartition) {
        if (topicPartition == null)
            throw new IllegalArgumentException("topicPartition cannot be null");

        OffsetAndMetadata offsetAndMetadata = consumer.committed(topicPartition);
        return offsetAndMetadata == null ? -1L : offsetAndMetadata.offset();
    }

    /**
     * Returns the partitions for the provided topics
     *
     * @param topics
     *       collection of Kafka topics
     * @return the partitions for the provided topics
     * @throws org.apache.kafka.common.KafkaException
     *      if there is an issue fetching the partitions
     * @throws IllegalArgumentException
     *      if topics is null
     */
    public Collection<TopicPartition> getPartitionsFor(Collection<String> topics) {
        if (topics == null)
            throw new IllegalArgumentException("topics cannot be null");
        return topics.stream()
                .flatMap(topic -> {
                    List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);

                    // partition infos could be null if the topic does not exist
                    if (partitionInfos == null)
                        return Collections.<TopicPartition>emptyList().stream();

                    return partitionInfos.stream()
                            .map(partitionInfo -> new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));
                })
                .collect(Collectors.toList());

    }

    @Override
    public void close() {
        if (!providedByUser)
            IOUtils.closeQuietly(consumer);
    }
}
