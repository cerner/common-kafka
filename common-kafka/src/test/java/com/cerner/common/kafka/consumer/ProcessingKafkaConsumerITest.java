package com.cerner.common.kafka.consumer;

import com.cerner.common.kafka.KafkaTests;
import com.cerner.common.kafka.admin.KafkaAdminClient;
import com.cerner.common.kafka.consumer.assignors.FairAssignor;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertThat;

// Integration testing of ProcessingKafkaConsumer client
public class ProcessingKafkaConsumerITest {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProcessingKafkaConsumerITest.class);

    private static final double FAIL_PERCENTAGE = 0.6; // 60%
    private static final long MAX_SLEEP = 1500; // 1.5s
    private static final long CONSUMER_REINIT_WAIT_TIME_MAX = 5000; // 5s - max time we wait before re-initializing our consumer
    private static final long CONSUMER_SHUTDOWN_TIME_MAX = 30000; // 30s - max time we wait before shutting down our consumer
    private static final long CONSUMER_STARTUP_STAGGER_TIME_MAX = 2000; // 2s - max interval between consecutive consumer startup
    private static final long CONSUMER_POLL_TIMEOUT = 1000; // 1s
    private static final long CONSUMER_POLL_INTERVAL_TIMEOUT = 1000; // 1s - max time between polls before rebalancing consumer group
    private static final long CONSUMER_THREAD_JOIN_TIMEOUT = 5000; // 5s - max time we wait for consumer thread to finish running
    private static final int TOPICS = 4;
    private static final int PARTITIONS = 8;
    private static final int CONSUMERS = 5;
    private static final int MESSAGES_PER_TOPIC = 50;
    private static final long HISTORY_CHECK_TIME = 15000; // 15s - how often to check/print processing history during testing

    private static Properties CONSUMER_PROPERTIES = new Properties();

    private static KafkaAdminClient kafkaAdminClient;

    @Rule
    public TestName name = new TestName();

    @BeforeClass
    public static void startup() throws Exception {
        KafkaTests.startTest();
        kafkaAdminClient = new KafkaAdminClient(KafkaTests.getProps());

        CONSUMER_PROPERTIES.putAll(KafkaTests.getProps());
        CONSUMER_PROPERTIES.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, FairAssignor.class.getName());
        CONSUMER_PROPERTIES.setProperty(ProcessingConfig.COMMIT_SIZE_THRESHOLD_PROPERTY, "5"); // >= 5 offsets in a partition
        CONSUMER_PROPERTIES.setProperty(ProcessingConfig.COMMIT_TIME_THRESHOLD_PROPERTY, "5000"); // >= 5s since last commit
        CONSUMER_PROPERTIES.setProperty(ProcessingConfig.FAIL_PAUSE_TIME_PROPERTY, "2500"); // 2.5s
        CONSUMER_PROPERTIES.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        CONSUMER_PROPERTIES.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        CONSUMER_PROPERTIES.remove(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG);
        CONSUMER_PROPERTIES.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    }

    @AfterClass
    public static void shutdown() throws Exception {
        kafkaAdminClient.close();
        KafkaTests.endTest();
    }

    public enum Condition {
        FAILURES,
        SHUTDOWN_CONSUMERS,
        TIMEOUT_CONSUMERS
    }

    @Test
    public void processing() throws IOException, InterruptedException {
        runProcessing();
    }

    @Test
    public void processingWithFailures() throws IOException, InterruptedException {
        runProcessing(Condition.FAILURES);
    }

    @Test
    public void processingWithShutdown() throws IOException, InterruptedException {
        runProcessing(Condition.SHUTDOWN_CONSUMERS);
    }

    @Test
    public void processingWithTimeouts() throws IOException, InterruptedException {
        runProcessing(Condition.TIMEOUT_CONSUMERS);
    }

    @Test
    public void processingWithFailuresAndShutdown() throws IOException, InterruptedException {
        runProcessing(Condition.FAILURES, Condition.SHUTDOWN_CONSUMERS);
    }

    @Test
    public void processingWithFailuresAndTimeouts() throws IOException, InterruptedException {
        runProcessing(Condition.FAILURES, Condition.TIMEOUT_CONSUMERS);
    }

    @Test
    public void processingWithShutdownAndTimeouts() throws IOException, InterruptedException {
        runProcessing(Condition.SHUTDOWN_CONSUMERS, Condition.TIMEOUT_CONSUMERS);
    }

    @Test
    public void processingWithFailuresAndShutdownAndTimeouts() throws IOException, InterruptedException {
        runProcessing(Condition.FAILURES, Condition.SHUTDOWN_CONSUMERS, Condition.TIMEOUT_CONSUMERS);
    }

    public void runProcessing(Condition... conditions) throws IOException, InterruptedException {
        runProcessing(Arrays.asList(conditions));
    }

    // If you need to debug this add 'log4j.logger.com.cerner.common.kafka=DEBUG' to log4j.properties in src/test/resources
    // Save output to a file as there will be a lot
    public void runProcessing(Collection<Condition> conditions) throws IOException, InterruptedException {
        boolean failures = conditions.contains(Condition.FAILURES);
        boolean shutdownConsumers = conditions.contains(Condition.SHUTDOWN_CONSUMERS);
        boolean timeoutConsumers= conditions.contains(Condition.TIMEOUT_CONSUMERS);

        AtomicBoolean finishedProcessing = new AtomicBoolean(false);
        Map<RecordId, List<ConsumerAction>> recordHistory = new ConcurrentHashMap<>();

        Set<String> topicList = new HashSet<>();

        for(int i=0; i<TOPICS; i++) {
            String topic = "topic-" + i + name.getMethodName();
            topicList.add(topic);

            // Only 1 replica since our testing only has 1 broker
            kafkaAdminClient.createTopic(topic, PARTITIONS, 1, new Properties());
        }

        // Setup consumer threads
        Properties consumerProperties = new Properties();
        consumerProperties.putAll(CONSUMER_PROPERTIES);
        consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "processing-group-" + name.getMethodName());

        if (timeoutConsumers) {
            consumerProperties.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG,
                    String.valueOf(CONSUMER_POLL_INTERVAL_TIMEOUT));
        }
        ProcessingConfig config = new ProcessingConfig(consumerProperties);

        double failPercentage = failures ? FAIL_PERCENTAGE : 0.0;

        List<ConsumerThread> threads = new ArrayList<>();
        for(int i=0; i<CONSUMERS; i++) {
            ConsumerThread thread = new ConsumerThread("consumer" + i, config, topicList, shutdownConsumers,
                    failPercentage, recordHistory, finishedProcessing);
            threads.add(thread);
        }

        // Write some data
        Properties producerProperties = new Properties();
        producerProperties.putAll(KafkaTests.getProps());
        producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        Producer<String, String> producer = new KafkaProducer<String, String>(producerProperties);

        int writtenRecords = 0;
        for(String topic : topicList) {
            LOGGER.info("Writing {} messages to topic {}", MESSAGES_PER_TOPIC, topic);

            for(int message=0; message < MESSAGES_PER_TOPIC; message++) {

                int partition = message % PARTITIONS;
                long offset = message / PARTITIONS;

                String key = "key-" + topic + "-" + partition + "-" + message;
                String value = "value" + message;

                recordHistory.put(new RecordId(new ConsumerRecord<>(topic, partition, offset, key, value)),
                        new CopyOnWriteArrayList<>());

                producer.send(new ProducerRecord<>(topic, partition, key, value));
                writtenRecords++;
            }
        }

        producer.flush();

        // Start consumers
        Random random = new Random();
        for(int i=0; i<CONSUMERS; i++) {
            ConsumerThread thread = threads.get(i);
            LOGGER.info("Starting consumer {} of {}", i + 1, CONSUMERS);
            thread.start();
            long staggerTime = (long) (random.nextFloat() * CONSUMER_STARTUP_STAGGER_TIME_MAX);
            Thread.sleep(staggerTime);
        }

        // Processing loop
        long nextHistoryCheck = System.currentTimeMillis() + HISTORY_CHECK_TIME;
        int committedRecords = recordsWithAction(recordHistory, Action.COMMITTED);
        LOGGER.info("Committed records: {} of {}", committedRecords, writtenRecords);

        while(committedRecords < writtenRecords) {
            Thread.sleep(1000L);

            if (System.currentTimeMillis() >= nextHistoryCheck) {
                // Verify the history is as we expect (excluding terminal conditions)
                verifyHistory(recordHistory, false, false);

                for(Map.Entry<RecordId, List<ConsumerAction>> entry : recordHistory.entrySet()) {
                    LOGGER.debug("Record history: {} - {}", entry.getKey(), entry.getValue());
                    if (!findRecordAction(entry, Action.ACKED)) {
                        LOGGER.debug("Unprocessed Record: {}", entry.getKey());
                    }
                    if (!findRecordAction(entry, Action.COMMITTED)) {
                        LOGGER.debug("Uncommitted Record: {}", entry.getKey());
                    }
                }

                nextHistoryCheck = System.currentTimeMillis() + HISTORY_CHECK_TIME;
            }

            committedRecords = recordsWithAction(recordHistory, Action.COMMITTED);
            LOGGER.info("Committed records: {} of {}", committedRecords, writtenRecords);
            LOGGER.info("Processed records: {} of {}", recordsWithAction(recordHistory, Action.ACKED), writtenRecords);
        }

        finishedProcessing.set(true);

        for(Map.Entry<RecordId, List<ConsumerAction>> entry : recordHistory.entrySet()) {
            LOGGER.debug("Record history: {} - {}", entry.getKey(), entry.getValue());
        }

        // Verify the history is as we expect (including terminal conditions)
        verifyHistory(recordHistory, true, !(shutdownConsumers || timeoutConsumers));

        // Cleanup threads, if there are any problems just log a warning since the test was otherwise successful
        for(ConsumerThread thread : threads) {
            thread.join(CONSUMER_THREAD_JOIN_TIMEOUT);
            if (thread.isAlive()) {
                LOGGER.warn("Could not join thread {} after {} ms", thread.id, CONSUMER_THREAD_JOIN_TIMEOUT);
            }
            try {
                thread.close();
            } catch (Exception e) {
                LOGGER.warn("Could not close thread {}", thread.id, e);
            }
        }

        producer.close();
    }

    private int recordsWithAction(Map<RecordId, List<ConsumerAction>> recordHistory, Action action) {
        int records = 0;
        for(Map.Entry<RecordId, List<ConsumerAction>> entry : recordHistory.entrySet()) {
            if (findRecordAction(entry, action)) {
                ++records;
            }
        }
        return records;
    }

    private boolean findRecordAction(Map.Entry<RecordId, List<ConsumerAction>> entry, Action action) {
        for (ConsumerAction consumerAction : entry.getValue()) {
            if (consumerAction.getAction() == action) {
                return true;
            }
        }
        return false;
    }

    private void verifyHistory(Map<RecordId, List<ConsumerAction>> recordHistory, boolean end, boolean verifyLastAction) {
        for(Map.Entry<RecordId, List<ConsumerAction>> recordHist : recordHistory.entrySet()) {
            int read = 0;
            int acked = 0;
            int failed = 0;
            int committed = 0;
            // Track unique consumers that acked the record.
            Set<String> ackedBy = new HashSet<>();

            List<ConsumerAction> actionHistory = recordHist.getValue();

            // Verify read/ack/fail/committed history for each record.
            for(ConsumerAction action : actionHistory) {
                switch (action.getAction()) {
                case READ:
                    ++read;
                    break;
                case ACKED:
                    assertThat("Record " + recordHist.getKey() + " was acked prior to being read. History = " + actionHistory,
                            read, is(greaterThanOrEqualTo(1)));
                    ackedBy.add(action.getConsumerId());
                    ++acked;
                    break;
                case FAILED:
                    assertThat("Record " + recordHist.getKey() + " was failed prior to being read. History = " + actionHistory,
                            read, is(greaterThanOrEqualTo(1)));
                    ++failed;
                    break;
                case COMMITTED:
                    assertThat("Record " + recordHist.getKey() + " was committed prior to being read. History = " + actionHistory,
                            read, is(greaterThanOrEqualTo(1)));
                    assertThat("Record " + recordHist.getKey() + " was committed prior to being acked. History = " + actionHistory,
                            acked, is(greaterThanOrEqualTo(1)));
                    ++committed;
                    break;
                }
            }

            // Each record should be read prior to ack or fail
            assertThat("Record " + recordHist.getKey() + " read count is less than acked+failed. History = " + actionHistory,
                    read, is(greaterThanOrEqualTo(acked + failed)));

            if (end) {
                // Verify record was acked at least once.
                assertThat("Record " + recordHist.getKey() + " never saw an ack in its history " + actionHistory,
                    acked, is(greaterThanOrEqualTo(1)));

                // Verify record was committed at least once.
                assertThat("Record " + recordHist.getKey() + " was not committed at least once. History = " + actionHistory,
                        committed, is(greaterThanOrEqualTo(1)));

                if (verifyLastAction) {
                    // Verify that the last action for each record is a commit.
                    Action lastAction = actionHistory.get(actionHistory.size() - 1).getAction();
                    assertThat("Record " + recordHist.getKey() + " final action was not commit. History = " + actionHistory,
                            lastAction, is(Action.COMMITTED));
                }
            }
        }
    }

    private static class RecordId {

        private final String topic;
        private final String key;
        private final String value;
        private final int partition;
        private final long offset;

        public RecordId(ConsumerRecord<String, String> record) {
            this.topic = record.topic();
            this.key = record.key();
            this.value = record.value();
            this.partition = record.partition();
            this.offset = record.offset();
        }

        public ConsumerRecord<String, String> toRecord() {
            return new ConsumerRecord<>(topic, partition, offset, key, value);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            RecordId recordId = (RecordId) o;

            if (partition != recordId.partition) return false;
            if (offset != recordId.offset) return false;
            if (!topic.equals(recordId.topic)) return false;
            if (!key.equals(recordId.key)) return false;
            return value.equals(recordId.value);

        }

        @Override
        public int hashCode() {
            int result = topic.hashCode();
            result = 31 * result + key.hashCode();
            result = 31 * result + value.hashCode();
            result = 31 * result + partition;
            result = 31 * result + (int) (offset ^ (offset >>> 32));
            return result;
        }

        @Override
        public String toString() {
            return "RecordId{" +
                    "topic='" + topic + '\'' +
                    ", key='" + key + '\'' +
                    ", value='" + value + '\'' +
                    ", partition=" + partition +
                    ", offset=" + offset +
                    '}';
        }
    }

    enum Action {
        READ, FAILED, ACKED, COMMITTED;
    }

    private class ConsumerAction {
        public ConsumerAction(String consumerId, Action action) {
            this.consumerId = consumerId;
            this.action = action;
        }

        private final String consumerId;
        private final Action action;

        public String getConsumerId() {
            return consumerId;
        }

        public Action getAction() {
            return action;
        }

        @Override
        public String toString() {
            return "(" + consumerId + "," + action + ")";
        }
    }

    private class ConsumerThread extends Thread implements Closeable {
        private final Logger LOGGER = LoggerFactory.getLogger(ConsumerThread.class);

        private final Random random = new Random();
        private ProcessingKafkaConsumer<String, String> consumer;
        private String consumerId;
        private long nextConsumerShutdownTime;

        private final String id;
        private final ProcessingConfig config;
        private final Set<String> topics;
        private final boolean shutdown;
        private final double failPercentage;
        private final Map<RecordId, List<ConsumerAction>> recordHistory;
        private final AtomicBoolean finishedProcessing;
        private final Map<RecordId, Long> recordsToBeProcessed = new HashMap<>();
        private final Set<RecordId> recordsToBeCommitted = ConcurrentHashMap.newKeySet();
        private final Set<RecordId> recordsCommittedDuringAck = ConcurrentHashMap.newKeySet();
        private volatile boolean ackInProgress;

        public ConsumerThread(String id, ProcessingConfig config, Set<String> topics, boolean shutdown, double failPercentage,
                              Map<RecordId, List<ConsumerAction>> recordHistory, AtomicBoolean finishedProcessing) {
            this.id = id;
            this.config = config;
            this.topics = topics;
            this.shutdown = shutdown;
            this.failPercentage = failPercentage;
            this.recordHistory = recordHistory;
            this.finishedProcessing = finishedProcessing;
        }

        private void addRecordHistory(RecordId recordId, Action action) {
            recordHistory.get(recordId).add(new ConsumerAction(consumerId, action));
        }

        @Override
        public void run() {
            while(!finishedProcessing.get()) {
                LOGGER.debug("{} running", id);
                long currentTime = System.currentTimeMillis();

                try {
                    if (consumer == null)
                        initializeConsumer();

                    if (shutdown && nextConsumerShutdownTime <= currentTime) {
                        long sleepTime = (long) (random.nextFloat() * CONSUMER_REINIT_WAIT_TIME_MAX);
                        LOGGER.info("Shutting down {} for {} ms", id, sleepTime);
                        try {
                            IOUtils.closeQuietly(consumer);
                        } finally {
                            consumer = null;
                        }
                        recordsToBeProcessed.clear();
                        recordsToBeCommitted.clear();

                        Thread.sleep(sleepTime);

                        continue;
                    }

                    // Read next record
                    Optional<ConsumerRecord<String, String>> optional = consumer.nextRecord(CONSUMER_POLL_TIMEOUT);
                    if (optional.isPresent()) {
                        RecordId recordId = new RecordId(optional.get());
                        addRecordHistory(recordId, Action.READ);
                        long sleepTime = (long) (random.nextDouble() * MAX_SLEEP);
                        LOGGER.debug("{} reading record {} with sleep time {}", new Object[] {id, recordId, sleepTime});

                        recordsToBeProcessed.put(recordId, currentTime + sleepTime);
                    }

                    // Process any records that are ready
                    for(Map.Entry<RecordId, Long> entry : new HashMap<>(recordsToBeProcessed).entrySet()) {
                        RecordId recordId = entry.getKey();
                        long time = entry.getValue();
                        ConsumerRecord<String, String> unprocessedRecord = recordId.toRecord();

                        if (time <= currentTime) {
                            LOGGER.debug("{} removing record {}", id, recordId);
                            recordsToBeProcessed.remove(recordId);

                            if (failPercentage <= random.nextDouble()) {
                                LOGGER.debug("{} ack'ing record {}", id, recordId);
                                recordsToBeCommitted.add(recordId);
                                boolean acked = false;
                                try {
                                    acked = consumer.ack(unprocessedRecord);
                                } catch (CommitFailedException e) {
                                    // Still acked, although not committed
                                    addRecordHistory(recordId, Action.ACKED);
                                    throw e;
                                }

                                if (acked) {
                                    // Mark as acked prior to committed
                                    addRecordHistory(recordId, Action.ACKED);
                                    for (RecordId committedRecordId : recordsCommittedDuringAck) {
                                        addRecordHistory(committedRecordId, Action.COMMITTED);
                                    }
                                    recordsCommittedDuringAck.clear();
                                } else {
                                    recordsToBeCommitted.remove(recordId);
                                }
                            }
                            else{
                                LOGGER.debug("{} failing record {}", id, recordId);
                                if (consumer.fail(unprocessedRecord)) {
                                    addRecordHistory(recordId, Action.FAILED);
                                }
                            }
                        }
                        else {
                            LOGGER.debug("{} not processing record yet {} waiting {}ms", new Object[] { id, recordId,
                                    (time - currentTime) });
                        }
                    }
                    if (recordsToBeProcessed.isEmpty())
                        LOGGER.debug("{} nothing to process", id);

                    Thread.sleep(100L);
                } catch (Exception e) {
                    LOGGER.error("{} saw exception", id, e);
                }
            }
        }

        private void initializeConsumer() {
            consumer = new CommitTrackingProcessingKafkaConsumer<>(config);
            consumerId = consumer.toString(); // generate a unique id
            consumer.subscribe(topics);
            nextConsumerShutdownTime = System.currentTimeMillis() + (long) (random.nextFloat() * CONSUMER_SHUTDOWN_TIME_MAX);
        }

        @Override
        public void close() throws IOException {
            if (consumer != null)
                consumer.close();
        }

        private class CommitTrackingProcessingKafkaConsumer<K, V> extends ProcessingKafkaConsumer<K, V> {

            public CommitTrackingProcessingKafkaConsumer(ProcessingConfig config) {
                super(config);
            }

            @Override
            public boolean ack(ConsumerRecord<K, V> record) {
                // Flip ack in progress indicator during the ack operation.
                ackInProgress = true;
                try {
                    return super.ack(record);
                } finally {
                    ackInProgress = false;
                }
            }

            @Override
            protected void commitOffsets(Map<TopicPartition, OffsetAndMetadata> offsetsToCommit) throws KafkaException {
                super.commitOffsets(offsetsToCommit);

                // Record the commit in the history for all applicable records
                for (RecordId recordId : recordsToBeCommitted) {
                    OffsetAndMetadata offset = offsetsToCommit.get(new TopicPartition(recordId.topic, recordId.partition));
                    if (offset != null && offset.offset() > recordId.offset) {
                        recordsToBeCommitted.remove(recordId);
                        // Delay history recording if there is an ack in progress so that we can verify ack/commit order
                        if (ackInProgress) {
                            recordsCommittedDuringAck.add(recordId);
                        } else {
                            addRecordHistory(recordId, Action.COMMITTED);
                        }
                    }
                }
            }

            @Override
            ConsumerRebalanceListener getRebalanceListener() {
                return new CommitTrackingProcessingRebalanceListener();
            }

            private class CommitTrackingProcessingRebalanceListener extends ProcessingRebalanceListener {

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitionsAssigned) {
                    super.onPartitionsAssigned(partitionsAssigned);

                    // Remove all commit-pending records we are no longer assigned to
                    for (RecordId recordId : recordsToBeCommitted) {
                        if (!partitionsAssigned.contains(new TopicPartition(recordId.topic, recordId.partition))) {
                            recordsToBeCommitted.remove(recordId);
                        }
                    }
                }
            }
        }
    }
}