package com.cerner.common.kafka.consumer;

import com.cerner.common.kafka.KafkaTests;
import com.cerner.common.kafka.admin.KafkaAdminClient;
import com.cerner.common.kafka.consumer.assignors.FairAssignor;
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

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * Integration test for a specific deadlocking condition in ProcessingKafkaConsumer client after repeated rebalances.
 */
public class ProcessingKafkaConsumerRebalanceIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProcessingKafkaConsumerITest.class);

    private static final long MAX_SLEEP = 150; // 150 ms
    private static final long CONSUMER_POLL_TIMEOUT = 1000; // 1s
    private static final long CONSUMER_THREAD_JOIN_TIMEOUT = 5000; // 5s - max time we wait for consumer thread to finish running
    private static final int PARTITIONS = 1;
    private static final int MESSAGES_PER_TOPIC = 25;
    private static final long HISTORY_CHECK_TIME = 1000; // 1s - how often to check/print processing history during testing

    private static Properties CONSUMER_PROPERTIES = new Properties();

    private static KafkaAdminClient kafkaAdminClient;

    @Rule
    public TestName name = new TestName();

    @BeforeClass
    public static void startup() throws Exception {
        KafkaTests.startTest();
        kafkaAdminClient = new KafkaAdminClient(KafkaTests.getProps());

        CONSUMER_PROPERTIES.putAll(KafkaTests.getProps());
        CONSUMER_PROPERTIES.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "3000");
        CONSUMER_PROPERTIES.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        CONSUMER_PROPERTIES.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, FairAssignor.class.getName());
        CONSUMER_PROPERTIES.setProperty(ProcessingConfig.COMMIT_SIZE_THRESHOLD_PROPERTY, "1"); // >= 1 offsets in a partition
        CONSUMER_PROPERTIES.setProperty(ProcessingConfig.COMMIT_TIME_THRESHOLD_PROPERTY, "1000"); // >= 1s since last commit
        CONSUMER_PROPERTIES.setProperty(ProcessingConfig.FAIL_PAUSE_TIME_PROPERTY, "100"); // 100 ms
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


    @Test(timeout = 300000)
    public void deadlockFreeProcessingAfterMissedGeneration() throws IOException, InterruptedException {

        Map<RecordId, List<ConsumerAction>> recordHistory = new ConcurrentHashMap<>();

        Set<String> topicList = new HashSet<>();

        String topic = "topic-1" + name.getMethodName();
        topicList.add(topic);

        // Only 1 replica since our testing only has 1 broker
        kafkaAdminClient.createTopic(topic, PARTITIONS, 1, new Properties());

        // Setup consumer threads
        Properties consumerProperties = new Properties();
        consumerProperties.putAll(CONSUMER_PROPERTIES);
        consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "processing-group-" + name.getMethodName());
        ProcessingConfig config = new ProcessingConfig(consumerProperties);

        AtomicBoolean finishedProcessingOutage = new AtomicBoolean(false);
        ConsumerThread outageThread = new ConsumerThread("outage_consumer", config, topicList,
                recordHistory, finishedProcessingOutage, true);

        // Write some data
        Properties producerProperties = new Properties();
        producerProperties.putAll(KafkaTests.getProps());
        producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        Producer<String, String> producer = new KafkaProducer<String, String>(producerProperties);

        int writtenRecords = 0;
        writtenRecords = publishRecords(writtenRecords, MESSAGES_PER_TOPIC, producer, topic, recordHistory);

        // Start outage consumer
        outageThread.start();

        LOGGER.info("Starting wait for outage thread to start sleeping");
        //wait long enough for outage thread to start sleeping
        Thread.sleep(5000L);
        LOGGER.info("Finished wait for outage thread to start sleeping");

        //Start other thread, triggering rebalance which outage thread won't join
        AtomicBoolean finishedProcessing1 = new AtomicBoolean(false);
        ConsumerThread thread1 = new ConsumerThread("consumer-1", config, topicList,
                recordHistory, finishedProcessing1, false);
        thread1.start();

        // Wait for all written records to be committed
        monitorRecordProcessing(writtenRecords, recordHistory);
        verifyHistory(recordHistory, true);
        LOGGER.info("All records committed. Shutting down live thread.");
        finishedProcessing1.set(true);
        cleanupThread(thread1);

        LOGGER.info("Starting wait for outage consumer thread to wake up");
        // wait for long enough for outage thread to rejoin.
        Thread.sleep(15000L);
        LOGGER.info("Done waiting for outage consumer thread to wake up. Publish next batch of records.");

        // write some more records to make sure we have new records in each partition so outage consumer
        //  has some messages returned by poll after it rejoins
        writtenRecords = publishRecords(writtenRecords, MESSAGES_PER_TOPIC, producer, topic, recordHistory);

        // Wait for all written records to be committed
        monitorRecordProcessing(writtenRecords, recordHistory);

        finishedProcessingOutage.set(true);

        for (Map.Entry<RecordId, List<ConsumerAction>> entry : recordHistory.entrySet()) {
            LOGGER.debug("Record history: {} - {}", entry.getKey(), entry.getValue());
        }

        // Verify the history is as we expect (including terminal conditions)
        verifyHistory(recordHistory, true);
        LOGGER.info("All records committed. Shutting down outage thread.");

        // Cleanup threads, if there are any problems just log a warning since the test was otherwise successful
        cleanupThread(outageThread);

        producer.close();
    }

    private int publishRecords(int writtenRecords, int messagesToWrite, Producer<String, String> producer, String topic, Map<RecordId, List<ConsumerAction>> recordHistory) {

        int start = writtenRecords;
        int stop = writtenRecords + messagesToWrite;
        for (int message = start; message < stop; message++) {

            int partition = message % PARTITIONS;
            long offset = message / PARTITIONS;

            String key = "key-" + topic + "-" + partition + "-" + message;
            String value = "value" + message;

            recordHistory.put(new RecordId(new ConsumerRecord<>(topic, partition, offset, key, value)),
                    new CopyOnWriteArrayList<>());

            producer.send(new ProducerRecord<>(topic, partition, key, value));
            writtenRecords++;
        }

        producer.flush();
        return writtenRecords;
    }

    private void monitorRecordProcessing(int writtenRecords, Map<RecordId, List<ConsumerAction>> recordHistory) throws InterruptedException {
        long nextHistoryCheck = System.currentTimeMillis() + HISTORY_CHECK_TIME;
        int committedRecords = recordsWithAction(recordHistory, Action.COMMITTED);
        LOGGER.info("Committed records: {} of {}", committedRecords, writtenRecords);

        while (committedRecords < writtenRecords) {
            Thread.sleep(1000L);

            if (System.currentTimeMillis() >= nextHistoryCheck) {
                // Verify the history is as we expect (excluding terminal conditions)
                verifyHistory(recordHistory, false);

                for (Map.Entry<RecordId, List<ConsumerAction>> entry : recordHistory.entrySet()) {
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
            int ackedRecords = recordsWithAction(recordHistory, Action.ACKED);
            LOGGER.debug("Committed records: {} of {}", committedRecords, writtenRecords);
            LOGGER.debug("Processed records: {} of {}", recordsWithAction(recordHistory, Action.ACKED), writtenRecords);
            if (ackedRecords == writtenRecords) {
                Thread.sleep(2000L);// Sleep 2x commit threshold
                if (committedRecords < writtenRecords) {
                    LOGGER.warn("Deadlock detected. All records are acked and eligible for commit, but have not been committed after processing.kafka.commit.time.threshold exceeded");
                    break;
                }
            }
        }
    }

    private void cleanupThread(ConsumerThread thread) throws InterruptedException {
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

    private int recordsWithAction(Map<RecordId, List<ConsumerAction>> recordHistory, Action action) {
        int records = 0;
        for (Map.Entry<RecordId, List<ConsumerAction>> entry : recordHistory.entrySet()) {
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

    private void verifyHistory(Map<RecordId, List<ConsumerAction>> recordHistory, boolean end) {
        for (Map.Entry<RecordId, List<ConsumerAction>> recordHist : recordHistory.entrySet()) {
            int read = 0;
            int acked = 0;
            int failed = 0;
            int committed = 0;
            // Track unique consumers that acked the record.
            Set<String> ackedBy = new HashSet<>();

            List<ConsumerAction> actionHistory = recordHist.getValue();

            // Verify read/ack/fail/committed history for each record.
            for (ConsumerAction action : actionHistory) {
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

                // Verify record was only committed once.
                assertThat("Record " + recordHist.getKey() + " was not committed exactly once. History = " + actionHistory,
                        committed, is(1));

                // The last action for each record should always be a commit.
                Action lastAction = actionHistory.get(actionHistory.size() - 1).getAction();
                assertThat("Record " + recordHist.getKey() + " final action was not commit. History = " + actionHistory,
                        lastAction, is(Action.COMMITTED));
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

        private final String id;
        private final ProcessingConfig config;
        private final Set<String> topics;
        private final Map<RecordId, List<ConsumerAction>> recordHistory;
        private final AtomicBoolean finishedProcessing;
        private final Map<RecordId, Long> recordsToBeProcessed = new HashMap<>();
        private final Set<RecordId> recordsToBeCommitted = ConcurrentHashMap.newKeySet();
        private final Set<RecordId> recordsCommittedDuringAck = ConcurrentHashMap.newKeySet();
        private final boolean simulateOutage;
        private volatile boolean ackInProgress;
        private int recordsProcessed = 0;
        private boolean alreadyHadMyNap = false;

        public ConsumerThread(String id, ProcessingConfig config, Set<String> topics,
                              Map<RecordId, List<ConsumerAction>> recordHistory, AtomicBoolean finishedProcessing, boolean simulateOutage) {
            this.id = id;
            this.config = config;
            this.topics = topics;
            this.recordHistory = recordHistory;
            this.finishedProcessing = finishedProcessing;
            this.simulateOutage = simulateOutage;
        }

        private void addRecordHistory(RecordId recordId, Action action) {
            recordHistory.get(recordId).add(new ConsumerAction(consumerId, action));
        }

        @Override
        public void run() {
            LOGGER.info("thread id = {}", id);
            while (!finishedProcessing.get()) {
                LOGGER.debug("{} running", id);
                long currentTime = System.currentTimeMillis();

                try {
                    if (consumer == null)
                        initializeConsumer();

                    // Read next record
                    Optional<ConsumerRecord<String, String>> optional = consumer.nextRecord(CONSUMER_POLL_TIMEOUT);
                    if (optional.isPresent()) {
                        RecordId recordId = new RecordId(optional.get());
                        addRecordHistory(recordId, Action.READ);
                        long sleepTime = (long) (random.nextDouble() * MAX_SLEEP);
                        LOGGER.debug("{} reading record {} with sleep time {}", new Object[]{id, recordId, sleepTime});

                        recordsToBeProcessed.put(recordId, currentTime + sleepTime);
                    }

                    // Process any records that are ready
                    for (Map.Entry<RecordId, Long> entry : new HashMap<>(recordsToBeProcessed).entrySet()) {
                        RecordId recordId = entry.getKey();
                        long time = entry.getValue();
                        ConsumerRecord<String, String> unprocessedRecord = recordId.toRecord();

                        if (time <= currentTime) {
                            LOGGER.debug("{} removing record {}", id, recordId);
                            recordsToBeProcessed.remove(recordId);
                            LOGGER.debug("{} ack'ing record {}", id, recordId);
                            recordsToBeCommitted.add(recordId);
                            if (consumer.ack(unprocessedRecord)) {
                                // Mark as acked prior to committed
                                addRecordHistory(recordId, Action.ACKED);
                                for (RecordId committedRecordId : recordsCommittedDuringAck) {
                                    addRecordHistory(committedRecordId, Action.COMMITTED);
                                }
                                recordsCommittedDuringAck.clear();
                            } else {
                                recordsToBeCommitted.remove(recordId);
                            }
                        } else {
                            LOGGER.debug("{} not processing record yet {} waiting {}ms", new Object[]{id, recordId,
                                    (time - currentTime)});
                        }

                        if (simulateOutage && recordsProcessed > 5 && !alreadyHadMyNap) {
                            simulateOutage();
                            LOGGER.info("{} thread going to sleep.", id);
                            Thread.sleep(10000L);
                            alreadyHadMyNap = true;
                            LOGGER.info("{} thread waking up.", id);
                            recordsToBeProcessed.clear();
                            recordsToBeCommitted.clear();
                            break;
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

        /**
         * Simulate failure of messages (e.g. commit failed exception due to rebalance / hbase compaction).
         * This should reset the internal state of the ProcessingKafkaConsumer and ProcessingPartitions
         * to the lastCommittedOffset, and seek the kafka consumer to lastCommittedOffset.
         */
        private void simulateOutage() {
            LOGGER.info("Simulating outage impacting consumer {}", id);
            for (Map.Entry<RecordId, Long> entry : new HashMap<>(recordsToBeProcessed).entrySet()) {
                RecordId recordId = entry.getKey();
                ConsumerRecord<String, String> unprocessedRecord = recordId.toRecord();

                LOGGER.debug("{} failing record {}", id, recordId);
                if (consumer.fail(unprocessedRecord)) {
                    addRecordHistory(recordId, Action.FAILED);
                }
            }
        }

        private void initializeConsumer() {
            consumer = new CommitTrackingProcessingKafkaConsumer<>(config);
            consumerId = consumer.toString(); // generate a unique id
            consumer.subscribe(topics);
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
                        recordsProcessed++;
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
                    /*
                     * Clear records which are planned to be recorded as committed by this thread
                     * when a later offset is committed, because another thread might have committed
                     * these offsets in an earlier group generation.
                     */
                    recordsToBeCommitted.clear();
                }
            }
        }
    }
}