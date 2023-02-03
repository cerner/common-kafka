package com.cerner.common.kafka.producer;

import com.cerner.common.kafka.KafkaExecutionException;
import com.cerner.common.kafka.KafkaTests;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

import static com.cerner.common.kafka.testing.AbstractKafkaTests.getProps;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Stephen Durfey
 */
@ExtendWith(MockitoExtension.class)
public class KafkaProducerWrapperTest {

    private static AdminClient kafkaAdminClient;

    private static Properties customProps;

    private String testName;

    @Mock
    private KafkaProducer<String, String> mockedProducer;

    @Mock
    private Future<RecordMetadata> mockedFuture;

    private String topicName;

    @BeforeAll
    public static void startup() throws Exception {
        customProps = new Properties();
        customProps.setProperty("message.max.bytes", "1000");
        customProps.setProperty("group.max.session.timeout.ms", "2000");
        customProps.setProperty("group.min.session.timeout.ms", "500");

        // start Kafka with custom properties.
        KafkaTests.startKafka(customProps);

        kafkaAdminClient = AdminClient.create(KafkaTests.getProps());
    }

    @AfterAll
    public static void shutdown() throws Exception {
        kafkaAdminClient.close();

        // Restart Kafka with default properties.
        KafkaTests.stopKafka();
        KafkaTests.startKafka();
    }

    @BeforeEach
    public void setup(TestInfo testInfo){
        testName = testInfo.getDisplayName().replaceAll("[^a-zA-Z0-9]", "-").trim();
        topicName = "topic_" + testName;
    }

    @Test
    public void test_messageSentSynchronouslySuccessfully() throws IOException {
        long previousSendCount = KafkaProducerWrapper.SEND_TIMER.count();
        long previousSyncSendCount = KafkaProducerWrapper.SYNC_SEND_TIMER.count();
        long previousFlushCount = KafkaProducerWrapper.FLUSH_TIMER.count();
        long previousBatchSizeCount = KafkaProducerWrapper.BATCH_SIZE_HISTOGRAM.count();
        double previousBatchSizeSum = KafkaProducerWrapper.BATCH_SIZE_HISTOGRAM.sum();

        Set<NewTopic> topics = new HashSet<>();
        topics.add(new NewTopic(topicName, 4, (short) 1));
        kafkaAdminClient.createTopics(topics);

        Properties props = KafkaTests.getProps();
        props.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "10000");
        props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "60000");

        KafkaProducerWrapper<String, String> producer = new KafkaProducerWrapper<>(new KafkaProducer<>(props));

        producer.sendSynchronously(
                new ProducerRecord<>(topicName, "key"+testName, "value"+ UUID.randomUUID()));
        producer.close();

        assertThat(KafkaProducerWrapper.SEND_TIMER.count(), is(previousSendCount));
        assertThat(KafkaProducerWrapper.SYNC_SEND_TIMER.count(), is(previousSyncSendCount + 1));
        assertThat(KafkaProducerWrapper.FLUSH_TIMER.count(), is(previousFlushCount));
        assertThat(KafkaProducerWrapper.BATCH_SIZE_HISTOGRAM.count(), is(previousBatchSizeCount + 1));
        assertThat(KafkaProducerWrapper.BATCH_SIZE_HISTOGRAM.sum(), is(previousBatchSizeSum + 1));
    }

    @Test
    public void test_multipleMessagesSentSynchronouslySuccessfully() throws IOException {
        long previousSendCount = KafkaProducerWrapper.SEND_TIMER.count();
        long previousSyncSendCount = KafkaProducerWrapper.SYNC_SEND_TIMER.count();
        long previousFlushCount = KafkaProducerWrapper.FLUSH_TIMER.count();
        long previousBatchSizeCount = KafkaProducerWrapper.BATCH_SIZE_HISTOGRAM.count();
        double previousBatchSizeSum = KafkaProducerWrapper.BATCH_SIZE_HISTOGRAM.sum();

        Set<NewTopic> topics = new HashSet<>();
        topics.add(new NewTopic(topicName, 4, (short) 1));
        kafkaAdminClient.createTopics(topics);

        Properties props = KafkaTests.getProps();
        props.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "10000");
        props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "60000");

        int batchSize = 10;
        KafkaProducerWrapper<String, String> producer = new KafkaProducerWrapper<>(new KafkaProducer<>(props));

        List<ProducerRecord<String, String>> records = new ArrayList<>();

        IntStream.range(0, batchSize).forEach(i ->
            records.add(new ProducerRecord<>(topicName, "key"+testName+i, "value"+i)));

        producer.sendSynchronously(records);
        producer.close();

        assertThat(KafkaProducerWrapper.SEND_TIMER.count(), is(previousSendCount));
        assertThat(KafkaProducerWrapper.SYNC_SEND_TIMER.count(), is(previousSyncSendCount + 1));
        assertThat(KafkaProducerWrapper.FLUSH_TIMER.count(), is(previousFlushCount));
        assertThat(KafkaProducerWrapper.BATCH_SIZE_HISTOGRAM.count(), is(previousBatchSizeCount + 1));
        assertThat(KafkaProducerWrapper.BATCH_SIZE_HISTOGRAM.sum(), is(previousBatchSizeSum + batchSize));
    }

    @Test
    public void test_flushRetriable() throws IOException {
        doThrow(new TimeoutException("boom")).when(mockedProducer).flush();

        KafkaProducerWrapper<String, String> producer = new KafkaProducerWrapper<>(mockedProducer);

        producer.send(new ProducerRecord<>(topicName, "key"+testName,
                "value"+UUID.randomUUID()));
        assertThrows(IOException.class, producer::flush,
                "Expected producer.flush() to throw IO exception, but it wasn't thrown.");
    }

    @Test
    public void test_flushNonRetriable() throws IOException {
        doThrow(new RuntimeException("boom")).when(mockedProducer).flush();

        KafkaProducerWrapper<String, String> producer = new KafkaProducerWrapper<>(mockedProducer);

        producer.send(new ProducerRecord<>(topicName, "key"+testName,
                "value"+UUID.randomUUID()));
        assertThrows(IOException.class, producer::flush,
                "Expected producer.flush() to throw IO exception, but it wasn't thrown.");
    }

    @Test
    public void test_flushFutureExecutionException() throws IOException, ExecutionException, InterruptedException {
        when(mockedProducer.send(ArgumentMatchers.any())).thenReturn(mockedFuture);
        when(mockedFuture.get()).thenThrow(new ExecutionException("boom", new IllegalStateException()));

        KafkaProducerWrapper<String, String> producer = new KafkaProducerWrapper<>(mockedProducer);

        producer.send(new ProducerRecord<>(topicName, "key"+testName,
                "value"+UUID.randomUUID()));
        assertThrows(IOException.class, producer::flush,
                "Expected producer.flush() to throw IO exception, but it wasn't thrown.");
    }

    @Test
    public void test_messageSentSuccessfully() throws IOException {
        long previousSendCount = KafkaProducerWrapper.SEND_TIMER.count();
        long previousSyncSendCount = KafkaProducerWrapper.SYNC_SEND_TIMER.count();
        long previousFlushCount = KafkaProducerWrapper.FLUSH_TIMER.count();
        long previousBatchSizeCount = KafkaProducerWrapper.BATCH_SIZE_HISTOGRAM.count();
        double previousBatchSizeSum = KafkaProducerWrapper.BATCH_SIZE_HISTOGRAM.sum();

        Set<NewTopic> topics = new HashSet<>();
        topics.add(new NewTopic(topicName, 4, (short) 1));
        kafkaAdminClient.createTopics(topics);

        Properties props = KafkaTests.getProps();
        props.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "10000");
        props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "60000");

        KafkaProducerWrapper<String, String> producer = new KafkaProducerWrapper<>(new KafkaProducer<>(props));

        producer.send(new ProducerRecord<>(topicName, "key"+testName, "value"+UUID.randomUUID()));
        producer.flush();
        producer.close();

        assertThat(KafkaProducerWrapper.SEND_TIMER.count(), is(previousSendCount + 1));
        assertThat(KafkaProducerWrapper.SYNC_SEND_TIMER.count(), is(previousSyncSendCount));
        assertThat(KafkaProducerWrapper.FLUSH_TIMER.count(), is(previousFlushCount + 1));
        assertThat(KafkaProducerWrapper.BATCH_SIZE_HISTOGRAM.count(), is(previousBatchSizeCount + 1));
        assertThat(KafkaProducerWrapper.BATCH_SIZE_HISTOGRAM.sum(), is(previousBatchSizeSum + 1));
    }

    @Test
    public void test_messageSentMultipleSuccessfully() throws IOException, ExecutionException, InterruptedException {
        long previousSendCount = KafkaProducerWrapper.SEND_TIMER.count();
        long previousSyncSendCount = KafkaProducerWrapper.SYNC_SEND_TIMER.count();
        long previousFlushCount = KafkaProducerWrapper.FLUSH_TIMER.count();
        long previousBatchSizeCount = KafkaProducerWrapper.BATCH_SIZE_HISTOGRAM.count();
        double previousBatchSizeSum = KafkaProducerWrapper.BATCH_SIZE_HISTOGRAM.sum();

        when(mockedProducer.send(ArgumentMatchers.any())).thenReturn(mockedFuture);
        int batchSize = 10;
        KafkaProducerWrapper<String, String> producer = new KafkaProducerWrapper<>(mockedProducer);

        IntStream.range(0, batchSize).forEach(i ->
                producer.send(new ProducerRecord<>(topicName, "key"+testName+i, "value"+i)));
        producer.flush();

        verify(mockedFuture, times(batchSize)).get();

        producer.close();

        assertThat(KafkaProducerWrapper.SEND_TIMER.count(), is(previousSendCount + batchSize));
        assertThat(KafkaProducerWrapper.SYNC_SEND_TIMER.count(), is(previousSyncSendCount));
        assertThat(KafkaProducerWrapper.FLUSH_TIMER.count(), is(previousFlushCount + 1));
        assertThat(KafkaProducerWrapper.BATCH_SIZE_HISTOGRAM.count(), is(previousBatchSizeCount + 1));
        assertThat(KafkaProducerWrapper.BATCH_SIZE_HISTOGRAM.sum(), is(previousBatchSizeSum + batchSize));
    }

    @Test
    public void test_sendNullRecord() throws IOException, ExecutionException, InterruptedException {
        KafkaProducerWrapper<String, String> producer = new KafkaProducerWrapper<>(mockedProducer);
        assertThrows(IllegalArgumentException.class, () -> producer.send(null),
                "Expected producer.send() to throw Illegal Argument exception, but it wasn't thrown.");
    }

    @Test
    public void test_WrapperNotCloseProducer() throws IOException, ExecutionException, InterruptedException {
        KafkaProducerWrapper<String, String> producer = new KafkaProducerWrapper<>(mockedProducer);

        producer.close();

        verify(mockedProducer, never()).close();
    }

    @Test
    public void testSynchronous_messageTooLarge() throws IOException {
        Set<NewTopic> topics = new HashSet<>();
        topics.add(new NewTopic(topicName, 4, (short) 1));
        kafkaAdminClient.createTopics(topics);

        Properties props = KafkaTests.getProps();
        props.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "10000");
        props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "60000");

        // create a payload that is too large
        StringBuilder bldr = new StringBuilder();
        for(int i = 0; i < 100000; i++) {
            bldr.append(i);
        }

        List<ProducerRecord<String, String>> records = new ArrayList<>(2);
        records.add(new ProducerRecord<>(topicName, "key", bldr.toString()));
        records.add(new ProducerRecord<>(topicName, "key2", "small"));
        boolean caughtRecordTooLargeException = false;
        try (KafkaProducerWrapper<String, String> producer =
                     new KafkaProducerWrapper<>(new KafkaProducer<>(props))) {
            producer.sendSynchronously(records);
        } catch (KafkaExecutionException e) {
            Throwable cause = e.getCause();

            assertThat(cause, instanceOf(ExecutionException.class));

            Throwable cause2 = cause.getCause();

            assertThat(cause2, instanceOf(RecordTooLargeException.class));

            caughtRecordTooLargeException = true;
        }

        assertThat(caughtRecordTooLargeException, is(true));
    }

    @Test
    public void test_messageSentSuccessfullyEvenWithFailure() throws IOException {

        Set<NewTopic> topics = new HashSet<>();
        topics.add(new NewTopic(topicName, 4, (short) 1));
        kafkaAdminClient.createTopics(topics);

        Properties props = getProps();
        props.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "10000");
        props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "60000");

        KafkaProducerWrapper<String, String> producer = new KafkaProducerWrapper<>(new KafkaProducer<>(props));

        producer.send(new ProducerRecord<>(topicName, "key1", "value1"));
        producer.flush();

        // Just stopping Kafka brokers not the Zookeepers (as every time Zookeepers are stopped and brought up they do not have
        // same config so for this test we need same config)
        KafkaTests.stopOnlyKafkaBrokers();

        // to simulate after some transient error Kafka brokers come back up and resumes
        Thread kafkaThread = new StartKafkaThread();

        kafkaThread.start();

        try {
            producer.send(new ProducerRecord<>(topicName, "key2", "value2"));
            producer.flush();
            producer.close();

        } finally {
            try {
                kafkaThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    // Separate thread class just to start Kafka cluster independent of test thread
    public class StartKafkaThread extends Thread {

        public void run() {
            try {
                try {
                    // to have some delay between stopping and starting of the brokers
                    Thread.sleep(3000L);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                // Just start the Kafka brokers, Zookeepers should already be running
                KafkaTests.startOnlyKafkaBrokers();

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

}