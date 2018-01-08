package com.cerner.common.kafka.producer;

import com.cerner.common.kafka.KafkaExecutionException;
import com.cerner.common.kafka.KafkaTests;
import com.cerner.common.kafka.admin.KafkaAdminClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

import static com.cerner.common.kafka.testing.AbstractKafkaTests.getProps;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Stephen Durfey
 */
@RunWith(MockitoJUnitRunner.class)
public class KafkaProducerWrapperTest {

    private static KafkaAdminClient kafkaAdminClient;

    private static Properties customProps;

    @Rule
    public TestName testName = new TestName();

    @Mock
    private KafkaProducer<String, String> mockedProducer;

    @Mock
    private Future<RecordMetadata> mockedFuture;

    private String topic;

    @BeforeClass
    public static void startup() throws Exception {
        customProps = new Properties();
        customProps.setProperty("message.max.bytes", "1000");
        customProps.setProperty("group.max.session.timeout.ms", "2000");
        customProps.setProperty("group.min.session.timeout.ms", "500");

        // Restart Kafka with custom properties.
        KafkaTests.stopKafka();
        KafkaTests.startKafka(customProps);

        kafkaAdminClient = new KafkaAdminClient(KafkaTests.getProps());
    }

    @AfterClass
    public static void shutdown() throws Exception {
        kafkaAdminClient.close();

        // Restart Kafka with default properties.
        KafkaTests.stopKafka();
        KafkaTests.startKafka();
    }

    @Before
    public void setup(){
        topic = "topic_" + testName.getMethodName();

        when(mockedProducer.send(Matchers.anyObject())).thenReturn(mockedFuture);
    }

    @Test
    public void test_messageSentSynchronouslySuccessfully() throws IOException {

        kafkaAdminClient.createTopic(topic, 4, 1, new Properties());

        Properties props = KafkaTests.getProps();
        props.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "10000");
        props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "600000");

        KafkaProducerWrapper<String, String> producer = new KafkaProducerWrapper<>(new KafkaProducer<>(props));

        producer.sendSynchronously(
                new ProducerRecord<>(topic, "key"+testName.getMethodName(), "value"+ UUID.randomUUID()));
        producer.close();
    }


    @Test(expected=IOException.class)
    public void test_flushRetriable() throws IOException {
        doThrow(new TimeoutException("boom")).when(mockedProducer).flush();

        KafkaProducerWrapper<String, String> producer = new KafkaProducerWrapper<>(mockedProducer);


        producer.send(new ProducerRecord<>(topic, "key"+testName.getMethodName(),
                "value"+UUID.randomUUID()));
        producer.flush();
    }

    @Test(expected=IOException.class)
    public void test_flushNonRetriable() throws IOException {
        doThrow(new RuntimeException("boom")).when(mockedProducer).flush();

        KafkaProducerWrapper<String, String> producer = new KafkaProducerWrapper<>(mockedProducer);

        producer.send(new ProducerRecord<>(topic, "key"+testName.getMethodName(),
                "value"+UUID.randomUUID()));
        producer.flush();
    }

    @Test(expected=IOException.class)
    public void test_flushFutureExecutionException() throws IOException, ExecutionException, InterruptedException {
        when(mockedFuture.get()).thenThrow(new ExecutionException("boom", new IllegalStateException()));

        KafkaProducerWrapper<String, String> producer = new KafkaProducerWrapper<>(mockedProducer);

        producer.send(new ProducerRecord<>(topic, "key"+testName.getMethodName(),
                "value"+UUID.randomUUID()));
        producer.flush();
    }

    @Test
    public void test_messageSentSuccessfully() throws IOException {
        kafkaAdminClient.createTopic(topic, 4, 1, new Properties());

        Properties props = KafkaTests.getProps();
        props.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "10000");
        props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "600000");

        KafkaProducerWrapper<String, String> producer = new KafkaProducerWrapper<>(new KafkaProducer<>(props));

        producer.send(new ProducerRecord<>(topic, "key"+testName.getMethodName(), "value"+UUID.randomUUID()));
        producer.flush();
        producer.close();
    }

    @Test
    public void test_messageSentMultipleSuccessfully() throws IOException, ExecutionException, InterruptedException {
        KafkaProducerWrapper<String, String> producer = new KafkaProducerWrapper<>(mockedProducer);

        IntStream.range(0, 10).forEach(i ->
                producer.send(new ProducerRecord<>(topic, "key"+testName.getMethodName()+i, "value"+i)));
        producer.flush();

        verify(mockedFuture, times(10)).get();

        producer.close();
    }

    @Test(expected=IllegalArgumentException.class)
    public void test_sendNullRecord() throws IOException, ExecutionException, InterruptedException {
        KafkaProducerWrapper<String, String> producer = new KafkaProducerWrapper<>(mockedProducer);
        producer.send(null);
    }

    @Test
    public void test_WrapperNotCloseProducer() throws IOException, ExecutionException, InterruptedException {
        KafkaProducerWrapper<String, String> producer = new KafkaProducerWrapper<>(mockedProducer);

        producer.close();

        verify(mockedProducer, never()).close();
    }

    @Test
    public void testSynchronous_messageTooLarge() throws IOException {
        kafkaAdminClient.createTopic(topic, 4, 1, new Properties());

        Properties props = KafkaTests.getProps();
        props.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "10000");
        props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "600000");

        // create a payload that is too large
        StringBuilder bldr = new StringBuilder();
        for(int i = 0; i < 100000; i++) {
            bldr.append(i);
        }

        List<ProducerRecord<String, String>> records = new ArrayList<>(2);
        records.add(new ProducerRecord<>(topic, "key", bldr.toString()));
        records.add(new ProducerRecord<>(topic, "key2", "small"));
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

        kafkaAdminClient.createTopic(topic, 4, 1, new Properties());

        Properties props = getProps();
        props.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "10000");
        props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "600000");

        KafkaProducerWrapper<String, String> producer = new KafkaProducerWrapper<>(new KafkaProducer<>(props));

        producer.send(new ProducerRecord<>(topic, "key1", "value1"));
        producer.flush();

        // Just stopping Kafka brokers not the Zookeepers (as every time Zookeepers are stopped and brought up they do not have
        // same config so for this test we need same config)
        KafkaTests.stopOnlyKafkaBrokers();

        // to simulate after some transient error Kafka brokers come back up and resumes
        Thread kafkaThread = new StartKafkaThread();

        kafkaThread.start();

        try {
            producer.send(new ProducerRecord<>(topic, "key2", "value2"));
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