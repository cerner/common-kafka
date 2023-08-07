package com.cerner.common.kafka.producer;

import com.cerner.common.kafka.consumer.ConsumerOffsetClient;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.cerner.common.kafka.KafkaExecutionException;
import com.cerner.common.kafka.KafkaTests;
import com.cerner.common.kafka.testing.KafkaTestUtils;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static com.cerner.common.kafka.producer.KafkaProducerPool.DEFAULT_KAFKA_PRODUCER_CONCURRENCY_INT;
import static com.cerner.common.kafka.producer.KafkaProducerPool.KAFKA_PRODUCER_CONCURRENCY;
import static org.apache.kafka.clients.producer.ProducerConfig.LINGER_MS_CONFIG;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;

/**
 * @author Stephen Durfey
 */
public class KafkaProducerPoolTest {

    private KafkaProducerPool<String, String> pool;
    private static AdminClient kafkaAdminClient;

    private static final String FORCE_PRODUCER_ROTATION_OVERFLOW = "producer.rotation.overflow";

    @BeforeAll
    public static void startup() throws Exception {
        KafkaTests.startTest();
        kafkaAdminClient = AdminClient.create(KafkaTests.getProps());
    }

    @AfterAll
    public static void shutdown() throws Exception {
        kafkaAdminClient.close();
        KafkaTests.endTest();
    }

    @BeforeEach
    public void initializePool() {
        pool = new KafkaProducerPool<>();
    }

    @AfterEach
    public void closePool() throws IOException {
        pool.close();
    }

    @Test
    public void nullProperties() {
        assertThrows(IllegalArgumentException.class, () -> pool.getProducer(null),
                "Expected pool.getProdcuer to throw illegal argument exception, but it wasn't thrown.");
    }

    @Test
    public void sameConfiguration() {
        Properties props = KafkaTests.getProps();
        props.setProperty(KAFKA_PRODUCER_CONCURRENCY, "1");
        props.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        Producer<String, String> p1 = pool.getProducer(props);
        Producer<String, String> p2 = pool.getProducer(props);
        assertThat(p1, is(p2));
    }

    @Test
    public void differentConfiguration() {
        Properties props1 = KafkaTests.getProps();
        props1.setProperty(KAFKA_PRODUCER_CONCURRENCY, "1");
        props1.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props1.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        Properties props2 = new Properties();
        props2.putAll(props1);
        props2.setProperty("some.property", "property.value");

        Producer<String, String> p1 = pool.getProducer(props1);
        Producer<String, String> p2 = pool.getProducer(props2);
        assertThat(p1, is(not(p2)));
    }

    @Test
    public void sameConfigurationDefaultConcurrency() throws IOException {
        Properties props = KafkaTests.getProps();
        props.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        assertPoolConcurrency(props, DEFAULT_KAFKA_PRODUCER_CONCURRENCY_INT);
    }

    @Test
    public void sameConfigurationCustomConcurrency() throws IOException {
        int producerConcurrency = 25;
        Properties props = KafkaTests.getProps();
        props.setProperty(KAFKA_PRODUCER_CONCURRENCY, String.valueOf(producerConcurrency));
        props.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        assertPoolConcurrency(props, producerConcurrency);
    }

    @Test
    public void concurrencyConfigIsString() throws IOException {
        Properties props = KafkaTests.getProps();
        props.setProperty(KAFKA_PRODUCER_CONCURRENCY, "notANumber");
        props.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        assertPoolConcurrency(props, DEFAULT_KAFKA_PRODUCER_CONCURRENCY_INT);
    }

    @Test
    public void concurrencyConfigIsZero() throws IOException {
        Properties props = KafkaTests.getProps();
        props.setProperty(KAFKA_PRODUCER_CONCURRENCY, String.valueOf(0));
        props.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        assertPoolConcurrency(props, DEFAULT_KAFKA_PRODUCER_CONCURRENCY_INT);
    }

    @Test
    public void concurrencyConfigIsNegative() throws IOException {
        Properties props = KafkaTests.getProps();
        props.setProperty(KAFKA_PRODUCER_CONCURRENCY, String.valueOf(-12));
        props.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        assertPoolConcurrency(props, DEFAULT_KAFKA_PRODUCER_CONCURRENCY_INT);
    }

    @Test
    public void defaultUsed() throws IOException {
        Properties props = KafkaTests.getProps();

        MockProducerPool mockPool = new MockProducerPool();
        mockPool.getProducer(props);

        assertThat(mockPool.getProducerProperties().getProperty(ACKS_CONFIG), is(String.valueOf(-1)));
    }

    @Test
    public void defaultOverridden() throws IOException {
        Properties props = KafkaTests.getProps();
        props.setProperty(ACKS_CONFIG, String.valueOf(1));

        MockProducerPool mockPool = new MockProducerPool();
        mockPool.getProducer(props);

        assertThat(mockPool.getProducerProperties().getProperty(ACKS_CONFIG), is(String.valueOf(1)));
    }

    @Test
    public void closeEmptyPool() throws IOException {
        pool.close();
    }

    @Test
    public void closeClosesProducers() throws IOException, ExecutionException, InterruptedException {
        KafkaProducerPool<Object, Object> mockPool = new MockProducerPool();
        Properties props = KafkaTests.getProps();
        props.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        Producer<Object, Object> producer = mockPool.getProducer(props);
        mockPool.close();
        verify(producer).close();
    }

    @Test
    public void multipleCloses() throws IOException {
        KafkaProducerPool<Object, Object> mockPool = new MockProducerPool();
        Properties props = KafkaTests.getProps();
        props.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        List<Producer<?, ?>> producers = new LinkedList<>();
        for (int i = 0; i < 10; ++i) {
            producers.add(mockPool.getProducer(props));
        }
        mockPool.close();
        mockPool.close();
        producers.forEach(producer -> verify(producer, times(1)).close());
    }

    @Test
    @Timeout(10)
    public void messageProductionWithProducerConfig() throws InterruptedException, KafkaExecutionException, ExecutionException {
        Properties props = KafkaTests.getProps();
        props.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(LINGER_MS_CONFIG, String.valueOf(100));

        messageProduction(props);
    }

    @Test
    @Timeout(10)
    public void messageProductionWithProperties() throws InterruptedException, KafkaExecutionException, ExecutionException {
        Properties props = KafkaTests.getProps();
        props.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(LINGER_MS_CONFIG, String.valueOf(100));

        messageProduction(props);
    }

    @Test
    public void producerRotationOverflow() throws IOException {
        KafkaProducerPool<Object, Object> mockPool = new MockProducerPool();
        Properties props = KafkaTests.getProps();
        props.setProperty(FORCE_PRODUCER_ROTATION_OVERFLOW, String.valueOf(true));

        mockPool.getProducer(props);
        mockPool.close();
    }

    private void messageProduction(Properties config) throws InterruptedException, ExecutionException {
        String topicName = "topic_" + UUID.randomUUID().toString();
        NewTopic topic = new NewTopic(topicName, 4, (short) 1);
        Set<NewTopic> topics = new HashSet<>();
        topics.add(topic);

        kafkaAdminClient.createTopics(topics).all().get();

        Producer<String, String> producer = pool.getProducer(config);

        long messages = 10;
        for (long i = 0; i < messages; ++i) {
            producer.send(new ProducerRecord<>(topicName, String.valueOf(i), UUID.randomUUID().toString())).get();
        }

        // We loop here since the producer doesn't necessarily write to ZK immediately after receiving a write
        ConsumerOffsetClient consumerOffsetClient = new ConsumerOffsetClient(config);
        while(KafkaTestUtils.getTopicAndPartitionOffsetSum(topicName, consumerOffsetClient.getEndOffsets(Arrays.asList(topicName))) != messages)
            Thread.sleep(100);
    }

    @Test
    public void concurrencyTest() throws Exception {
        // Run 10 threads in parallel for approximately 3 seconds.
        int threads = 10;
        long stopTime = System.currentTimeMillis() + 3000;

        int producerConcurrency = 25;
        final Properties props = KafkaTests.getProps();
        props.setProperty(KAFKA_PRODUCER_CONCURRENCY, String.valueOf(producerConcurrency));
        props.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        final Set<Producer<String, String>> producers = Collections.synchronizedSet(new HashSet<>());

        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CompletionService<Void> service = new ExecutorCompletionService<Void>(executor);
        Map<Future<Void>, Callable<Void>> futures = new IdentityHashMap<Future<Void>, Callable<Void>>(threads);

        while (System.currentTimeMillis() < stopTime) {
            final Future<Void> future = service.submit(() -> {
                producers.add(pool.getProducer(props));
                return null;
            });

            // Keep track of submitted jobs.
            futures.put(future, () -> {
                Void nothing = future.get();
                return nothing;
            });

            // Collect any completed jobs.
            Future<Void> done;
            while ((done = service.poll()) != null) {
                futures.remove(done).call();
            }

            // Respect the concurrency limit.
            while (futures.size() >= threads) {
                done = service.take();
                futures.remove(done).call();
            }
        }

        // Allow pending jobs to complete.
        while (futures.size() > 0) {
            Future<Void> done = service.take();
            futures.remove(done).call();
        }

        executor.shutdown();
        assertThat(producers.size(), is(producerConcurrency));
    }

    private void assertPoolConcurrency(Properties props, int concurrency) {
        Set<Producer<String, String>> producers = new HashSet<>();
        for (int i = 0; i < concurrency * 10; ++i) {
            producers.add(pool.getProducer(props));
        }
        assertThat(producers.size(), is(concurrency));
    }

    @SuppressWarnings("unchecked")
    private class MockProducerPool extends KafkaProducerPool<Object, Object> {
        private Properties producerProperties;

        @Override
        Producer<Object, Object> createProducer(Properties properties) {
            producerProperties = properties;
            return mock(Producer.class);
        }

        @Override
        public int getProducerRotation() {
            if (producerProperties.getProperty(FORCE_PRODUCER_ROTATION_OVERFLOW) != null) {
                return Integer.MAX_VALUE + 2;
            } else {
                return super.getProducerRotation();
            }
        }

        public Properties getProducerProperties() {
            assertNotNull(producerProperties);
            return producerProperties;
        }
    }

}
