package com.cerner.common.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.record.CompressionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BATCH_SIZE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.COMPRESSION_TYPE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.LINGER_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.MAX_REQUEST_SIZE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.RETRIES_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.RETRY_BACKOFF_MS_CONFIG;

/**
 * Manages a pool of Kafka {@link Producer} instances.
 *
 * <p>
 * This class is thread safe.
 * </p>
 *
 * @param <K>
 *            Producer key type
 * @param <V>
 *            Producer message type
 *
 * @author A. Olson
 */
public class KafkaProducerPool<K, V> implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProducerPool.class);

    /**
     * The set of default producer properties
     */
    private static final Properties DEFAULT_PRODUCER_PROPERTIES;

    static {
        // Setup default producer properties.
        DEFAULT_PRODUCER_PROPERTIES = new Properties();
        DEFAULT_PRODUCER_PROPERTIES.setProperty(COMPRESSION_TYPE_CONFIG, CompressionType.LZ4.name);

        // Require acknowledgment from all replicas
        DEFAULT_PRODUCER_PROPERTIES.setProperty(ACKS_CONFIG, String.valueOf(-1));

        // Slightly more conservative retry policy (Kafka default is Integer.MAX_VALUE retries, 100 milliseconds apart,
        // up to the delivery.timeout.ms value).
        DEFAULT_PRODUCER_PROPERTIES.setProperty(RETRY_BACKOFF_MS_CONFIG, String.valueOf(1000));
        DEFAULT_PRODUCER_PROPERTIES.setProperty(RETRIES_CONFIG, String.valueOf(5));

        // For better performance increase batch size default to 10MB and linger time to 50 milliseconds
        DEFAULT_PRODUCER_PROPERTIES.setProperty(BATCH_SIZE_CONFIG, String.valueOf(10 * 1024 * 1024));
        DEFAULT_PRODUCER_PROPERTIES.setProperty(LINGER_MS_CONFIG, String.valueOf(50));

        // For better performance, increase the request size default to 100MB. The max message size will be the
        // minimum among this and the message.max.bytes set on the brokers.
        DEFAULT_PRODUCER_PROPERTIES.setProperty(MAX_REQUEST_SIZE_CONFIG, String.valueOf(100 * 1024 * 1024));
    }

    /**
     * KafkaProducerPool concurrency setting.
     */
    public static final String KAFKA_PRODUCER_CONCURRENCY = "kafka.pool.concurrency";

    /**
     * The default value for {@link #KAFKA_PRODUCER_CONCURRENCY}.
     */
    public static final int DEFAULT_KAFKA_PRODUCER_CONCURRENCY_INT = 4;

    /**
     * The default value for {@link #KAFKA_PRODUCER_CONCURRENCY}.
     */
    public static final String DEFAULT_KAFKA_PRODUCER_CONCURRENCY = String.valueOf(DEFAULT_KAFKA_PRODUCER_CONCURRENCY_INT);

    /**
     * Pool of producers. A separate group of producers is maintained for each unique configuration.
     */
    private final Map<Properties, List<Producer<K, V>>> pool;

    /**
     * Read/write lock for the pool's thread safety.
     */
    private final ReadWriteLock lock;

    /**
     * Read lock for the pool.
     */
    private final Lock readLock;

    /**
     * Write lock for the pool.
     */
    private final Lock writeLock;

    /**
     * Round-robin retrieval counter. For the sake of simplicity, a single rotator value is used although there may actually be
     * multiple producer arrays managed by the pool.
     */
    private final AtomicInteger producerRotation;

    /**
     * Pool shutdown indicator.
     */
    private boolean shutdown;

    /**
     * Creates a new producer pool.
     */
    public KafkaProducerPool() {
        this.pool = new HashMap<>();
        this.lock = new ReentrantReadWriteLock(true);
        this.readLock = lock.readLock();
        this.writeLock = lock.writeLock();
        this.producerRotation = new AtomicInteger(0);
        this.shutdown = false;
    }

    /**
     * Retrieves a {@link Producer} instance corresponding to the given {@link Properties}.
     *
     * <p>
     * The following properties are set by default, and can be overridden by setting a different value in the passed {@code
     * properties}:
     *
     * <pre>
     *       key          |            value
     * ================================================================
     * compression.type   | lz4
     * acks               | -1
     * retry.backoff.ms   | 1000 ms
     * retries            | 5
     * batch.size         | 10485760 bytes
     * linger.ms          | 50 ms
     * max.request.size   | 104857600 bytes
     * </pre>
     *
     * <p>
     * The {@link #KAFKA_PRODUCER_CONCURRENCY} property can be set to control the number of producers created
     * for each unique configuration.
     *
     * <p>
     * <b>NOTE:</b> the returned producer must not be {@link Producer#close() closed}. Use the {@link #close()} method instead to
     * close all producers in the pool.
     *
     * @param properties
     *            the properties for the producer
     * @return a Kafka message producer
     * @throws org.apache.kafka.common.KafkaException
     *             if an error occurs creating the producer
     * @throws IllegalArgumentException
     *             if properties is {@code null}
     * @throws IllegalStateException
     *             if the pool has already been {@link #close() closed}
     * @throws NumberFormatException
     *             if the value of the {@link #KAFKA_PRODUCER_CONCURRENCY} property cannot be parsed as an
     *             integer.
     */
    public Producer<K, V> getProducer(Properties properties) {
        if (properties == null) {
            throw new IllegalArgumentException("properties cannot be null");
        }

        // Include all default producer properties first, then add in the passed properties
        Properties producerProperties = new Properties();
        producerProperties.putAll(DEFAULT_PRODUCER_PROPERTIES);
        producerProperties.putAll(properties);

        List<Producer<K, V>> producers = null;
        readLock.lock();
        try {
            if (shutdown) {
                throw new IllegalStateException("pool has already been shutdown");
            }
            producers = pool.get(producerProperties);
        } finally {
            readLock.unlock();
        }

        if (producers == null) {
            writeLock.lock();
            try {
                // Check shutdown status again, in case someone else has already shutdown the pool.
                if (shutdown) {
                    throw new IllegalStateException("pool has already been shutdown");
                }

                // Check for existence again, in case someone else beat us here.
                if (pool.containsKey(producerProperties)) {
                    producers = pool.get(producerProperties);
                } else {
                    int producerConcurrency = getProducerConcurrency(producerProperties);

                    // Create a new group of producers.
                    producers = new ArrayList<>(producerConcurrency);
                    for (int i = 0; i < producerConcurrency; ++i) {
                        producers.add(createProducer(producerProperties));
                    }

                    pool.put(producerProperties, producers);
                }
            } finally {
                writeLock.unlock();
            }
        }

        // Return the next producer in the rotation.
        return producers.get(producerRotation.getAndIncrement() % producers.size());
    }

    // Visible for testing
    Producer<K, V> createProducer(Properties properties) {
        return new KafkaProducer<>(properties);
    }

    /**
     * Retrieves the {@link #KAFKA_PRODUCER_CONCURRENCY} from the given {@link Properties}
     *
     * @param props
     *            the {@link Properties} used to configure the producer
     * @return the {@link #KAFKA_PRODUCER_CONCURRENCY} from the given {@link Properties}
     */
    private static int getProducerConcurrency(Properties props) {
        String producerConcurrencyProperty = props.getProperty(KAFKA_PRODUCER_CONCURRENCY,
                DEFAULT_KAFKA_PRODUCER_CONCURRENCY);

        int producerConcurrency = DEFAULT_KAFKA_PRODUCER_CONCURRENCY_INT;

        try {
            producerConcurrency = Integer.parseInt(producerConcurrencyProperty);
        } catch (NumberFormatException e) {
            if (LOGGER.isWarnEnabled())
                LOGGER.warn("Unable to parse [{}] config from value [{}]. Using default [{}] instead.",
                    KAFKA_PRODUCER_CONCURRENCY, producerConcurrencyProperty,
                    DEFAULT_KAFKA_PRODUCER_CONCURRENCY, e);
        }

        // Verify producer concurrency is valid
        if (producerConcurrency <= 0) {
            if (LOGGER.isWarnEnabled())
                LOGGER.warn("The value for config [{}] was <= 0 [{}]. Using default [{}] instead.",
                    KAFKA_PRODUCER_CONCURRENCY, producerConcurrency,
                    DEFAULT_KAFKA_PRODUCER_CONCURRENCY);

            producerConcurrency = DEFAULT_KAFKA_PRODUCER_CONCURRENCY_INT;
        }

        return producerConcurrency;
    }

    /**
     * Closes all {@link Producer producers} that have been produced by this pool.
     * <p>
     * Any subsequent invocation of this method is ignored.
     * </p>
     *
     * @throws IOException
     *             if an error occurs during producer closure.
     *
     * {@inheritDoc}
     */
    @Override
    public void close() throws IOException {
        writeLock.lock();
        try {
            if (!shutdown) {
                shutdown = true;
                final Stream<Exception> exceptions = pool.values().stream()
                        .flatMap(Collection::stream)
                        .flatMap(producer -> {
                            try {
                                producer.close();
                            } catch (Exception e) {
                                LOGGER.error("Could not close producer", e);
                                return Stream.of(e);
                            }
                            return Stream.empty();
                        });

                // Throw exception if any of the producers in the pool could not be closed.
                final Optional<Exception> exception = exceptions.findFirst();
                if (exception.isPresent()) {
                    throw new IOException(exception.get());
                }
            }
        } finally {
            writeLock.unlock();
        }
    }
}
