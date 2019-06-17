package com.cerner.common.kafka.producer;

import com.cerner.common.kafka.KafkaExecutionException;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

/**
 * Wraps around an instance of a {@link Producer producer}.  {@link #close() Closing} the wrapper will
 * not affect the wrapped producer instance.  The creators of the wrapped producer instances are still
 * responsible for closing the producer when appropriate.
 *
 * Supports synchronously sending {@link ProducerRecord records}.
 * <p>
 * Example usage:
 * <pre>
 *     Properties producerProps = new Properties();
 *     // required properties to construct a KafkaProducer instance
 *     // constants for these config keys exist in org.apache.kafka.clients.producer.ProducerConfig
 *     producerProps.put("bootstrap.servers", "localhost:4242");
 *     producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
 *     producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
 *
 *     // properties that affect batching
 *
 *     // linger.ms is used to tell the producer to wait this amount of time
 *     // for more records to fill the buffer before sending. The default value of
 *     // 0 will tell the producer to start sending immediately and to not wait.
 *     // {@link #sendSynchronously(List)} will attempt to send the entire batch at once
 *     // but could be affected by this value if it is not set, or set too low. This could
 *     // result in more I/O operations taking place than what is desired. It is recommended
 *     // to set this value appropriately high as to not affect sending in batches.
 *     producerProps.put("linger.ms", 10000);
 *
 *     // batch.size will tell the producer how many records to hold in memory, in bytes,
 *     // before it should send its buffer. This value will include both the key and the payload.
 *     // It is recommended to set this value at the value of 'max.message.bytes' * desired batch (in number of records) size
 *     // to ensure that all records sent to {@link #sendSynchronously(List)} are batched together.
 *     // NOTE: the setting of 'linger.ms' will supersede this value.
 *     producerProps.put("batch.size", 16384);
 *
 *     KafkaProducerWrapper&lt;String, String&gt; producerWrapper = new KafkaProducerWrapper&lt;&gt;(
 *                              new KafkaProducer&lt;&gt;(producerProps);
 *
 *     String topic = "myTopic";
 *     String key = "myKey";
 *     String value = "myValue";
 *
 *     ProducerRecord&lt;String, String&gt; record = new ProducerRecord&lt;&gt;(topic, key, value);
 *
 *     // synchronously sends the specified record; the underlying KafkaProducer will be flushed
 *     // immediately and the record will be sent.
 *     producerWrapper.sendSynchronously(record);
 *
 *     // alternatively, a collection of ProducerRecords can be submitted at a time to
 *     // the sendSynchronous method and it will behave the same way and block until all messages
 *     // have been submitted. If a KafkaExecutionException is thrown the context as to which
 *     // message in the collection caused the failure will not be included. See the
 *     // javadoc for KafkaExecutionException for more information about the potential errors
 *
 *     String topic = "mySecondTopic";
 *     String key = "mySecondKey";
 *     String value = "mySecondValue";
 *
 *     ProducerRecord&lt;String, String&gt; secondRecord = new ProducerRecord&lt;&gt;(topic, key, value);
 *     List&lt;ProducerRecord&lt;String, String&gt;&gt; records = new ArrayList&lt;&gt;();
 *     records.add(record);
 *     records.add(secondRecord);
 *
 *     producerWrapper.sendSynchronously(records);
 *
 *     // when the producerWrapper is done being used call #close(), which will close
 *     // the underlying KafkaProducer. NOTE: be aware of how the wrapper instance is being
 *     // managed, as closing it may cause issues if the instance is being used
 *     // across multiple threads.
 *     producerWrapper.close();
 * </pre>
 *
 * The wrapper also supports the idea of sending incrementally instead of as a large collection.  The synchronous
 * sending behavior can still be supported but feedback about a failure will not be provided until the wrapper has
 * been {@link #flush() flushed}.  Similar to the description above the "linger.ms" and "batch.size" should be
 * configured appropriately for your desired throughput and latency.
 *
 * <pre>
 *      KafkaProducerWrapper&lt;String, String&gt; producerWrapper = new KafkaProducerWrapper&lt;&gt;(
 *                              new KafkaProducer&lt;&gt;(producerProps);
 *
 *      producerWrapper.send(new ProducerRecord&lt;&gt;(topic, key1, value1));
 *      producerWrapper.send(new ProducerRecord&lt;&gt;(topic, key2, value2));
 *      producerWrapper.send(new ProducerRecord&lt;&gt;(topic, key3, value3));
 *
 *      //will block and return only when all sent events have been successfully flushed or an error has occurred.
 *      try{}
 *          producerWrapper.flush();
 *      }catch(IOException ioe){
 *          //an error occurred and all pending sent messages should be expected to be resent or handled
 *          //appropriately.
 *      }finally{
 *          producerWrapper.close();
 *      }
 *
 * </pre>
 *
 * This class is not thread safe.
 *
 * @author Stephen Durfey
 */
public class KafkaProducerWrapper<K, T> implements Closeable, Flushable {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProducerWrapper.class);

    private final Producer<K, T> kafkaProducer;

    private final List<Future<RecordMetadata>> pendingWrites;

    /**
     * A timer for {@link #send(ProducerRecord)} calls
     */
    static final Timer SEND_TIMER = Metrics.newTimer(KafkaProducerWrapper.class, "send");

    /**
     * A timer for {@link #sendSynchronously(ProducerRecord)} or {@link #sendSynchronously(List)} calls
     */
    static final Timer SYNC_SEND_TIMER = Metrics.newTimer(KafkaProducerWrapper.class, "send-synchronously");

    /**
     * A timer for {@link #flush()} calls
     */
    static final Timer FLUSH_TIMER = Metrics.newTimer(KafkaProducerWrapper.class, "flush");

    /**
     * A histogram tracking batch size, updated by {@link #sendSynchronously(List)} or {@link #flush()}
     */
    static final Histogram BATCH_SIZE_HISTOGRAM = Metrics.newHistogram(KafkaProducerWrapper.class, "batch-size", true);

    /**
     * Creates an instance to manage interacting with the {@code kafkaProducer}.
     *
     * @param kafkaProducer
     *            the {@link Producer} to interact with
     */
    public KafkaProducerWrapper(Producer<K, T> kafkaProducer) {
        if (kafkaProducer == null) {
            throw new IllegalStateException("kafkaProducer cannot be null");
        }

        this.kafkaProducer = kafkaProducer;
        pendingWrites = new LinkedList<>();
    }

    /**
     * Sends the specified {@code record}.  Data is not guaranteed to be written until {@link #flush()} is called.
     *
     * @param record the record to send to Kafka
     * @throws IllegalArgumentException the {@code record} cannot be {@code null}.
     * @throws org.apache.kafka.common.KafkaException if there is an issue sending the message to Kafka
     */
    public void send(ProducerRecord<K, T> record){
        if(record == null){
            throw new IllegalArgumentException("The 'record' cannot be 'null'.");
        }

        TimerContext context = SEND_TIMER.time();
        try {
            pendingWrites.add(kafkaProducer.send(record));
        } finally {
            context.stop();
        }
    }

    /**
     * Synchronously sends the {@code record}. The underlying {@link Producer} is immediately flushed and the call will block until
     * the {@code record} is sent.
     *
     * @param record
     *            the records to send to Kafka
     * @throws IOException
     *             indicates that there was a Kafka error that led to the message not being sent.
     * @throws org.apache.kafka.common.KafkaException
     *             if there is an issue sending the message to Kafka
     */
    public void sendSynchronously(ProducerRecord<K, T> record) throws IOException {
        sendSynchronously(Collections.singletonList(record));
    }

    /**
     * Synchronously sends all {@code records}. The underlying {@link Producer} is immediately flushed and the call will block until
     * the {@code records} have been sent.
     *
     * @param records
     *            the records to send to Kafka
     * @throws IOException
     *             indicates that there was a Kafka error that lead to one of the messages not being sent.
     * @throws org.apache.kafka.common.KafkaException
     *             if there is an issue sending the messages to Kafka
     */
    public void sendSynchronously(List<ProducerRecord<K, T>> records) throws IOException {
        // Disregard empty batches.
        if (records.isEmpty()) {
            LOGGER.debug("records was empty; nothing to process");
            return;
        }

        BATCH_SIZE_HISTOGRAM.update(records.size());

        TimerContext context = SYNC_SEND_TIMER.time();
        try {
            List<Future<RecordMetadata>> futures = records.stream().map(kafkaProducer::send).collect(Collectors.toList());

            handlePendingWrites(futures);
        } finally {
            context.stop();
        }
    }

    /**
     * Flushes the underlying pending writes created by calls to {@link #send(ProducerRecord)}
     * to the {@link Producer} ensuring they persisted.
     *
     * If an exception occurs when flushing, all pending writes should be
     * {@link #send(ProducerRecord) sent again}.
     *
     * @throws IOException If there was an error persisting one of the pending writes
     */
    @Override
    public void flush() throws IOException{
        if (pendingWrites.isEmpty()) {
            LOGGER.debug("nothing to flush");
            return;
        }

        BATCH_SIZE_HISTOGRAM.update(pendingWrites.size());

        TimerContext context = FLUSH_TIMER.time();
        try {
            handlePendingWrites(pendingWrites);
        } finally{
            pendingWrites.clear();
            context.stop();
        }
    }

    private void handlePendingWrites(List<Future<RecordMetadata>> pendingWrites) throws IOException{
        // Future#get will sit and wait until 'linger.ms' has been reached
        // or flush is called, so flush here.
        try {
            kafkaProducer.flush();
        }catch(RuntimeException re){
            //Exception isn't retriable so wrap in known exception which represents fatal.
            throw new IOException("Unable to flush producer.",re);
        }
        for (final Future<RecordMetadata> future : pendingWrites) {
            try {
                future.get();
            } catch (InterruptedException e) {
                throw new IOException(e);
            } catch (ExecutionException e) {
                throw new KafkaExecutionException(e);
            }
        }
    }

    @Override
    public void close() throws IOException {
        LOGGER.debug("Closing the producer wrapper and flushing outstanding writes.");
        flush();
    }
}