package com.cerner.common.kafka.connect.kafka;

import com.cerner.common.kafka.connect.Version;
import com.cerner.common.kafka.producer.KafkaProducerWrapper;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;

/**
 * The sink task associated to the {@link KafkaSinkConnector} used to replicate Kafka data from one cluster to another
 */
public class KafkaSinkTask extends SinkTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSinkTask.class);

    // We maintain a variable for the Kafka producer since the wrapper will not close it
    private Producer<byte[], byte[]> kafkaProducer;

    private KafkaProducerWrapper<byte[], byte[]> producer;

    @Override
    public String version() {
        return Version.getVersion();
    }

    @Override
    public void start(Map<String, String> taskConfig) {
        Properties properties = new Properties();

        // Ensures all data is written successfully and received by all in-sync replicas. This gives us strong consistency
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");

        // Ensures messages are effectively written in order and we maintain strong consistency
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");

        // Tell producer to effectively try forever to avoid connect task stopping due to transient issues
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));

        // We avoid any serialization by just leaving it in the format we read it as
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        // Apply all connector configuration (includes bootstrap config and any other overrides)
        properties.putAll(taskConfig);

        kafkaProducer = buildProducer(properties);
        producer = new KafkaProducerWrapper<>(kafkaProducer);
    }

    // Visible for testing
    protected Producer<byte[], byte[]> buildProducer(Properties properties) {
        return new KafkaProducer<>(properties);
    }

    @Override
    public void put(Collection<SinkRecord> collection) {
        // Any retriable exception thrown here will be attempted again and not cause the task to pause
        for(SinkRecord sinkRecord : collection) {
            if (sinkRecord.keySchema() != Schema.OPTIONAL_BYTES_SCHEMA || sinkRecord.valueSchema() != Schema.OPTIONAL_BYTES_SCHEMA)
                throw new IllegalStateException("Expected sink record key/value to be optional bytes, but saw instead key: "
                        + sinkRecord.keySchema() + " value: " + sinkRecord.valueSchema() + ". Must use converter: " +
                        ByteArrayConverter.class.getName());

            LOGGER.debug("Sending record {}", sinkRecord);

            try {
                producer.send(new ProducerRecord<>(sinkRecord.topic(), sinkRecord.kafkaPartition(), (byte[]) sinkRecord.key(),
                        (byte[]) sinkRecord.value()));
            } catch (KafkaException e) {
                // If send throws an exception ensure we always retry the record/collection
                throw new RetriableException(e);
            }
        }
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
        LOGGER.debug("Flushing kafka sink");

        try {
            producer.flush();
        } catch (IOException e) {
            LOGGER.debug("IOException on flush, re-throwing as retriable", e);
            // Re-throw exception as connect retriable since we just want connect to keep retrying forever
            throw new RetriableException(e);
        }

        super.flush(offsets);
    }

    @Override
    public void stop() {
        try {
            producer.close();
        } catch (IOException e) {
            LOGGER.warn("Failed to close producer wrapper", e);
        }

        try {
            kafkaProducer.close();
        } catch (KafkaException e) {
            LOGGER.warn("Failed to close producer", e);
        }
    }
}
