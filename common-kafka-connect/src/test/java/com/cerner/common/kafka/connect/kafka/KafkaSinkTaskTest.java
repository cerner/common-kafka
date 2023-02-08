package com.cerner.common.kafka.connect.kafka;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class KafkaSinkTaskTest {

    @Mock
    private Producer<byte[], byte[]> kafkaProducer;

    @Mock
    private Future<RecordMetadata> recordMetadataFuture;

    private SinkRecord sinkRecord;
    private KafkaSinkTaskTester task;

    @BeforeEach
    public void before() {
        task = new KafkaSinkTaskTester();

        // Doesn't matter what we provide it since everything is mocked just need to call start
        task.start(Collections.emptyMap());

        sinkRecord = new SinkRecord("topic", 0, Schema.OPTIONAL_BYTES_SCHEMA, "key".getBytes(),
                Schema.OPTIONAL_BYTES_SCHEMA, "value".getBytes(), 0L);
    }

    @Test
    public void put() {
        when(kafkaProducer.send(any())).thenReturn(recordMetadataFuture);
        task.put(Collections.singletonList(sinkRecord));
        verify(kafkaProducer).send(any());
    }

    @Test
    public void put_recordKeyIsNotNullOrBytes() {
        sinkRecord = new SinkRecord("topic", 0, Schema.STRING_SCHEMA, "key",
                Schema.OPTIONAL_BYTES_SCHEMA, "value".getBytes(), 0L);
        assertThrows(IllegalStateException.class,
                () -> task.put(Collections.singletonList(sinkRecord)));
    }

    @Test
    public void put_recordValueIsNotNullOrBytes() {
        sinkRecord = new SinkRecord("topic", 0, Schema.OPTIONAL_BYTES_SCHEMA, "key".getBytes(),
                Schema.STRING_SCHEMA, "value", 0L);
        assertThrows(IllegalStateException.class,
                () -> task.put(Collections.singletonList(sinkRecord)));
    }

    @Test
    public void put_producerThrowsException() {
        when(kafkaProducer.send(any())).thenReturn(recordMetadataFuture);
        when(kafkaProducer.send(any())).thenThrow(new KafkaException());
        assertThrows(RetriableException.class,
                () -> task.put(Collections.singletonList(sinkRecord)));
    }

    @Test
    public void flush() {
        when(kafkaProducer.send(any())).thenReturn(recordMetadataFuture);
        task.put(Collections.singletonList(sinkRecord));

        // Doesn't matter what the offset map is just need to call flush
        task.flush(Collections.emptyMap());

        verify(kafkaProducer).flush();
    }

    @Test
    public void flush_producerThrowsException() {
        when(kafkaProducer.send(any())).thenReturn(recordMetadataFuture);
        doThrow(new KafkaException()).when(kafkaProducer).flush();

        task.put(Collections.singletonList(sinkRecord));

        // Doesn't matter what the offset map is just need to call flush
        assertThrows(RetriableException.class,
                () -> task.flush(Collections.emptyMap()));
    }

    private class KafkaSinkTaskTester extends KafkaSinkTask {

        protected Properties producerProperties;

        @Override
        protected Producer<byte[], byte[]> buildProducer(Properties properties) {
            producerProperties = properties;
            return kafkaProducer;
        }
    }
}
