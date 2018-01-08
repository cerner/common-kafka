package com.cerner.common.kafka.connect.kafka;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.Future;

import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class KafkaSinkTaskTest {

    @Mock
    private Producer<byte[], byte[]> kafkaProducer;

    @Mock
    private Future<RecordMetadata> recordMetadataFuture;

    private SinkRecord sinkRecord;
    private KafkaSinkTaskTester task;

    @Before
    public void before() {
        task = new KafkaSinkTaskTester();

        // Doesn't matter what we provide it since everything is mocked just need to call start
        task.start(Collections.emptyMap());

        sinkRecord = new SinkRecord("topic", 0, Schema.OPTIONAL_BYTES_SCHEMA, "key".getBytes(),
                Schema.OPTIONAL_BYTES_SCHEMA, "value".getBytes(), 0L);

        when(kafkaProducer.send(anyObject())).thenReturn(recordMetadataFuture);
    }

    @Test
    public void put() {
        task.put(Collections.singletonList(sinkRecord));
        verify(kafkaProducer).send(anyObject());
    }

    @Test (expected = IllegalStateException.class)
    public void put_recordKeyIsNotNullOrBytes() {
        sinkRecord = new SinkRecord("topic", 0, Schema.STRING_SCHEMA, "key",
                Schema.OPTIONAL_BYTES_SCHEMA, "value".getBytes(), 0L);
        task.put(Collections.singletonList(sinkRecord));
    }

    @Test (expected = IllegalStateException.class)
    public void put_recordValueIsNotNullOrBytes() {
        sinkRecord = new SinkRecord("topic", 0, Schema.OPTIONAL_BYTES_SCHEMA, "key".getBytes(),
                Schema.STRING_SCHEMA, "value", 0L);
        task.put(Collections.singletonList(sinkRecord));
    }

    @Test (expected = RetriableException.class)
    public void put_producerThrowsException() {
        when(kafkaProducer.send(anyObject())).thenThrow(new KafkaException());
        task.put(Collections.singletonList(sinkRecord));
    }

    @Test
    public void flush() {
        task.put(Collections.singletonList(sinkRecord));

        // Doesn't matter what the offset map is just need to call flush
        task.flush(Collections.emptyMap());

        verify(kafkaProducer).flush();
    }

    @Test (expected = RetriableException.class)
    public void flush_producerThrowsException() {
        doThrow(new KafkaException()).when(kafkaProducer).flush();

        task.put(Collections.singletonList(sinkRecord));

        // Doesn't matter what the offset map is just need to call flush
        task.flush(Collections.emptyMap());
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
