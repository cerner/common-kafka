package com.cerner.common.kafka.connect.kafka;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;

import java.util.Map;

/**
 * A pass-through Kafka Connect converter for raw byte data
 */
// This can eventually replaced by Kafka's converter
// https://github.com/apache/kafka/blob/trunk/connect/runtime/src/main/java/org/apache/kafka/connect/converters/
// ByteArrayConverter.java - (Kafka 0.11.0) https://issues.apache.org/jira/browse/KAFKA-4783
public class ByteArrayConverter implements Converter {

    @Override
    public void configure(Map<String, ?> config, boolean isKey) {
    }

    @Override
    public byte[] fromConnectData(String topic, Schema schema, Object value) {
        if (schema != null && (schema.type() != Schema.Type.BYTES))
            throw new DataException("Invalid schema type for ByteArrayConverter: " + schema.type());

        if (value != null && !(value instanceof byte[]))
            throw new DataException(getClass().getSimpleName() + " is not compatible with objects of type " + value.getClass());

        return (byte[]) value;
    }

    @Override
    public SchemaAndValue toConnectData(String topic, byte[] bytes) {
        return new SchemaAndValue(Schema.OPTIONAL_BYTES_SCHEMA, bytes);
    }
}
