package com.cerner.common.kafka.connect.kafka;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.*;

public class ByteArrayConverterTest {

    private static final String TOPIC = "topic";
    private static final byte[] SAMPLE_BYTES = "sample string".getBytes(StandardCharsets.UTF_8);

    private ByteArrayConverter converter = new ByteArrayConverter();

    @Before
    public void setUp() {
        converter.configure(Collections.<String, String>emptyMap(), false);
    }

    @Test
    public void testFromConnect() {
        assertArrayEquals(
                SAMPLE_BYTES,
                converter.fromConnectData(TOPIC, Schema.BYTES_SCHEMA, SAMPLE_BYTES)
        );
    }

    @Test
    public void testFromConnectSchemaless() {
        assertArrayEquals(
                SAMPLE_BYTES,
                converter.fromConnectData(TOPIC, null, SAMPLE_BYTES)
        );
    }

    @Test(expected = DataException.class)
    public void testFromConnectBadSchema() {
        converter.fromConnectData(TOPIC, Schema.INT32_SCHEMA, SAMPLE_BYTES);
    }

    @Test(expected = DataException.class)
    public void testFromConnectInvalidValue() {
        converter.fromConnectData(TOPIC, Schema.BYTES_SCHEMA, 12);
    }

    @Test
    public void testFromConnectNull() {
        assertNull(converter.fromConnectData(TOPIC, Schema.BYTES_SCHEMA, null));
    }

    @Test
    public void testToConnect() {
        SchemaAndValue data = converter.toConnectData(TOPIC, SAMPLE_BYTES);
        assertEquals(Schema.OPTIONAL_BYTES_SCHEMA, data.schema());
        assertTrue(Arrays.equals(SAMPLE_BYTES, (byte[]) data.value()));
    }

    @Test
    public void testToConnectNull() {
        SchemaAndValue data = converter.toConnectData(TOPIC, null);
        assertEquals(Schema.OPTIONAL_BYTES_SCHEMA, data.schema());
        assertNull(data.value());
    }
}
