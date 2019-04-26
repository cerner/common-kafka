package com.cerner.common.kafka.admin;

import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;

import java.nio.charset.Charset;

/**
 * A {@link ZkSerializer Zookeeper serializer} for {@link String} objects.
 * <p>
 * Ported from the {@code kafka.utils.ZKStringSerializer} scala object.
 * </p>
 *
 * @author A. Olson
 */
public class ZkStringSerializer implements ZkSerializer {

    private static final Charset UTF_8 = Charset.forName("UTF-8");

    @Override
    public byte[] serialize(Object data) throws ZkMarshallingError {
        return ((String) data).getBytes(UTF_8);
    }

    @Override
    public Object deserialize(byte[] bytes) throws ZkMarshallingError {
        return bytes != null ? new String(bytes, UTF_8) : null;
    }
}