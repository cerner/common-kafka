package com.cerner.common.kafka.testing;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import org.apache.kafka.common.TopicPartition;

/**
 * Assorted Kafka testing utility methods.
 *
 * @author A. Olson
 */
public class KafkaTestUtils {

    private static final String TEMP_DIR_PREFIX = "kafka-";

    private static final Set<Integer> USED_PORTS = new HashSet<>();

    /**
     * Creates and returns a new randomly named temporary directory. It will be deleted upon JVM exit.
     *
     * @return a new temporary directory.
     *
     * @throws RuntimeException if a new temporary directory could not be created.
     */
    public static File getTempDir() {
        try {
            File tempDir = Files.createTempDirectory(TEMP_DIR_PREFIX).toFile();
            tempDir.deleteOnExit();
            return tempDir;
        } catch (IOException e) {
            throw new RuntimeException("could not create temp directory", e);
        }
    }

    /**
     * Returns an array containing the specified number of available local ports.
     *
     * @param count Number of local ports to identify and return.
     *
     * @return an array of available local port numbers.
     *
     * @throws RuntimeException if an I/O error occurs opening or closing a socket.
     */
    public static int[] getPorts(int count) {
        int[] ports = new int[count];
        Set<ServerSocket> openSockets = new HashSet<>(count + USED_PORTS.size());

        for (int i = 0; i < count;) {
            try {
                ServerSocket socket = new ServerSocket(0);
                int port = socket.getLocalPort();
                openSockets.add(socket);

                // Disallow port reuse.
                if (!USED_PORTS.contains(port)) {
                    ports[i++] = port;
                    USED_PORTS.add(port);
                }
            } catch (IOException e) {
                throw new RuntimeException("could not open socket", e);
            }
        }

        // Close the sockets so that their port numbers can be used by the caller.
        for (ServerSocket socket : openSockets) {
            try {
                socket.close();
            } catch (IOException e) {
                throw new RuntimeException("could not close socket", e);
            }
        }

        return ports;
    }

    /**
     * Sums the messages for the given map of {@link TopicPartition} to offsets for the specified topic. This map can be
     * retrieved using {@code com.cerner.common.kafka.consumer.ConsumerOffsetClient#getEndOffsets(Collection<String>)}.
     *
     * @param topic   the topic to count messages for
     * @param offsets the map of broker offset values
     * @return the sum of the offsets for a particular topic
     * @throws IllegalArgumentException if topic is {@code null}, empty or blank, or if offsets is {@code null}
     */
    public static long getTopicAndPartitionOffsetSum(String topic, Map<TopicPartition, Long> offsets) {
        if (StringUtils.isBlank(topic))
            throw new IllegalArgumentException("topic cannot be null, empty or blank");
        if (offsets == null)
            throw new IllegalArgumentException("offsets cannot be null");

        long count = 0;
        for (Entry<TopicPartition, Long> entry : offsets.entrySet()) {
            Long value = entry.getValue();
            if (topic.equals(entry.getKey().topic()) && value != null) {
                count += value;
            }
        }
        return count;
    }
}


