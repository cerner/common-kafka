package com.cerner.common.kafka.producer.partitioners;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * A basic default {@link Partitioner} implementation that uses a combination of {@link #getTimeHash() time-based} and
 * {@link #getKeyHash(Object) key-based} hashing to determine the target partition for a produced message. Additionally,
 * {@link Cluster#availablePartitionsForTopic(String) available} partitions are always preferred over non-available partitions.
 *
 * @author A. Olson
 */
public class FairPartitioner implements Partitioner {

    /**
     * Logger.
     */
    private static final Logger LOG = LoggerFactory.getLogger(FairPartitioner.class);

    /**
     * Number of milliseconds between rotations of partition assignments.
     */
    public static final int ROTATE_MILLIS = 250;

    /**
     * Randomized clock skew to distribute load when there are multiple concurrent writers.
     */
    public static final int ROTATE_OFFSET = ROTATE_MILLIS * new Random().nextInt(Short.MAX_VALUE);

    @Override
    public void configure(Map<String, ?> configs) {
        //no-op because not configuring anything
    }

    /**
     * Returns a hash value roughly based on the current time. By default the returned value is incremented every
     * {@link #ROTATE_MILLIS} milliseconds, so that all messages produced in a single batch are very likely to be written
     * to the same partition.
     *
     * @return the time hash
     */
    protected int getTimeHash() {
        // Return a temporarily sticky value.
        return (int) ((System.currentTimeMillis() + ROTATE_OFFSET) / ROTATE_MILLIS);
    }

    /**
     * Returns a hash value based on the message key. By default the return value is 0 so that only {@link #getTimeHash()} is
     * used to determine which partition the message is written to.
     *
     * @param key
     *         the message key
     * @return the message key hash
     */
    protected int getKeyHash(@SuppressWarnings("unused") Object key) {
        return 0;
    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        // Combine time and key hashes, then map to a partition.
        return getPartition(topic, cluster, getTimeHash() + getKeyHash(key));
    }

    @Override
    public void close() {
        //no-op because not maintaining resources
    }

    private static int getPartition(String topic, Cluster cluster, int hash) {
        List<PartitionInfo> partitions = cluster.availablePartitionsForTopic(topic);
        if (partitions.isEmpty()) {
            LOG.warn("No available partitions for {} therefore using total partition count for calculations.", topic);
            partitions = cluster.partitionsForTopic(topic);
        }

        int index = Math.abs(hash) % partitions.size();
        return partitions.get(index).partition();
    }
}