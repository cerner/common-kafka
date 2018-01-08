package com.cerner.common.kafka;

import org.apache.kafka.common.TopicPartition;

import java.io.Serializable;
import java.util.Comparator;

/**
 * A {@link Comparator} for {@link TopicPartition} objects.
 */
public class TopicPartitionComparator implements Comparator<TopicPartition>, Serializable {
    private static final long serialVersionUID = -2400690278866168934L;

    @Override
    public int compare(TopicPartition a, TopicPartition b) {
        int compareTopics = a.topic().compareTo(b.topic());
        return compareTopics != 0 ? compareTopics : Integer.compare(a.partition(), b.partition());
    }
}
