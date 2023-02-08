package com.cerner.common.kafka.producer.partitioners;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.number.OrderingComparison.greaterThanOrEqualTo;
import static org.hamcrest.number.OrderingComparison.lessThan;
import static org.hamcrest.MatcherAssert.assertThat;

public class FairPartitionerTest {

    private FairPartitioner partitioner;
    private String topic;
    private Object key;
    private byte[] keyBytes;
    private Object value;
    private byte[] valueBytes;
    private Cluster cluster;
    private List<PartitionInfo> allPartitions;
    private List<PartitionInfo> notAvailablePartitions;
    private Node node;

    @BeforeEach
    public void setup(TestInfo testInfo) throws InterruptedException {
        partitioner = new FairPartitioner();
        topic = testInfo.getDisplayName().replaceAll("[^a-zA-Z0-9]", "-").trim();
        key = new Object();
        keyBytes = new byte[0];
        value = new Object();
        valueBytes = new byte[0];

        node = new Node(1, "example.com", 6667);

        allPartitions =
                IntStream.range(0, 8).mapToObj(i -> {
                    //null leader means not available
                    Node leader = null;
                    if(i % 2 == 0){
                        //a non-null leader means it is available
                        leader = node;
                    }
                    return new PartitionInfo(topic, i, leader, null, null);
                }).collect(Collectors.toList());
        notAvailablePartitions = allPartitions.stream().filter(p -> p.leader() == null).collect(Collectors.toList());

        cluster = new Cluster("clusterId", Collections.singleton(node), allPartitions,
                Collections.emptySet(), Collections.emptySet());

        // Wait until next clock window tick.
        long millis = System.currentTimeMillis() / FairPartitioner.ROTATE_MILLIS;
        while (System.currentTimeMillis() / FairPartitioner.ROTATE_MILLIS == millis) {
            Thread.sleep(1);
        }
    }

    @Test
    public void partitionAvailable() {
        int partition = partitioner.partition(topic, key, keyBytes, value, valueBytes, cluster);
        assertThat(partition, is(lessThan(allPartitions.size())));
        assertThat(partition, is(greaterThanOrEqualTo(0)));
        assertThat(partition % 2, is(0));
    }

    @Test
    public void partitionNotAvailable() {
        cluster = new Cluster("clusterId", Collections.singleton(node), notAvailablePartitions,
                Collections.emptySet(), Collections.emptySet());
        int partition = partitioner.partition(topic, key, keyBytes, value, valueBytes, cluster);
        assertThat(partition, is(lessThan(allPartitions.size())));
        assertThat(partition, is(greaterThanOrEqualTo(0)));
        assertThat(partition % 2, is(1));
    }

    @Test
    public void partitionSameTimeWindow() {
        int messages = allPartitions.size();
        Set<Integer> partitions = new HashSet<>();

        for (int i = 0; i < messages; ++i) {
            int partition = partitioner.partition(topic, key, keyBytes, value, valueBytes, cluster);
            assertThat(partition, is(greaterThanOrEqualTo(0)));
            assertThat(partition, is(lessThan(allPartitions.size())));
            partitions.add(partition);
        }

        // Since all messages were produced in the same time window, they should be assigned to the same partition.
        assertThat(partitions.size(), is(1));
    }

    @Test
    public void partitionDifferentTimeWindows() throws InterruptedException {
        int messages = allPartitions.size();
        Set<Integer> partitions = new HashSet<>();

        for (int i = 0; i < messages; ++i) {
            int partition = partitioner.partition(topic, key, keyBytes, value, valueBytes, cluster);
            assertThat(partition, is(greaterThanOrEqualTo(0)));
            assertThat(partition, is(lessThan(allPartitions.size())));
            partitions.add(partition);
            Thread.sleep(FairPartitioner.ROTATE_MILLIS);
        }

        // Verify that partition is periodically rotated across all available partitions as expected.
        assertThat(partitions.size(), is(allPartitions.size() - notAvailablePartitions.size()));
    }
}