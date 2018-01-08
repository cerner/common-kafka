package com.cerner.common.kafka;

import com.cerner.common.kafka.consumer.ConsumerOffsetClientTest;
import com.cerner.common.kafka.consumer.ProcessingConfigTest;
import com.cerner.common.kafka.consumer.ProcessingKafkaConsumerTest;
import com.cerner.common.kafka.consumer.ProcessingPartitionTest;
import com.cerner.common.kafka.consumer.assignors.FairAssignorTest;
import com.cerner.common.kafka.producer.partitioners.FairPartitionerTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

/**
 * Suite of tests that can be run independently.
 * 
 * IMPORTANT: New tests added to this project <em>not</em> requiring cluster services should be added to this suite so they will be
 * executed as part of the build.
 */
@RunWith(Suite.class)
@SuiteClasses({
        // com.cerner.common.kafka.consumer
        ConsumerOffsetClientTest.class, ProcessingConfigTest.class, ProcessingKafkaConsumerTest.class,
        ProcessingPartitionTest.class,

        // com.cerner.common.kafka.consumer.assignors
        FairAssignorTest.class,

        // com.cerner.common.kafka.producer.partitioners
        FairPartitionerTest.class
})
public class StandaloneTests {
}