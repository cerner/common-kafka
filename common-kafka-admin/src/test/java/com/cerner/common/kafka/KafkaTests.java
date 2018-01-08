package com.cerner.common.kafka;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import com.cerner.common.kafka.admin.KafkaAdminClientTest;
import com.cerner.common.kafka.testing.AbstractKafkaTests;

/**
 * Suite of tests requiring an internal Kafka + Zookeeper cluster to be started up.
 *
 * IMPORTANT: New tests added to this project requiring Kafka and/or Zookeeper should be added to this suite so they will be
 * executed as part of the build.
 */
@RunWith(Suite.class)
@SuiteClasses({
        // com.cerner.common.kafka.admin
        KafkaAdminClientTest.class
})
public class KafkaTests extends AbstractKafkaTests {
}
