# common-kafka-test

This project contains testing infrastructure that is useful when developing integration or "heavy" unit
tests that need a running Kafka broker and ZooKeeper service.

The primary classes available for use are listed below.

## Kafka

### [KafkaBrokerTestHarness](src/main/java/com/cerner/common/kafka/testing/KafkaBrokerTestHarness.java)

* Creates a cluster of one or more Kafka brokers which run within the process.
* Adjusts broker configuration to minimize runtime resource utilization.
* Cleans up data on broker shutdown.

### [AbstractKafkaTests](src/main/java/com/cerner/common/kafka/testing/AbstractKafkaTests.java)

* Provides a simple framework for coordinating the `KafkaBrokerTestHarness` with a suite of unit tests.
