# common-kafka

This project provides a lightweight wrapper for
[producers](http://kafka.apache.org/0102/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html)
and [consumers](http://kafka.apache.org/0102/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html)
in the [kafka-clients](https://github.com/apache/kafka/tree/trunk/clients) library. It is intended to
simplify and standardize basic Kafka interactions, without introducing any extraneous dependencies.

The primary classes available for use are listed below.

## Producer

### [KafkaProducerPool](src/main/java/com/cerner/common/kafka/producer/KafkaProducerPool.java)

* Manages a thread-safe pool of producers to improve performance for highly concurrent applications.
* Provides a set of reasonable default configuration properties for producers.
* Creates multiple pools as needed to accommodate producers with differing configuration.

### [KafkaProducerWrapper](src/main/java/com/cerner/common/kafka/producer/KafkaProducerWrapper.java)

* Simplifies the sending of Kafka messages and corresponding error-handling concerns.
* Supports both synchronous and asynchronous usage patterns.

### [FairPartitioner](src/main/java/com/cerner/common/kafka/producer/partitioners/FairPartitioner.java)

* Maximizes message throughput and broker storage efficiency for high-volume applications by grouping
messages produced in a window of time to the same partition.
* Retains some functionality of the default partitioner such as preference of partitions with an
available leader.
* Allows a supplemental key-based hash value to be supplied.

## Consumer

### [ProcessingKafkaConsumer](src/main/java/com/cerner/common/kafka/consumer/ProcessingKafkaConsumer.java)

* Manages consumer group topic subscriptions and the resulting partition assignments.
* Allows consumed messages to be either acknowledged as successfully processed or marked as failed and
scheduled for future retried consumption.
* Identifies the acked message offsets that are eligible to commit for each assigned partition.
* Periodically commits the offset marking the end of a contiguous range of acked messages, triggered by
either the number of commit-pending messages or elapsed time since the last commit
* Tracks a number of important consumer processing metrics, to assist with monitoring and troubleshooting
needs.
* Simplifies consumer error-handling logic by catching and dealing with certain commonly encountered
exceptions.
* Handles the processing state changes that occur when the consumer group partition assignments are
rebalanced due to group membership or topic subscription changes.
* Dynamically pauses and resumes message consumption from partitions based on configurable thresholds to
limit the rate of processing failures.

### [ProcessingConfig](src/main/java/com/cerner/common/kafka/consumer/ProcessingConfig.java)

* Encapsulates the configuration for a `ProcessingKafkaConsumer` including offset commit thresholds and
failure handling behavior.

### [ConsumerOffsetClient](src/main/java/com/cerner/common/kafka/consumer/ConsumerOffsetClient.java)

* Wraps consumer offset management functionality including the following capabilities:
  * Retrieve the earliest or latest available broker log offsets for each partition of a collection of
topics.
  * Retrieve the broker log offset of the first message written after a specified timestamp for each
partition of a collection of topics.
  * Retrieve the committed processing offset for a consumer group for each partition of a collection of
topics, or a specific partition.
  * Commit specific processing offsets for a collection of partitions for a consumer group.
  * Identify the existing partitions for a collection of topics.

### [FairAssignor](src/main/java/com/cerner/common/kafka/consumer/assignors/FairAssignor.java)

* Balances assigned partitions across the members of a consumer group such that each group member is
assigned approximately the same number of partitions, even if the consumer topic subscriptions are
substantially different.

## Miscellany

### [TopicPartitionComparator](src/main/java/com/cerner/common/kafka/TopicPartitionComparator.java)

* A comparator for sorting collections of `TopicPartition` objects first by topic name and then partition
number.