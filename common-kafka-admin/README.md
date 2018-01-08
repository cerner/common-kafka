# common-kafka-admin

This project provides a client library to simplify some commonly performed administrative
operations.

## Classes

### [KafkaAdminClient](src/main/java/com/cerner/common/kafka/admin/KafkaAdminClient.java)

* Wraps Kafka resource administration functionality including the following capabilities:
  * Topic creation and deletion
  * Topic and partition inventory
  * Topic configuration management
  * Topic partition management
  * ACL management
  * Consumer group status
* Simplifies application error-handling logic by catching and dealing with certain commonly
encountered exceptions.
* Coordinates retry and timeout logic for asynchronously completed operations

## Scala compatibility

`common-kafka-admin` is verified to be compatible with both Scala 2.12 and 2.11.

`common-kafka-admin` uses Scala 2.12 by default. As such, any consumers of `common-kafka-admin` depending on Scala 2.11 should exclude the `kafka_2.12` and `scala-library` dependencies.

Contributors to `common-kafka-admin` codebase should attempt to avoid using Scala libraries as Scala does not provide guarantees on binary compatibility between major versions. We specifically have encountered problems with JavaConverters, and consequently found it necessary to roll our own collection conversion logic.

To verify compatibility, simply use the `verify-compatibility` profile:

```
mvn clean verify -P verify-compatibility
```
