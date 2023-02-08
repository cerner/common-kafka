# Common Kafka [![Build Status](https://travis-ci.com/cerner/common-kafka.svg?branch=master)](https://travis-ci.com/cerner/common-kafka)


This repository contains common Kafka code supporting Cerner's cloud-based solutions.

For Maven, add the following,

```
<!-- For client utilities -->
<dependency>
    <groupId>com.cerner.common.kafka</groupId>
    <artifactId>common-kafka</artifactId>
    <version>3.0</version>
</dependency>
<!-- For connect utilities -->
<dependency>
    <groupId>com.cerner.common.kafka</groupId>
    <artifactId>common-kafka-connect</artifactId>
    <version>3.0</version>
</dependency>
<!-- For test utilities -->
<dependency>
    <groupId>com.cerner.common.kafka</groupId>
    <artifactId>common-kafka-test</artifactId>
    <version>3.0</version>
</dependency>
```

## Project Inventory

The following modules are available for use,

* [common-kafka](common-kafka/README.md): Lightweight wrapper for
[producers](http://kafka.apache.org/32/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html)
and [consumers](http://kafka.apache.org/32/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html)
in the [kafka-clients](https://github.com/apache/kafka/tree/trunk/clients) library.
* [common-kafka-connect](common-kafka-connect/README.md): Contains
[Kafka Connect](http://kafka.apache.org/documentation.html#connect) component implementations.
* [common-kafka-test](common-kafka-test/README.md): Provides infrastructure for integration or "heavy"
unit tests that need a running Kafka broker and ZooKeeper service.

Please refer to the project-specific README documentation for content details.

## Version Requirements

The 3.0 release of common-kafka uses the following dependency versions.

* [Kafka](http://kafka.apache.org/): 3.2.0
* [Metrics](http://metrics.dropwizard.io/): 2.2.0
* [Scala](https://scala-lang.org/): 2.13.5

Note that the Scala dependency is only applicable for common-kafka-test.

## Upgrading from common-kafka 2.x to 3.0

* The common-kafka-admin module has been removed, KafkaAdminClient can be replaced by Kafka's [admin client](https://github.com/apache/kafka/blob/3.2.0/clients/src/main/java/org/apache/kafka/clients/admin/AdminClient.java)
* KafkaBrokerTestHarness has been updated to require TestInfo in the setup method, which requires junit5.
 By extending AbstractKafkaTests instead of using KafkaBrokerTestHarness directly you can avoid needing to add a junit5 dependency.

## Contribute

You are welcome to contribute to Common-Kafka.

Read our [Contribution guidelines](CONTRIBUTING.md).

## License

```
Copyright 2017 Cerner Innovation, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
