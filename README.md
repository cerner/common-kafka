# Common Kafka

This repository contains common Kafka code supporting Cerner's cloud-based solutions.

## Project Inventory

The following modules are available for use,

* [common-kafka](common-kafka/README.md): Lightweight wrapper for
[producers](http://kafka.apache.org/0102/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html)
and [consumers](http://kafka.apache.org/0102/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html)
in the [kafka-clients](https://github.com/apache/kafka/tree/trunk/clients) library.
* [common-kafka-admin](common-kafka-admin/README.md): Simplifies some commonly performed administrative
operations.
* [common-kafka-connect](common-kafka-connect/README.md): Contains
[Kafka Connect](http://kafka.apache.org/documentation.html#connect) component implementations.
* [common-kafka-test](common-kafka-test/README.md): Provides infrastructure for integration or "heavy"
unit tests that need a running Kafka broker and ZooKeeper service.

Please refer to the project-specific README documentation for content details.

## Version Requirements

The 1.0 release of common-kafka requires the following dependency versions.

* [Kafka](http://kafka.apache.org/): 0.10.2.1
* [Metrics](http://metrics.dropwizard.io/): 2.2.0
* [Scala](https://scala-lang.org/): 2.12.1
* [ZooKeeper](https://zookeeper.apache.org/): 3.4.9

Note that the Scala and ZooKeeper dependencies are only applicable for common-kafka-admin and
common-kafka-test.

## Contribute

You are more than welcome to Contribute to Beadledom.

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