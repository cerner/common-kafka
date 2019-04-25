# common-kafka-connect

This project contains [Kafka Connect]((http://kafka.apache.org/documentation.html#connect)) component
implementations.

The primary classes available for use are listed below.

## Sources

No Kafka Connect Sources are currently provided.

## Sinks

### [KafkaSinkConnector](src/main/java/com/cerner/common/kafka/connect/kafka/KafkaSinkConnector.java)

* A simple Kafka Connect Sink Connector that sends data to another Kafka cluster without any transformation or filtering.
* Requires the use of the `org.apache.kafka.connect.converters.ByteArrayConverter` converter.
* To run this connector in kafka-connect add the `common-kafka` and `commmon-kafka-connect` jars to the kafka-connect classpath.

#### Configuration

 * `bootstrap.servers`: The list of broker host/port for the kafka cluster to send data to (Required)
 * `producer.*`: Kafka producer configuration. To add a particular producer config prefix it with `producer.` (i.e. `producer.acks`)

By default the following is set to ensure everything is delivered,

 * `acks`: `all`
 * `max.in.flight.requests.per.connection`: `1`
 * `retries`: `2147483647` (i.e. `Integer.MAX_VALUE`)

### [KafkaSinkTask](src/main/java/com/cerner/common/kafka/connect/kafka/KafkaSinkTask.java)

* Sink Task implementation used by the `KafkaSinkConnector`.
* Does not do any deserialization or serialization of data.
* Sets producer configuration to ensure strong consistency.
* Writes data to the same topic and partition in the destination Kafka cluster that it was read from the source Kafka cluster.