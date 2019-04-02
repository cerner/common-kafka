
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cerner.common.kafka.testing;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.ZKConfig;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.utils.Time;
import scala.Option;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static scala.collection.JavaConversions.asJavaIterable;

/**
 * A test harness that brings up some number of Kafka broker nodes.
 * <p>
 * Adapted from the {@code kafka.integration.KafkaServerTestHarness} class.
 * </p>
 *
 * @author A. Olson
 */
public class KafkaBrokerTestHarness extends ZookeeperTestHarness {

    /**
     * Default number of brokers in the Kafka cluster.
     */
    public static final int DEFAULT_BROKERS = 1;

    /**
     * Default number of partitions per Kafka topic.
     */
    public static final int PARTITIONS_PER_TOPIC = 4;

    private List<KafkaConfig> brokerConfigs;
    private List<KafkaServer> brokers;
    private boolean setUp;
    private boolean tornDown;
    private String clusterId;

    /**
     * Creates a new Kafka broker test harness using the {@link #DEFAULT_BROKERS default} number of brokers.
     */
    public KafkaBrokerTestHarness() {
        this(DEFAULT_BROKERS, KafkaTestUtils.getPorts(1)[0]);
    }

    /**
     * Creates a new Kafka broker test harness using the {@link #DEFAULT_BROKERS default} number of brokers and the supplied
     * {@link Properties} which will be applied to the brokers.
     *
     * @param properties
     *            the additional {@link Properties} supplied to the brokers
     * @throws IllegalArgumentException
     *             if {@code properties} is {@code null}
     */
    public KafkaBrokerTestHarness(Properties properties) {
        this(DEFAULT_BROKERS, KafkaTestUtils.getPorts(1)[0], properties);
    }

    /**
     * Creates a new Kafka broker test harness using the given number of brokers and Zookeeper port.
     *
     * @param brokers Number of Kafka brokers to start up.
     * @param zookeeperPort The port number to use for Zookeeper client connections.
     *
     * @throws IllegalArgumentException if {@code brokers} is less than 1.
     */
    public KafkaBrokerTestHarness(int brokers, int zookeeperPort) {
        this(getBrokerConfig(brokers, zookeeperPort), zookeeperPort, "");
    }

    /**
     * Creates a new Kafka broker test harness using the given number of brokers and Zookeeper port.
     *
     * @param brokers
     *            Number of Kafka brokers to start up.
     * @param zookeeperPort
     *            The port number to use for Zookeeper client connections.
     * @param properties
     *            the additional {@link Properties} supplied to the brokers.
     *
     * @throws IllegalArgumentException
     *             if {@code brokers} is less than 1 or if {@code baseProperties} is {@code null}
     */
    public KafkaBrokerTestHarness(int brokers, int zookeeperPort, Properties properties) {
        this(getBrokerConfig(brokers, zookeeperPort, properties), zookeeperPort, properties.getProperty("cluster.id", ""));
    }

    /**
     * Creates a new Kafka broker test harness using the given broker configuration properties and Zookeeper port.
     *
     * @param brokerConfigs List of Kafka broker configurations.
     * @param zookeeperPort The port number to use for Zookeeper client connections.
     * @param clusterId the Kafka cluster id.
     *
     * @throws IllegalArgumentException if {@code brokerConfigs} is {@code null} or empty.
     */
    public KafkaBrokerTestHarness(final List<KafkaConfig> brokerConfigs, final int zookeeperPort, final String clusterId) {
        super(zookeeperPort);
        if (brokerConfigs == null || brokerConfigs.isEmpty()) {
            throw new IllegalArgumentException("Must supply at least one broker configuration.");
        }
        this.brokerConfigs = brokerConfigs;
        this.brokers = null;
        this.setUp = false;
        this.tornDown = false;
        this.clusterId = clusterId;
    }

    /**
     * Get the cluster id.
     *
     * @return the cluster id.
     */
    public String getClusterId() {
        return clusterId;
    }

    /**
     * Start up the Kafka broker cluster.
     *
     * @throws IOException if an error occurs during Kafka broker startup.
     * @throws IllegalStateException if the Kafka broker cluster has already been {@link #setUp() setup}.
     */
    @Override
    public void setUp() throws IOException {
        if (setUp) {
            throw new IllegalStateException("Already setup, cannot setup again");
        }
        setUp = true;

        // Start up zookeeper.
        super.setUp();

        startKafkaCluster();
    }

    /**
     * Shutdown the Kafka broker cluster. Attempting to {@link #setUp()} a cluster again after calling this method is not allowed;
     * a new {@code KafkaBrokerTestHarness} must be created instead.
     *
     * @throws IllegalStateException if the Kafka broker cluster has already been {@link #tearDown() torn down} or has not been
     *      {@link #setUp()}.
     * @throws IOException if an error occurs during Kafka broker shutdown.
     */
    @Override
    public void tearDown() throws IOException {
        if (!setUp) {
            throw new IllegalStateException("Not set up, cannot tear down");
        }
        if (tornDown) {
            throw new IllegalStateException("Already torn down, cannot tear down again");
        }
        tornDown = true;

        stopKafkaCluster();

        // Shutdown zookeeper
        super.tearDown();
    }

    /**
     * Start only the Kafka brokers and not the Zookeepers.
     *
     * @throws IllegalStateException if already started
     */
    public void startKafkaCluster() {
        if((brokers != null) && (!brokers.isEmpty()))
            throw new IllegalStateException("Kafka brokers are already running.");

        brokers = new ArrayList<>(brokerConfigs.size());
        for (KafkaConfig config : brokerConfigs) {
            brokers.add(startBroker(config));
        }
    }

    /**
     * Stop only the Kafka brokers not the Zookeepers.
     *
     * @throws IllegalStateException if already stopped
     * @throws IOException if an error occurs during Kafka broker shutdown.
     */
    public void stopKafkaCluster() throws IOException {

        if (brokers == null) {
            throw new IllegalStateException("Kafka brokers are already stopped.");
        }

        for (KafkaServer broker : brokers) {
            broker.shutdown();
        }

        for (KafkaServer broker : brokers) {
            for (String logDir : asJavaIterable(broker.config().logDirs())) {
                FileUtils.deleteDirectory(new File(logDir));
            }
        }

        brokers = null;
    }


    private String getBootstrapServers() {
        return brokerConfigs.stream()
                .map(i -> i.hostName() + ":" + i.port())
                .collect(Collectors.joining(","));
    }

    /**
     * Returns the configs for all brokers in the test cluster
     *
     * @return Broker Configs
     */
    public List<KafkaConfig> getBrokerConfigs() {
        return brokerConfigs;
    }

    /**
     * Returns properties for a Kafka producer.
     *
     * @return Producer properties.
     */
    public Properties getProducerProps() {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());

        return props;
    }

    /**
     * Returns properties for a Kafka consumer.
     *
     * @return Consumer properties.
     */
    public Properties getConsumerProps() {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());

        return props;
    }

    /**
     * Returns properties for either a Kafka producer or consumer.
     *
     * @return Combined producer and consumer properties.
     */
    public Properties getProps() {

        // Combine producer and consumer properties.
        Properties props = getProducerProps();
        props.putAll(getConsumerProps());

        // Add zookeeper connect which can be used by KafkaAdminClient or other older clients
        props.setProperty(ZKConfig.ZkConnectProp(), brokerConfigs.get(0).zkConnect());

        return props;
    }

    /**
     * Creates a collection of Kafka Broker configurations based on the number of brokers and zookeeper.
     * @param brokers the number of brokers to create configuration for.
     * @param zookeeperPort the zookeeper port for the brokers to connect to.
     * @return configuration for a collection of brokers.
     * @throws IllegalArgumentException if {@code brokers} is less than 1
     */
    public static List<KafkaConfig> getBrokerConfig(int brokers, int zookeeperPort) {
        return getBrokerConfig(brokers, zookeeperPort, new Properties());
    }

    /**
     * Creates a collection of Kafka Broker configurations based on the number of brokers and zookeeper.
     * @param brokers the number of brokers to create configuration for.
     * @param zookeeperPort the zookeeper port for the brokers to connect to.
     * @param properties properties that should be applied for each broker config.  These properties will be
     *                       honored in favor of any default properties.
     * @return configuration for a collection of brokers.
     * @throws IllegalArgumentException if {@code brokers} is less than 1 or {@code properties} is {@code null}.
     */
    public static List<KafkaConfig> getBrokerConfig(int brokers, int zookeeperPort, Properties properties) {
        if (brokers < 1) {
            throw new IllegalArgumentException("Invalid broker count: " + brokers);
        }
        if(properties == null){
            throw new IllegalArgumentException("The 'properties' cannot be 'null'.");
        }

        int ports[] = KafkaTestUtils.getPorts(brokers);

        List<KafkaConfig> configs = new ArrayList<>(brokers);
        for (int i = 0; i < brokers; ++i) {
            Properties props = new Properties();
            props.setProperty(KafkaConfig.ZkConnectProp(), "localhost:" + zookeeperPort);
            props.setProperty(KafkaConfig.BrokerIdProp(), String.valueOf(i + 1));
            props.setProperty(KafkaConfig.AdvertisedHostNameProp(), "localhost");
            props.setProperty(KafkaConfig.HostNameProp(), "localhost");
            props.setProperty(KafkaConfig.PortProp(), String.valueOf(ports[i]));
            props.setProperty(KafkaConfig.LogDirProp(), KafkaTestUtils.getTempDir().getAbsolutePath());
            props.setProperty(KafkaConfig.LogFlushIntervalMessagesProp(), String.valueOf(1));
            props.setProperty(KafkaConfig.AutoCreateTopicsEnableProp(), String.valueOf(false));
            props.setProperty(KafkaConfig.NumPartitionsProp(), String.valueOf(PARTITIONS_PER_TOPIC));
            props.setProperty(KafkaConfig.OffsetsTopicReplicationFactorProp(), String.valueOf(brokers));
            props.setProperty(KafkaConfig.DefaultReplicationFactorProp(), String.valueOf(brokers));
            props.setProperty(KafkaConfig.DeleteTopicEnableProp(), String.valueOf(true));
            props.setProperty(KafkaConfig.OffsetsTopicPartitionsProp(), String.valueOf(PARTITIONS_PER_TOPIC));
            props.setProperty(KafkaConfig.LogIndexSizeMaxBytesProp(), String.valueOf(1024 * 1024));
            props.setProperty(KafkaConfig.LogCleanerEnableProp(), String.valueOf(false));
            props.setProperty(KafkaConfig.GroupInitialRebalanceDelayMsProp(), String.valueOf(100));

            props.putAll(properties);

            configs.add(new KafkaConfig(props));
        }
        return configs;
    }

    private static KafkaServer startBroker(KafkaConfig config) {
        KafkaServer server = new KafkaServer(config, new SystemTime(), Option.empty(),
                new scala.collection.mutable.MutableList<>());
        server.startup();
        return server;
    }

    private static class SystemTime implements Time {
        @Override
        public long milliseconds() {
            return System.currentTimeMillis();
        }

        @Override
        public long nanoseconds() {
            return System.nanoTime();
        }

        @Override
        public void sleep(long ms) {
            try {
                Thread.sleep(ms);
            } catch (InterruptedException e) {
                // Ignore
            }
        }

        @Override
        public long hiResClockMs() {
            return TimeUnit.NANOSECONDS.toMillis(milliseconds());
        }
    }
}

