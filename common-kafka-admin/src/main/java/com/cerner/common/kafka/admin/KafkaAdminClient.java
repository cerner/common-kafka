package com.cerner.common.kafka.admin;

import kafka.admin.AdminClient;
import kafka.admin.AdminOperationException;
import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.common.TopicAlreadyMarkedForDeletionException;
import kafka.common.TopicAndPartition;
import kafka.security.auth.Acl;
import kafka.security.auth.Authorizer;
import kafka.security.auth.Resource;
import kafka.security.auth.SimpleAclAuthorizer;
import kafka.server.ConfigType;
import kafka.utils.VerifiableProperties;
import kafka.utils.ZKConfig;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.I0Itec.zkclient.exception.ZkException;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.GroupCoordinatorNotAvailableException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.Seq;
import scala.collection.immutable.Set.Set1;
import scala.collection.immutable.Set$;
import scala.collection.mutable.ListBuffer;

import java.io.Closeable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A client for administering a Kafka cluster
 *
 * <p>
 * This class is not thread safe.
 * </p>
 *
 * @author Bryan Baugher
 */
public class KafkaAdminClient implements Closeable {

    /**
     * Reference to the logger for this resource
     */
    private static final Logger LOG = LoggerFactory.getLogger(KafkaAdminClient.class);

    /**
     * Property used to control if zookeeper should be connected securely
     */
    public static final String ZOOKEEPER_SECURE = "zookeeper.secure";

    /**
     * Default value for {@link #ZOOKEEPER_SECURE}
     */
    public static final String DEFAULT_ZOOKEEPER_SECURE = Boolean.FALSE.toString();

    /**
     * Property used to control the maximum amount of time in ms to wait for any operation to complete
     */
    public static final String OPERATION_TIMEOUT_MS = "kafka.admin.operation.timeout";

    /**
     * The default value for {@link #OPERATION_TIMEOUT_MS} (30_000 ms or 30s)
     */
    public static final String DEFAULT_OPERATION_TIMEOUT_MS = String.valueOf(30_000);

    /**
     * Property used to control the amount of time in ms to sleep before verifying if an asynchronous Kafka operation
     * was successful
     */
    public static final String OPERATION_SLEEP_MS = "kafka.admin.operation.sleep";

    /**
     * The default value for {@link #OPERATION_SLEEP_MS} (50ms)
     */
    public static final String DEFAULT_OPERATION_SLEEP_MS = String.valueOf(50);

    /**
     * Zookeeper client used to talk to Kafka
     */
    protected final ZkUtils zkUtils;

    /**
     * The properties used to configure the client
     */
    private final Properties properties;

    /**
     * The time in ms to wait for any operation to complete
     */
    protected final long operationTimeout;

    /**
     * The amount of time to sleep before before verifying if an asynchronous Kafka operation was successful
     */
    protected final long operationSleep;

    /**
     * Authorization client to make ACL requests. Lazily created
     */
    private Authorizer authorizer = null;

    /**
     * Admin client from Kafka to look up information about consumer groups
     */
    private AdminClient adminClient = null;

    /**
     * Creates a Kafka admin client with the given properties
     *
     * @param properties
     *      the properties to use to connect to the Kafka cluster
     * @throws IllegalArgumentException
     * <ul>
     *     <li>if properties is {@code null}</li>
     *     <li>if the property for {@link ZKConfig#ZkConnectProp()} is not set</li>
     *     <li>if the property for {@link #OPERATION_TIMEOUT_MS} is not a number or less than zero</li>
     *     <li>if the property for {@link #OPERATION_SLEEP_MS} is not a number or less than zero</li>
     * </ul>
     *
     * @throws AdminOperationException
     *      if the admin client cannot successfully establish a connection
     */
    public KafkaAdminClient(Properties properties) {
        this(properties, getZkUtils(properties));
    }

    // Visible for testing
    KafkaAdminClient(Properties properties, ZkUtils zkUtils) {
        this.properties = properties;
        this.zkUtils = zkUtils;
        this.operationTimeout = parseLong(properties, OPERATION_TIMEOUT_MS, DEFAULT_OPERATION_TIMEOUT_MS);
        this.operationSleep = parseLong(properties, OPERATION_SLEEP_MS, DEFAULT_OPERATION_SLEEP_MS);

        if (operationTimeout < 0)
            throw new IllegalArgumentException("operationTimeout cannot be < 0");

        if (operationSleep < 0)
            throw new IllegalArgumentException("operationSleep cannot be < 0");
    }

    private static ZkUtils getZkUtils(Properties properties) {
        if (properties == null)
            throw new IllegalArgumentException("properties cannot be null");

        Tuple2<ZkClient, ZkConnection> tuple;
        try {
            ZKConfig zkConfig = new ZKConfig(new VerifiableProperties(properties));
            tuple = ZkUtils.createZkClientAndConnection(zkConfig.zkConnect(), zkConfig.zkSessionTimeoutMs(),
                    zkConfig.zkConnectionTimeoutMs());
        } catch (ZkException e) {
            throw new AdminOperationException("Unable to create admin connection", e);
        }

        boolean isSecure = Boolean.valueOf(properties.getProperty(ZOOKEEPER_SECURE, DEFAULT_ZOOKEEPER_SECURE));
        return new ZkUtils(tuple._1(), tuple._2(), isSecure);
    }

    private static long parseLong(Properties properties, String property, String defaultValue) {
        String value = properties.getProperty(property, defaultValue);
        try {
            return Long.parseLong(value);
        } catch(NumberFormatException e) {
            throw new IllegalArgumentException("Unable to parse property [" + property + "] with value [" + value +
                    "]. Expected long", e);
        }
    }

    /**
     * Returns the set of all topics in the Kafka cluster
     *
     * @return unmodifiable set of all topics in the Kafka cluster
     *
     * @throws AdminOperationException
     *      if there is an issue retrieving the set of all topics
     */
    public Set<String> getTopics() {
        LOG.debug("Retrieving all topics");
        try {
            return Collections.unmodifiableSet(convertToJavaSet(zkUtils.getAllTopics().iterator()));
        } catch (ZkException e) {
            throw new AdminOperationException("Unable to retrieve all topics", e);
        }
    }

    /**
     * Returns the set of all partitions in the Kafka cluster
     *
     * @return unmodifiable set of all partitions in the Kafka cluster
     * @throws AdminOperationException
     *      if there is an issue reading partitions from Kafka
     */
    public Set<TopicAndPartition> getPartitions() {
        LOG.debug("Retrieving all partitions");

        long start = System.currentTimeMillis();
        do {
            // The zkUtils.getAllPartitions(..) can throw ZkNoNodeException if a topic's partitions have not been added to
            // zookeeper but the topic is listed in zookeeper. Any other zookeeper exception is assumed to be non-transient and
            // will be rethrown.
            try {
                return Collections.unmodifiableSet(convertToJavaSet(zkUtils.getAllPartitions().iterator()));
            } catch (ZkNoNodeException e) {
                LOG.debug("Reading partitions had an error", e);
            } catch (ZkException e) {
                throw new AdminOperationException("Unable to retrieve all partitions", e);
            }

            LOG.debug("Sleeping for {} ms before trying to get all partitions again", operationSleep);
            try {
                Thread.sleep(operationSleep);
            } catch (InterruptedException e) {
                throw new AdminOperationException("Interrupted while getting partitions", e);
            }

        } while (!operationTimedOut(start));

        throw new AdminOperationException("Operation timed out trying to get partitions");
    }

    /**
     * Returns the set of all partitions for the given topic in the Kafka cluster
     *
     * @param topic
     *      a Kafka topic
     * @return unmodifiable set of all partitions for the given topic in the Kafka cluster
     * @throws AdminOperationException
     *      if there is an issue reading partitions from Kafka
     */
    public Set<TopicAndPartition> getPartitions(String topic) {
        LOG.debug("Retrieving all partitions for topic [{}]", topic);
        return Collections.unmodifiableSet(
                getPartitions().stream().filter(p -> p.topic().equals(topic)).collect(Collectors.toSet()));
    }

    /**
     * Returns an {@link Authorizer} to make {@link Acl} requests
     *
     * @return an {@link Authorizer} to make {@link Acl} requests
     *
     * @throws AdminOperationException
     *      if there is an issue creating the authorizer
     */
    public Authorizer getAuthorizer() {
        if (authorizer == null) {
            ZKConfig zkConfig = new ZKConfig(new VerifiableProperties(properties));

            Map<String, Object> authorizerProps = new HashMap<>();
            authorizerProps.put(ZKConfig.ZkConnectProp(), zkConfig.zkConnect());
            authorizerProps.put(ZKConfig.ZkConnectionTimeoutMsProp(), zkConfig.zkConnectionTimeoutMs());
            authorizerProps.put(ZKConfig.ZkSessionTimeoutMsProp(), zkConfig.zkSessionTimeoutMs());
            authorizerProps.put(ZKConfig.ZkSyncTimeMsProp(), zkConfig.zkSyncTimeMs());

            try {
                Authorizer simpleAclAuthorizer = new SimpleAclAuthorizer();
                simpleAclAuthorizer.configure(authorizerProps);
                authorizer = simpleAclAuthorizer;
            } catch (ZkException e) {
                throw new AdminOperationException("Unable to create authorizer", e);
            }
        }

        return authorizer;
    }

    /**
     * Returns all {@link Acl}s defined in the Kafka cluster
     *
     * @return unmodifiable map of all {@link Acl}s defined in the Kafka cluster
     *
     * @throws AdminOperationException
     *      if there is an issue reading the {@link Acl}s
     */
    public Map<Resource, Set<Acl>> getAcls() {
        LOG.debug("Fetching all ACLs");
        try {
            return convertKafkaAclMap(getAuthorizer().getAcls());
        } catch (ZkException e) {
            throw new AdminOperationException("Unable to retrieve all ACLs", e);
        }
    }

    /**
     * Returns all {@link Acl}s associated to the given {@link KafkaPrincipal}
     *
     * @param principal
     *      the {@link KafkaPrincipal} to look up {@link Acl}s for
     * @return unmodifiable map of all {@link Acl}s associated to the given {@link KafkaPrincipal}
     * @throws IllegalArgumentException
     *      if principal is {@code null}
     * @throws AdminOperationException
     *      if there is an issue reading the {@link Acl}s
     */
    public Map<Resource, Set<Acl>> getAcls(KafkaPrincipal principal) {
        if (principal == null)
            throw new IllegalArgumentException("principal cannot be null");

        LOG.debug("Fetching all ACLs for principal [{}]", principal);

        try {
            return convertKafkaAclMap(getAuthorizer().getAcls(principal));
        } catch (ZkException e) {
            throw new AdminOperationException("Unable to retrieve ACLs for principal: " + principal, e);
        }
    }

    private static Map<Resource, Set<Acl>> convertKafkaAclMap(scala.collection.immutable.Map<Resource,
            scala.collection.immutable.Set<Acl>> aclMap) {
        return Collections.unmodifiableMap(convertToJavaMap(aclMap.iterator()).entrySet().stream()
                .collect(Collectors.toMap(e -> e.getKey(), e -> convertToJavaSet(e.getValue().iterator()))));
    }

    /**
     * Returns all {@link Acl}s associated to the given {@link Resource}
     *
     * @param resource
     *      the {@link Resource} to look up {@link Acl}s for
     * @return unmodifiable set of all {@link Acl}s associated to the given {@link Resource}
     * @throws IllegalArgumentException
     *      if resource is {@code null}
     * @throws AdminOperationException
     *      if there is an issue reading the {@link Acl}s
     */
    public Set<Acl> getAcls(Resource resource) {
        if (resource == null)
            throw new IllegalArgumentException("resource cannot be null");

        LOG.debug("Fetching all ACLs for resource [{}]", resource);

        try {
            return Collections.unmodifiableSet(convertToJavaSet(getAuthorizer().getAcls(resource).iterator()));
        } catch (ZkException e) {
            throw new AdminOperationException("Unable to retrieve ACLs for resource: " + resource, e);
        }
    }

    /**
     * Adds the given {@link Acl}s to the {@link Resource}
     *
     * @param acls
     *      the {@link Acl}s to add
     * @param resource
     *      the {@link Resource} to add the {@link Acl}s to
     * @throws IllegalArgumentException
     *      if acls or resource is {@code null}
     * @throws AdminOperationException
     *      if there is an issue adding the {@link Acl}s
     */
    public void addAcls(Set<Acl> acls, Resource resource) {
        if (acls == null)
            throw new IllegalArgumentException("acls cannot be null");
        if (resource == null)
            throw new IllegalArgumentException("resource cannot be null");

        LOG.debug("Adding ACLs [{}] for resource [{}]", acls, resource);

        try {
            getAuthorizer().addAcls(toImmutableScalaSet(acls), resource);
        } catch (ZkException | IllegalStateException e) {
            throw new AdminOperationException("Unable to add ACLs for resource: " + resource, e);
        }
    }

    /**
     * Removes the given {@link Acl}s from the {@link Resource}
     *
     * @param acls
     *      the {@link Acl}s to remove
     * @param resource
     *      the {@link Resource} to remove the {@link Acl}s from
     * @throws IllegalArgumentException
     *      if acls or resource is {@code null}
     * @throws AdminOperationException
     *      if there is an issue removing the {@link Acl}s
     */
    public void removeAcls(Set<Acl> acls, Resource resource) {
        if (acls == null)
            throw new IllegalArgumentException("acls cannot be null");
        if (resource == null)
            throw new IllegalArgumentException("resource cannot be null");

        LOG.debug("Removing ACLs [{}] for resource [{}]", acls, resource);

        try {
            getAuthorizer().removeAcls(toImmutableScalaSet(acls), resource);
        } catch (ZkException e) {
            throw new AdminOperationException("Unable to remove ACLs for resource: " + resource, e);
        }
    }

    /**
     * Creates a topic with no config and blocks until a leader is elected for each partition
     *
     * @param topic
     *      the name of the topic
     * @param partitions
     *      the number of partitions
     * @param replicationFactor
     *      the replication factor
     * @throws IllegalArgumentException
     *      <ul>
     *          <li>If topic is {@code null}</li>
     *          <li>If partitions is less than 1</li>
     *          <li>If replicationFactor is less than 1</li>
     *      </ul>
     * @throws org.apache.kafka.common.errors.TopicExistsException
     *      if the topic already exists
     * @throws org.apache.kafka.common.errors.InvalidReplicationFactorException
     *      if the replication factor is larger than number of available brokers
     * @throws org.apache.kafka.common.errors.InvalidTopicException
     *      if the topic name contains illegal characters
     * @throws AdminOperationException
     *      if the operation times out waiting for the topic to be created with leaders elected for all partitions, or there is
     *      any other issue creating the topic
     */
    public void createTopic(String topic, int partitions, int replicationFactor) {
        createTopic(topic, partitions, replicationFactor, new Properties());
    }

    /**
     * Creates a topic and blocks until a leader is elected for each partition
     *
     * @param topic
     *      the name of the topic
     * @param partitions
     *      the number of partitions
     * @param replicationFactor
     *      the replication factor
     * @param topicConfig
     *      the config for the topic
     * @throws IllegalArgumentException
     *      <ul>
     *          <li>If topic is {@code null}</li>
     *          <li>If partitions is less than 1</li>
     *          <li>If replicationFactor is less than 1</li>
     *          <li>if topicConfig is {@code null}</li>
     *      </ul>
     * @throws org.apache.kafka.common.errors.TopicExistsException
     *      if the topic already exists
     * @throws org.apache.kafka.common.errors.InvalidReplicationFactorException
     *      if the replication factor is larger than number of available brokers
     * @throws org.apache.kafka.common.errors.InvalidTopicException
     *      if the topic name contains illegal characters
     * @throws AdminOperationException
     *      if the operation times out waiting for the topic to be created with leaders elected for all partitions, or there is
     *      any other issue creating the topic
     */
    public void createTopic(String topic, int partitions, int replicationFactor, Properties topicConfig) {
        if (topic == null)
            throw new IllegalArgumentException("topic cannot be null");
        if (partitions < 1)
            throw new IllegalArgumentException("partitions cannot be < 1");
        if (replicationFactor < 1)
            throw new IllegalArgumentException("replicationFactor cannot be < 1");
        if (topicConfig == null)
            throw new IllegalArgumentException("topicConfig cannot be null");

        LOG.debug("Creating topic [{}] with partitions [{}] and replication factor [{}] and topic config [{}]",
                 topic, partitions, replicationFactor, topicConfig );

        try {
            AdminUtils.createTopic(zkUtils, topic, partitions, replicationFactor, topicConfig, RackAwareMode.Disabled$.MODULE$);
        } catch (ZkException e) {
            throw new AdminOperationException("Unable to create topic: " + topic, e);
        }

        long start = System.currentTimeMillis();
        boolean operationCompleted = false;

        do {
            LOG.debug("Sleeping for {} ms for create topic operation to complete for topic [{}]", operationSleep, topic);
            try {
                Thread.sleep(operationSleep);
            } catch (InterruptedException e) {
                throw new AdminOperationException("Interrupted waiting for topic " + topic + " to be created", e);
            }
            operationCompleted = topicExists(topic) && topicHasPartitions(topic, partitions);
        } while (!operationCompleted && !operationTimedOut(start));

        if (!operationCompleted)
            throw new AdminOperationException("Timeout waiting for topic " + topic + " to be created");
    }

    /**
     * Delete the given topic if it exists
     *
     * @param topic
     *      the topic to delete
     * @throws IllegalArgumentException
     *      if topic is null, empty or blank
     * @throws AdminOperationException
     *      if the operation times out before deleting the topic, or there is any other issue deleting the topic (no exception is
     *      thrown if the topic does not exist)
     */
    public void deleteTopic(String topic) {
        if (StringUtils.isBlank(topic))
            throw new IllegalArgumentException("topic cannot be null, empty or blank");

        LOG.debug("Deleting topic [{}]", topic);

        try {
            AdminUtils.deleteTopic(zkUtils, topic);

            long start = System.currentTimeMillis();
            boolean operationCompleted = false;

            do {
                LOG.debug("Sleeping for {} ms for delete topic operation to complete for topic [{}]", operationSleep, topic);
                try {
                    Thread.sleep(operationSleep);
                } catch (InterruptedException e) {
                    throw new AdminOperationException("Interrupted waiting for topic " + topic + " to be deleted", e);
                }
                operationCompleted = !topicExists(topic);
            } while (!operationCompleted && !operationTimedOut(start));

            if (!operationCompleted)
                throw new AdminOperationException("Timeout waiting for topic " + topic + " to be deleted");

        } catch (TopicAlreadyMarkedForDeletionException e) {
            LOG.warn("Topic [{}] is already marked for deletion", topic, e);
        } catch (UnknownTopicOrPartitionException e) {
            LOG.warn("Topic [{}] to be deleted was not found", topic, e);
        } catch (ZkException e) {
            throw new AdminOperationException("Unable to delete topic: " + topic, e);
        }
    }

    /**
     * Returns the {@link Properties} associated to the topic
     *
     * @param topic
     *      a Kafka topic
     * @return the {@link Properties} associated to the topic
     * @throws IllegalArgumentException
     *      if topic is null, empty or blank
     * @throws AdminOperationException
     *      if there is an issue reading the topic config
     */
    public Properties getTopicConfig(String topic) {
        if (StringUtils.isBlank(topic))
            throw new IllegalArgumentException("topic cannot be null, empty or blank");

        LOG.debug("Fetching topic config for topic [{}]", topic);

        try {
            return AdminUtils.fetchEntityConfig(zkUtils, ConfigType.Topic(), topic);
        } catch (ZkException | KafkaException | IllegalArgumentException e) {
            throw new AdminOperationException("Unable to retrieve configuration for topic: " + topic, e);
        }
    }

    /**
     * Updates the given topic's config with the {@link Properties} provided. This is not additive but a full
     * replacement
     *
     * @param topic
     *      the topic to update config for
     * @param properties
     *      the properties to assign to the topic
     * @throws IllegalArgumentException
     *      if topic is null, empty or blank, or properties is {@code null}
     * @throws AdminOperationException
     *      if there is an issue updating the topic config
     */
    public void updateTopicConfig(String topic, Properties properties) {
        if (StringUtils.isBlank(topic))
            throw new IllegalArgumentException("topic cannot be null, empty or blank");
        if (properties == null)
            throw new IllegalArgumentException("properties cannot be null");

        LOG.debug("Updating topic config for topic [{}] with config [{}]", topic, properties);

        try {
            AdminUtils.changeTopicConfig(zkUtils, topic, properties);
        } catch (ZkException e) {
            throw new AdminOperationException("Unable to update configuration for topic: " + topic, e);
        }
    }

    /**
     * Returns the replication factor for the given topic
     *
     * @param topic
     *      a Kafka topic
     * @return the replication factor for the given topic
     *
     * @throws IllegalArgumentException
     *      if topic is null, empty or blank
     * @throws AdminOperationException
     *      if there is an issue retrieving the replication factor
     */
    public int getTopicReplicationFactor(String topic) {
        if (StringUtils.isBlank(topic))
            throw new IllegalArgumentException("topic cannot be null, empty or blank");

        try {
            return convertToJavaSet(zkUtils.getReplicasForPartition(topic, 0).iterator()).size();
        } catch (ZkException | KafkaException e) {
            throw new AdminOperationException("Unable to read replication factor for topic: " + topic, e);
        }
    }

    /**
     * Returns the number of partitions for the given topic
     *
     * @param topic
     *      a Kafka topic
     * @return the number of partitions for the given topic
     * @throws IllegalArgumentException
     *      if topic is null, empty or blank
     * @throws AdminOperationException
     *      if there is an issue looking up the partitions for the topic
     */
    public int getTopicPartitions(String topic) {
        if (StringUtils.isBlank(topic))
            throw new IllegalArgumentException("topic cannot be null, empty or blank");

        LOG.debug("Fetching topic partitions for topic [{}]", topic);

        Map<String, Seq<Object>> javaMap;
        try {
            javaMap = convertToJavaMap(zkUtils.getPartitionsForTopics(new Set1<String>(topic).toSeq()).iterator());
        } catch (ZkException | KafkaException e) {
            throw new AdminOperationException("Unable to retrieve number of partitions for topic: " + topic, e);
        }

        Seq<Object> partitions = javaMap.get(topic);
        if (partitions == null ) {
            throw new AdminOperationException("Failed to find any partitions for topic: " + topic);
        }

        Set<Object> partitionList = convertToJavaSet(partitions.iterator());
        if (partitionList.isEmpty()) {
            throw new AdminOperationException("Partition count is 0 for topic: " + topic);
        }

        return partitionList.size();
    }

    /**
     * Adds partitions to the given topic
     *
     * @param topic
     *      the topic to add partitions to
     * @param partitions
     *      the number of partitions the topic should have
     * @throws IllegalArgumentException
     *      if topic is null, empty or blank, or partitions is less than or equal to 1
     * @throws AdminOperationException
     *      if the number of partitions is less than or equal to the topic's current partition count, the operation times
     *      out while waiting for partitions to be added, or any other issue occurs adding the partitions
     */
    public void addTopicPartitions(String topic, int partitions) {
        if (StringUtils.isBlank(topic))
            throw new IllegalArgumentException("topic cannot be null, empty or blank");
        if (partitions <= 1)
            throw new IllegalArgumentException("partitions cannot be <= 1");

        LOG.debug("Adding topic partitions for topic [{}] with partitions [{}]", topic, partitions);

        try {
            // Argument 4 is replica assignment. We pass "" to tell Kafka to come up with its own assignments for the new
            // partitions
            // Argument 5 is to check if the assigned broker replica is available
            AdminUtils.addPartitions(zkUtils, topic, partitions, "", true, RackAwareMode.Enforced$.MODULE$);
        } catch (ZkException e) {
            throw new AdminOperationException("Unable to add partitions to topic: " + topic, e);
        }

        long start = System.currentTimeMillis();
        boolean operationCompleted = false;

        do {
            LOG.debug("Sleeping for {} ms for add topic partition operation to complete for topic [{}]", operationSleep,
                    topic);
            try {
                Thread.sleep(operationSleep);
            } catch (InterruptedException e) {
                throw new AdminOperationException("Interrupted waiting for partitions to be added to topic: " + topic, e);
            }
            operationCompleted = topicHasPartitions(topic, partitions);
        } while (!operationCompleted && !operationTimedOut(start));

        if (!operationCompleted)
            throw new AdminOperationException("Timeout waiting for partitions to be added to topic: " + topic);
    }

    /**
     * Retrieves the {@link AdminClient.ConsumerGroupSummary} information from Kafka
     *
     * @param consumerGroup
     *      the name of the consumer group
     * @return the {@link AdminClient.ConsumerGroupSummary} information from Kafka
     * @throws AdminOperationException
     *      if there is an issue retrieving the consumer group summary
     */
    public AdminClient.ConsumerGroupSummary getConsumerGroupSummary(String consumerGroup) {
        if (StringUtils.isBlank(consumerGroup))
            throw new IllegalArgumentException("consumerGroup cannot be null, empty or blank");

        try {
            return getAdminClient().describeConsumerGroup(consumerGroup);
        } catch (KafkaException e) {
            throw new AdminOperationException("Unable to retrieve summary for consumer group: " + consumerGroup, e);
        }
    }

    /**
     * Returns the collection of consumer summaries about the consumers in the group or empty collection if the group does not exist
     * or is not active
     *
     * @param consumerGroup
     *      the name of the consumer group
     * @return unmodifiable collection of consumer summaries about the consumers in the group or empty collection if the group does
     *      not exist or is not active
     * @throws IllegalArgumentException
     *      if the consumerGroup is null, empty or blank
     * @throws AdminOperationException
     *      if an issue occurs retrieving the summaries
     */
    public Collection<AdminClient.ConsumerSummary> getConsumerGroupSummaries(String consumerGroup) {
        if (StringUtils.isBlank(consumerGroup))
            throw new IllegalArgumentException("consumerGroup cannot be null, empty or blank");

        AdminClient.ConsumerGroupSummary summary;

        try {
            // this will throw IAE if the consumer group is dead/empty and GroupCoordinatorNotAvailableException if the group is
            // re-balancing / initializing

            // We can't call the getConsumerGroupSummary(..) above as it would wrap the GroupCoordinatorNotAvailableException and
            // we wouldn't handle it properly here
            summary = getAdminClient().describeConsumerGroup(consumerGroup);
        } catch (IllegalArgumentException | GroupCoordinatorNotAvailableException e) {
            LOG.debug("Error while attempting to describe consumer group {}", consumerGroup, e);
            return Collections.emptyList();
        } catch (KafkaException e) {
            throw new AdminOperationException("Unable to retrieve summaries for consumer group: " + consumerGroup, e);
        }

        return Collections.unmodifiableCollection(convertToJavaSet(summary.consumers().get().iterator()));
    }

    /**
     * Returns the consumer group assignments of partitions to client IDs or empty map if the group does not exist or is not active
     *
     * @param consumerGroup
     *      the name of the consumer group
     * @return unmodifiable map of the consumer group assignments of partitions to client IDs or empty map if the group does not
     *      exist or is not active
     * @throws IllegalArgumentException
     *      if the consumerGroup is null, empty or blank
     * @throws AdminOperationException
     *      if an issue occurs retrieving the assignments
     */
    public Map<TopicPartition, String> getConsumerGroupAssignments(String consumerGroup) {
        if (StringUtils.isBlank(consumerGroup))
            throw new IllegalArgumentException("consumerGroup cannot be null, empty or blank");

        Map<TopicPartition, String> assignments = new HashMap<>();

        Collection<AdminClient.ConsumerSummary> summaries = getConsumerGroupSummaries(consumerGroup);

        for (final AdminClient.ConsumerSummary consumerSummary : summaries) {
            Set<TopicPartition> topicPartitions = convertToJavaSet(consumerSummary.assignment().iterator());

            for (final TopicPartition topicPartition : topicPartitions) {
                assignments.put(topicPartition, consumerSummary.clientId());
            }
        }

        return Collections.unmodifiableMap(assignments);
    }

    private AdminClient getAdminClient() {
        if (adminClient == null)
            adminClient = AdminClient.create(properties);

        return adminClient;
    }

    @Override
    public void close() {
        zkUtils.close();

        if (authorizer != null)
            authorizer.close();

        if (adminClient != null)
            adminClient.close();
    }

    private boolean operationTimedOut(long start) {
        return System.currentTimeMillis() - start >= operationTimeout;
    }

    private boolean topicExists(String topic) {
        return getTopics().contains(topic);
    }

    private boolean topicHasPartitions(String topic, int partitions) {
        return getPartitions(topic).size() == partitions;
    }

    /**
     * Manually converting to scala set to avoid binary compatibility issues between scala versions when using JavaConverters
     */
    @SuppressWarnings("unchecked")
    static <T> scala.collection.immutable.Set<T> toImmutableScalaSet(Set<T> set) {
        ListBuffer<T> buffer = new ListBuffer<>();
        set.forEach((e) -> buffer.$plus$eq(e));
        return Set$.<T> MODULE$.apply(buffer);
    }

    /**
     * Manually converting to java set to avoid binary compatibility issues between scala versions when using JavaConverters
     */
    static <E> Set<E> convertToJavaSet(Iterator<E> iterator) {
        Set<E> set = new HashSet<>();
        while(iterator.hasNext()) {
            set.add(iterator.next());
        }
        return Collections.unmodifiableSet(set);
    }

    /**
     * Manually converting to java map to avoid binary compatibility issues between scala versions when using JavaConverters
     */
    static <K, V> Map<K, V> convertToJavaMap(Iterator<Tuple2<K, V>> mapIterator) {
        Map<K, V> map = new HashMap<>();
        while(mapIterator.hasNext()) {
            Tuple2<K, V> entry = mapIterator.next();
            map.put(entry.copy$default$1(), entry.copy$default$2());
        }
        return Collections.unmodifiableMap(map);
    }
}
