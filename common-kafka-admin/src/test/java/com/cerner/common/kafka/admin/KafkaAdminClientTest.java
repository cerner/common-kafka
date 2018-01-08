package com.cerner.common.kafka.admin;

import static com.cerner.common.kafka.admin.KafkaAdminClient.convertToJavaSet;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.cerner.common.kafka.testing.ZkStringSerializer;
import com.cerner.common.kafka.testing.ZookeeperTestHarness;
import kafka.admin.AdminClient;
import kafka.admin.AdminOperationException;
import kafka.security.auth.Topic;
import kafka.utils.ZKConfig;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import com.cerner.common.kafka.KafkaTests;
import com.fasterxml.jackson.databind.ObjectMapper;

import kafka.common.TopicAndPartition;
import kafka.log.LogConfig;
import kafka.security.auth.Acl;
import kafka.security.auth.Allow$;
import kafka.security.auth.Read$;
import kafka.security.auth.Resource;
import kafka.server.ConfigType;
import kafka.utils.ZkUtils;

public class KafkaAdminClientTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Rule
    public TestName testName = new TestName();
    Properties properties;
    String topic;
    KafkaAdminClient client;

    static KafkaAdminClient failureClient;

    @BeforeClass
    public static void startup() throws Exception {
        KafkaTests.startTest();

        // Create and start a temporary secondary ZK for testing failures.
        ZookeeperTestHarness zk = new ZookeeperTestHarness();
        zk.setUp();

        // Need to avoid Kafka's default ZK client behavior which retries forever. Also reduce the connection timeout for faster
        // tests.
        String zkConnect = zk.getZkUtils().zkConnection().getServers();
        int zkRetryMs = 0;
        int zkConnectTimeoutMs = 500;

        Properties props = new Properties();
        props.setProperty(ZKConfig.ZkConnectProp(), zkConnect);
        props.setProperty(ZKConfig.ZkConnectionTimeoutMsProp(), String.valueOf(zkConnectTimeoutMs));

        ZkConnection zkConnection = new ZkConnection(zkConnect);
        ZkClient zkClient = new ZkClient(zkConnection, zkConnectTimeoutMs, new ZkStringSerializer(), zkRetryMs);

        // Establish a connection while the temporary ZK is still running.
        failureClient = new KafkaAdminClient(props, new ZkUtils(zkClient, zkConnection, false));

        // Kill the temporary ZK, we no longer need it.
        zk.tearDown();
    }

    @AfterClass
    public static void shutdown() throws Exception {
        KafkaTests.endTest();

        failureClient.close();
    }

    @Before
    public void before() {
        properties = new Properties();
        properties.putAll(KafkaTests.getProps());

        client = new KafkaAdminClient(properties);

        topic = "KafkaAdminClient-" + testName.getMethodName();
    }

    @After
    public void after() {
        // Remove all ACLs
        client.getAcls().keySet().forEach(resource -> client.getAuthorizer().removeAcls(resource));

        client.close();
    }

    @Test(expected = AdminOperationException.class)
    public void constructor_zkException() {
        Properties props = new Properties();
        props.setProperty(ZKConfig.ZkConnectProp(), "some_invalid_host:7777");
        new KafkaAdminClient(props);
    }

    @Test(expected = IllegalArgumentException.class)
    public void constructorProperties_nullProperties() {
        new KafkaAdminClient(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void constructorProperties_operationTimeoutNotANumber() {
        properties.setProperty(KafkaAdminClient.OPERATION_TIMEOUT_MS, "notANumber");
        new KafkaAdminClient(properties);
    }

    @Test(expected = IllegalArgumentException.class)
    public void constructorProperties_zkConnectNotSet() {
        new KafkaAdminClient(new Properties());
    }

    @Test(expected = IllegalArgumentException.class)
    public void constructorProperties_operationTimeoutLessThanZero() {
        properties.setProperty(KafkaAdminClient.OPERATION_TIMEOUT_MS, "-1");
        new KafkaAdminClient(properties);
    }
    @Test
    public void constructorProperties_defaultConfig() {
        assertThat(client.operationTimeout, is(Long.parseLong(KafkaAdminClient.DEFAULT_OPERATION_TIMEOUT_MS)));
        assertThat(client.operationSleep, is(Long.parseLong(KafkaAdminClient.DEFAULT_OPERATION_SLEEP_MS)));
        assertThat(client.zkUtils.isSecure(), is(false));
    }

    @Test
    public void constructorProperties_alternateConfig() {
        properties.setProperty(KafkaAdminClient.OPERATION_TIMEOUT_MS, "123");
        properties.setProperty(KafkaAdminClient.OPERATION_SLEEP_MS, "321");
        properties.setProperty(KafkaAdminClient.ZOOKEEPER_SECURE, "true");

        try (KafkaAdminClient adminClient = new KafkaAdminClient(properties)) {
            assertThat(adminClient.operationTimeout, is(123L));
            assertThat(adminClient.operationSleep, is(321L));
            assertThat(adminClient.zkUtils.isSecure(), is(true));
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void createTopic_nullTopic() {
        client.createTopic(null, 1, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void createTopic_partitionsLessThanOne() {
        client.createTopic(topic, 0, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void createTopic_replicationFactorLessThanOne() {
        client.createTopic(topic, 1, 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void createTopic_topicConfigIsNull() {
        client.createTopic(topic, 1, 1, null);
    }

    @Test(expected = TopicExistsException.class)
    public void createTopic_topicExists() {
        client.createTopic(topic, 1, 1);
        client.createTopic(topic, 1, 1);
    }

    @Test
    public void createTopic_noConfig() {
        client.createTopic(topic, 1, 1);
        assertThat(client.getPartitions(), hasItem(new TopicAndPartition(topic, 0)));
    }

    @Test
    public void createTopic_multiplePartitions() {
        client.createTopic(topic, 3, 1);

        assertThat(client.getPartitions(), hasItems(
                new TopicAndPartition(topic, 0),
                new TopicAndPartition(topic, 1),
                new TopicAndPartition(topic, 2)
        ));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void createTopic_withConfig() throws IOException {
        Properties topicConfig = new Properties();
        topicConfig.setProperty(LogConfig.CleanupPolicyProp(), "delete");

        client.createTopic(topic, 1, 1, topicConfig);

        assertThat(client.getPartitions(), hasItem(new TopicAndPartition(topic, 0)));

        Map<Object, Object> zkTopicConfig = OBJECT_MAPPER.readValue((String) KafkaTests.getZkUtils().zkClient()
                .readData(ZkUtils.getEntityConfigPath(ConfigType.Topic(), topic)), Map.class);
        assertThat(((Map<Object, Object>) zkTopicConfig.get("config")).get(LogConfig.CleanupPolicyProp()), is("delete"));
    }

    @Test(expected = InvalidReplicationFactorException.class)
    public void createTopic_replicationFactorMoreThanAvailableBrokers() {
        client.createTopic(topic, 1, 2);
    }

    @Test(expected = InvalidTopicException.class)
    public void createTopic_invalidTopicName() {
        client.createTopic("I'm not a valid topic!", 1, 1);
    }

    @Test(expected = AdminOperationException.class)
    public void createTopic_zkException() {
        failureClient.createTopic(topic, 1, 1);
    }

    @Test
    public void getTopics() {
        client.createTopic(topic, 1, 1);
        assertThat(client.getTopics(), hasItem(topic));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void getTopics_immutable() {
        client.createTopic(topic, 1, 1);
        client.getTopics().clear();
    }

    @Test(expected = AdminOperationException.class)
    public void getTopics_zkException() {
        failureClient.getTopics();
    }

    @Test
    public void getPartitions() {
        String topic1 = topic + "-1";
        String topic2 = topic + "-2";
        client.createTopic(topic1, 1, 1);
        client.createTopic(topic2, 1, 1);

        assertThat(client.getPartitions(), hasItems(new TopicAndPartition(topic1, 0),
                new TopicAndPartition(topic2, 0)));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void getPartitions_immutable() {
        client.createTopic(topic, 1, 1);
        client.getPartitions().clear();
    }

    @Test
    public void getPartitionsManyPartitions() {
        int partitionsPerTopic = 25;
        List<TopicAndPartition> partitions = new ArrayList<>();
        for (int i = 0; i < partitionsPerTopic; ++i) {
            partitions.add(new TopicAndPartition(topic, i));
        }

        client.createTopic(topic, partitionsPerTopic, 1);
        assertThat(client.getPartitions(), hasItems(partitions.toArray(new TopicAndPartition[partitions.size()])));
    }

    @Test
    public void getPartitionsForTopic() {
        String topic1 = topic + "-1";
        String topic2 = topic + "-2";
        int partitionsPerTopic = 10;
        client.createTopic(topic1, partitionsPerTopic, 1);
        client.createTopic(topic2, partitionsPerTopic, 1);

        Set<TopicAndPartition> partitions = client.getPartitions(topic1);
        assertThat(partitions.size(), is(partitionsPerTopic));

        for (int i = 0; i < partitionsPerTopic; ++i) {
            assertThat(partitions, hasItem(new TopicAndPartition(topic1, i)));
            assertThat(partitions, not(hasItem(new TopicAndPartition(topic2, i))));
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    public void getPartitionsForTopic_immutable() {
        client.createTopic(topic, 1, 1);
        client.getPartitions(topic).clear();
    }

    @Test
    public void getAuthorizer() {
        assertThat(client.getAuthorizer(), is(not(nullValue())));
    }

    @Test(expected = AdminOperationException.class)
    public void getAuthorizer_zkException() {
        failureClient.getAuthorizer();
    }

    @Test
    public void getAcls() {
        KafkaPrincipal user = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "my_user");
        Resource topic1 = Resource.fromString(Topic.name() + Resource.Separator() + "topic1");
        Resource topic2 = Resource.fromString(Topic.name() + Resource.Separator() + "topic2");

        Set<Acl> readAcl = Collections.singleton(new Acl(user, Allow$.MODULE$, Acl.WildCardHost(), Read$.MODULE$));

        client.addAcls(readAcl, topic1);
        client.addAcls(readAcl, topic2);

        Map<Resource, Set<Acl>> allAcls = new HashMap<>();
        allAcls.put(topic1, readAcl);
        allAcls.put(topic2, readAcl);

        assertThat(client.getAcls(), is(allAcls));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void getAcls_immutable() {
        KafkaPrincipal user = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "my_user");
        Resource topic = Resource.fromString(Topic.name() + Resource.Separator() + "topic");

        Set<Acl> readAcl = Collections.singleton(new Acl(user, Allow$.MODULE$, Acl.WildCardHost(), Read$.MODULE$));
        client.addAcls(readAcl, topic);
        client.getAcls().clear();
    }

    @Test(expected = AdminOperationException.class)
    public void getAcls_zkException() {
        failureClient.getAcls();
    }

    @Test
    public void getAcls_withKafkaPrincipal() {
        KafkaPrincipal user1 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "user1");
        KafkaPrincipal user2 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "user2");
        Resource topic1 = Resource.fromString(Topic.name() + Resource.Separator() + "topic1");

        Set<Acl> user1Acl = Collections.singleton(new Acl(user1, Allow$.MODULE$, Acl.WildCardHost(), Read$.MODULE$));
        Set<Acl> user2Acl = Collections.singleton(new Acl(user2, Allow$.MODULE$, Acl.WildCardHost(), Read$.MODULE$));

        client.addAcls(user1Acl, topic1);
        client.addAcls(user2Acl, topic1);

        assertThat(client.getAcls(user1), is(Collections.singletonMap(topic1, user1Acl)));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void getAcls_withKafkaPrincipal_immutable() {
        KafkaPrincipal user = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "user");
        Resource topic = Resource.fromString(Topic.name() + Resource.Separator() + "topic");

        Set<Acl> userAcl = Collections.singleton(new Acl(user, Allow$.MODULE$, Acl.WildCardHost(), Read$.MODULE$));
        client.addAcls(userAcl, topic);
        client.getAcls(user).clear();
    }

    @Test (expected = IllegalArgumentException.class)
    public void getAcls_withKafkaPrincipal_principalIsNull() {
        client.getAcls((KafkaPrincipal) null);
    }

    @Test(expected = AdminOperationException.class)
    public void getAcls_withKafkaPrincipal_zkException() {
        KafkaPrincipal user = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "user");

        failureClient.getAcls(user);
    }

    @Test
    public void getAcls_withResource() {
        KafkaPrincipal user = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "my_user");
        Resource topic1 = Resource.fromString(Topic.name() + Resource.Separator() + "topic1");
        Resource topic2 = Resource.fromString(Topic.name() + Resource.Separator() + "topic2");

        Set<Acl> readAcl = Collections.singleton(new Acl(user, Allow$.MODULE$, Acl.WildCardHost(), Read$.MODULE$));

        client.addAcls(readAcl, topic1);
        client.addAcls(readAcl, topic2);

        assertThat(client.getAcls(topic1), is(readAcl));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void getAcls_withResource_immutable() {
        KafkaPrincipal user = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "user");
        Resource topic = Resource.fromString(Topic.name() + Resource.Separator() + "topic");

        Set<Acl> userAcl = Collections.singleton(new Acl(user, Allow$.MODULE$, Acl.WildCardHost(), Read$.MODULE$));
        client.addAcls(userAcl, topic);
        client.getAcls(topic).clear();
    }

    @Test (expected = IllegalArgumentException.class)
    public void getAcls_withResource_resourceIsNull() {
        client.getAcls((Resource) null);
    }

    @Test(expected = AdminOperationException.class)
    public void getAcls_withResource_zkException() {
        Resource resource = Resource.fromString(Topic.name() + Resource.Separator() + "topic");

        failureClient.getAcls(resource);
    }

    @Test
    public void addAcls() {
        KafkaPrincipal user = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "my_user");
        Resource topic1 = Resource.fromString(Topic.name() + Resource.Separator() + "topic1");
        Set<Acl> readAcl = Collections.singleton(new Acl(user, Allow$.MODULE$, Acl.WildCardHost(), Read$.MODULE$));

        client.addAcls(readAcl, topic1);

        assertThat(client.getAcls(topic1), is(readAcl));
    }

    @Test (expected = IllegalArgumentException.class)
    public void addAcls_nullAcls() {
        Resource topic1 = Resource.fromString(Topic.name() + Resource.Separator() + "topic1");
        client.addAcls(null, topic1);
    }

    @Test (expected = IllegalArgumentException.class)
    public void addAcls_nullResource() {
        KafkaPrincipal user = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "my_user");
        Set<Acl> readAcl = Collections.singleton(new Acl(user, Allow$.MODULE$, Acl.WildCardHost(), Read$.MODULE$));
        client.addAcls(readAcl, null);
    }

    @Test(expected = AdminOperationException.class)
    public void addAcls_zkException() {
        KafkaPrincipal user = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "user");
        Resource resource = Resource.fromString(Topic.name() + Resource.Separator() + "topic");
        Set<Acl> readAcl = Collections.singleton(new Acl(user, Allow$.MODULE$, Acl.WildCardHost(), Read$.MODULE$));

        failureClient.addAcls(readAcl, resource);
    }

    @Test
    public void removeAcls() {
        KafkaPrincipal user = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "my_user");
        Resource topic1 = Resource.fromString(Topic.name() + Resource.Separator() + "topic1");
        Set<Acl> readAcl = Collections.singleton(new Acl(user, Allow$.MODULE$, Acl.WildCardHost(), Read$.MODULE$));

        client.addAcls(readAcl, topic1);

        assertThat(client.getAcls(topic1), is(readAcl));

        client.removeAcls(readAcl, topic1);

        assertThat(client.getAcls(topic1), is(empty()));
    }

    @Test (expected = IllegalArgumentException.class)
    public void removeAcls_nullAcls() {
        Resource topic1 = Resource.fromString(Topic.name() + Resource.Separator() + "topic1");
        client.removeAcls(null, topic1);
    }

    @Test (expected = IllegalArgumentException.class)
    public void removeAcls_nullResource() {
        KafkaPrincipal user = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "my_user");
        Set<Acl> readAcl = Collections.singleton(new Acl(user, Allow$.MODULE$, Acl.WildCardHost(), Read$.MODULE$));

        client.removeAcls(readAcl, null);
    }

    @Test(expected = AdminOperationException.class)
    public void removeAcls_zkException() {
        KafkaPrincipal user = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "user");
        Resource resource = Resource.fromString(Topic.name() + Resource.Separator() + "topic");
        Set<Acl> readAcl = Collections.singleton(new Acl(user, Allow$.MODULE$, Acl.WildCardHost(), Read$.MODULE$));

        failureClient.removeAcls(readAcl, resource);
    }

    @Test
    public void deleteTopic() {
        client.createTopic(topic, 1, 1);
        assertThat(client.getTopics(), hasItem(topic));

        client.deleteTopic(topic);

        assertThat(client.getTopics(), not(hasItem(topic)));
    }

    @Test
    public void deleteTopic_doesNotExist() {
        // Verifying that this does not throw an exception
        client.deleteTopic("does_not_exist");
    }

    @Test(expected = AdminOperationException.class)
    public void deleteTopic_zkException() {
        failureClient.deleteTopic(topic);
    }

    @Test (expected = IllegalArgumentException.class)
    public void deleteTopic_nullTopic() {
        client.deleteTopic(null);
    }

    @Test (expected = IllegalArgumentException.class)
    public void deleteTopic_emptyTopic() {
        client.deleteTopic("");
    }

    @Test
    public void getTopicConfig() {
        Properties topicConfig = new Properties();
        topicConfig.setProperty(LogConfig.MinInSyncReplicasProp(), "1");

        client.createTopic(topic, 1, 1, topicConfig);

        assertThat(client.getTopicConfig(topic), is(topicConfig));
    }

    @Test (expected = IllegalArgumentException.class)
    public void getTopicConfig_nullTopic() {
        client.getTopicConfig(null);
    }

    @Test (expected = IllegalArgumentException.class)
    public void getTopicConfig_emptyTopic() {
        client.getTopicConfig("");
    }

    @Test(expected = AdminOperationException.class)
    public void getTopicConfig_zkException() {
        failureClient.getTopicConfig(topic);
    }

    @Test
    public void updateTopicConfig() {
        Properties topicConfig = new Properties();
        topicConfig.setProperty(LogConfig.MinInSyncReplicasProp(), "1");

        client.createTopic(topic, 1, 1, topicConfig);

        assertThat(client.getTopicConfig(topic), is(topicConfig));

        topicConfig.setProperty(LogConfig.RetentionMsProp(), "3600000");

        client.updateTopicConfig(topic, topicConfig);

        assertThat(client.getTopicConfig(topic), is(topicConfig));
    }

    @Test (expected = IllegalArgumentException.class)
    public void updateTopicConfig_nullTopic() {
        client.updateTopicConfig(null, new Properties());
    }

    @Test (expected = IllegalArgumentException.class)
    public void updateTopicConfig_emptyTopic() {
        client.updateTopicConfig("", new Properties());
    }

    @Test (expected = IllegalArgumentException.class)
    public void updateTopicConfig_nullProperties() {
        client.updateTopicConfig(topic, null);
    }

    @Test(expected = AdminOperationException.class)
    public void updateTopicConfig_zkException() {
        failureClient.updateTopicConfig(topic, new Properties());
    }

    @Test
    public void getTopicPartitions() {
        client.createTopic(topic, 1, 1);
        assertThat(client.getTopicPartitions(topic), is(1));
    }

    @Test (expected = IllegalArgumentException.class)
    public void getTopicPartitions_nullTopic() {
        client.getTopicPartitions(null);
    }

    @Test (expected = IllegalArgumentException.class)
    public void getTopicPartitions_emptyTopic() {
        client.getTopicPartitions("");
    }

    @Test (expected = AdminOperationException.class)
    public void getTopicPartitions_zkException() {
        failureClient.getTopicPartitions(topic);
    }

    @Test
    public void addTopicPartitions() {
        client.createTopic(topic, 1, 1);
        assertThat(client.getTopicPartitions(topic), is(1));

        client.addTopicPartitions(topic, 2);

        assertThat(client.getTopicPartitions(topic), is(2));
    }

    @Test (expected = IllegalArgumentException.class)
    public void addTopicPartitions_nullTopic() {
        client.addTopicPartitions(null, 1);
    }

    @Test (expected = IllegalArgumentException.class)
    public void addTopicPartitions_emptyTopic() {
        client.addTopicPartitions("", 1);
    }

    @Test (expected = IllegalArgumentException.class)
    public void addTopicPartitions_partitionsLessThanZero() {
        client.addTopicPartitions(topic, -1);
    }

    @Test (expected = IllegalArgumentException.class)
    public void addTopicPartitions_partitionsIsZero() {
        client.addTopicPartitions(topic, 0);
    }

    @Test (expected = IllegalArgumentException.class)
    public void addTopicPartitions_partitionsIsOne() {
        client.addTopicPartitions(topic, 1);
    }

    @Test (expected = AdminOperationException.class)
    public void addTopicPartitions_zkException() {
        failureClient.addTopicPartitions(topic, 5);
    }

    @Test (expected = IllegalArgumentException.class)
    public void getTopicReplicationFactor_nullTopic() {
        client.getTopicReplicationFactor(null);
    }

    @Test (expected = IllegalArgumentException.class)
    public void getTopicReplicationFactor_emptyTopic() {
        client.getTopicReplicationFactor("");
    }

    @Test (expected = IllegalArgumentException.class)
    public void getTopicReplicationFactor_blankTopic() {
        client.getTopicReplicationFactor(" ");
    }

    @Test
    public void getTopicReplicationFactor() {
        client.createTopic(topic, 1, 1);
        assertThat(client.getTopicReplicationFactor(topic), is(1));
    }

    @Test (expected = AdminOperationException.class)
    public void getTopicReplicationFactor_zkException() {
        failureClient.getTopicReplicationFactor(topic);
    }

    @Test (expected = IllegalArgumentException.class)
    public void getConsumerGroupSummary_nullConsumerGroup() {
        client.getConsumerGroupSummary(null);
    }

    @Test (expected = IllegalArgumentException.class)
    public void getConsumerGroupSummary_emptyConsumerGroup() {
        client.getConsumerGroupSummary("");
    }

    @Test (expected = IllegalArgumentException.class)
    public void getConsumerGroupSummary_blankConsumerGroup() {
        client.getConsumerGroupSummary(" ");
    }

    @Test
    public void getConsumerGroupSummary() {
        client.createTopic(testName.getMethodName(), 1, 1);

        Properties properties = new Properties();
        properties.putAll(KafkaTests.getProps());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, BytesDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, BytesDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, testName.getMethodName());
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, testName.getMethodName() + "-client-id");

        try (Consumer<Object, Object> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(Arrays.asList(testName.getMethodName()));
            consumer.poll(0L);

            AdminClient.ConsumerGroupSummary summary = client.getConsumerGroupSummary(testName.getMethodName());
            assertThat("Expected only 1 consumer summary when getConsumerGroupSummaries(" + testName.getMethodName() + ")",
                    convertToJavaSet(summary.consumers().get().iterator()).size(), is(1));

            assertThat(summary.state(), is(notNullValue()));
            assertThat(summary.coordinator(), is(notNullValue()));
            assertThat(summary.assignmentStrategy(), is(notNullValue()));
        }
    }

    @Test (expected = AdminOperationException.class)
    public void getConsumerGroupSummary_doesNotExist() {
        failureClient.getConsumerGroupSummary("consumer-group-does-not-exist");
    }

    @Test (expected = IllegalArgumentException.class)
    public void getConsumerGroupSummaries_nullConsumerGroup() {
        client.getConsumerGroupSummaries(null);
    }

    @Test (expected = IllegalArgumentException.class)
    public void getConsumerGroupSummaries_emptyConsumerGroup() {
        client.getConsumerGroupSummaries("");
    }

    @Test (expected = IllegalArgumentException.class)
    public void getConsumerGroupSummaries_blankConsumerGroup() {
        client.getConsumerGroupSummaries(" ");
    }

    @Test
    public void getConsumerGroupSummaries() {
        client.createTopic(testName.getMethodName(), 1, 1);

        Properties properties = new Properties();
        properties.putAll(KafkaTests.getProps());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, BytesDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, BytesDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, testName.getMethodName());
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, testName.getMethodName() + "-client-id");

        try (Consumer<Object, Object> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(Arrays.asList(testName.getMethodName()));
            consumer.poll(0L);

            Collection<AdminClient.ConsumerSummary> summaries = client.getConsumerGroupSummaries(testName.getMethodName());
            assertThat("Expected only 1 consumer summary when getConsumerGroupSummaries(" + testName.getMethodName() + ")",
                    summaries.size(), is(1));

            AdminClient.ConsumerSummary summary = summaries.iterator().next();

            Collection<TopicPartition> assignments = convertToJavaSet(summary.assignment().iterator());
            assertThat("Expected consumer assignment to have single partition", assignments.size(), is(1));
            assertThat(assignments.iterator().next(), is(new TopicPartition(testName.getMethodName(), 0)));
            assertThat(summary.clientId(), is(testName.getMethodName() + "-client-id"));
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    public void getConsumerGroupSummaries_immutable() {
        client.createTopic(testName.getMethodName(), 1, 1);

        Properties properties = new Properties();
        properties.putAll(KafkaTests.getProps());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, BytesDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, BytesDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, testName.getMethodName());
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, testName.getMethodName() + "-client-id");

        try (Consumer<Object, Object> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(Arrays.asList(testName.getMethodName()));
            consumer.poll(0L);

            client.getConsumerGroupSummaries(testName.getMethodName()).clear();
        }
    }

    @Test
    public void getConsumerGroupSummaries_consumerGroupDoesNotExist() {
        assertThat("expected consumer group summaries to be empty for group that does not exist is not empty",
                client.getConsumerGroupSummaries("does-not-exist").isEmpty(), is(true));
    }

    @Test (expected = AdminOperationException.class)
    public void getConsumerGroupSummaries_zkException() {
        failureClient.getConsumerGroupSummaries(topic);
    }

    @Test (expected = IllegalArgumentException.class)
    public void getConsumerGroupAssignments_nullConsumerGroup() {
        client.getConsumerGroupAssignments(null);
    }

    @Test (expected = IllegalArgumentException.class)
    public void getConsumerGroupAssignments_emptyConsumerGroup() {
        client.getConsumerGroupAssignments("");
    }

    @Test (expected = IllegalArgumentException.class)
    public void getConsumerGroupAssignments_blankConsumerGroup() {
        client.getConsumerGroupAssignments(" ");
    }

    @Test
    public void getConsumerGroupAssignments() {
        client.createTopic(testName.getMethodName(), 1, 1);

        Properties properties = new Properties();
        properties.putAll(KafkaTests.getProps());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, BytesDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, BytesDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, testName.getMethodName());
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, testName.getMethodName() + "-client-id");

        try (Consumer<Object, Object>  consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(Arrays.asList(testName.getMethodName()));
            consumer.poll(0L);

            assertThat(client.getConsumerGroupAssignments(testName.getMethodName()),
                    is(Collections.singletonMap(new TopicPartition(testName.getMethodName(), 0),
                            testName.getMethodName() + "-client-id")));
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    public void getConsumerGroupAssignments_immutable() {
        client.createTopic(testName.getMethodName(), 1, 1);

        Properties properties = new Properties();
        properties.putAll(KafkaTests.getProps());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, BytesDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, BytesDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, testName.getMethodName());
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, testName.getMethodName() + "-client-id");

        try (Consumer<Object, Object> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(Arrays.asList(testName.getMethodName()));
            consumer.poll(0L);

            client.getConsumerGroupAssignments(testName.getMethodName()).clear();
        }
    }

    @Test
    public void getConsumerGroupAssignments_consumerGroupDoesNotExist() {
        assertThat(client.getConsumerGroupAssignments("does-not-exist"), is(Collections.emptyMap()));
    }

    @Test (expected = AdminOperationException.class)
    public void getConsumerGroupAssignments_zkException() {
        failureClient.getConsumerGroupAssignments(topic);
    }
}
