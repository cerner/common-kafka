package com.cerner.common.kafka.testing;


import java.io.IOException;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;

import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;

/**
 * A test harness that brings up an embedded Zookeeper instance.
 * <p>
 * Adapted from the {@code kafka.zk.ZooKeeperTestHarness} class.
 * </p>
 *
 * @author A. Olson
 */
public class ZookeeperTestHarness {

    /**
     * Zookeeper connection info.
     */
    protected final String zookeeperConnect;

    private EmbeddedZookeeper zookeeper;
    private final int zkConnectionTimeout;
    private final int zkSessionTimeout;

    /**
     * Zookeeper client connection.
     */
    protected ZkUtils zkUtils;

    /**
     * Creates a new Zookeeper broker test harness.
     */
    public ZookeeperTestHarness() {
        this(KafkaTestUtils.getPorts(1)[0]);
    }

    /**
     * Creates a new Zookeeper service test harness using the given port.
     *
     * @param zookeeperPort The port number to use for Zookeeper client connections.
     */
    public ZookeeperTestHarness(int zookeeperPort) {
        this.zookeeper = null;
        this.zkUtils = null;
        this.zkConnectionTimeout = 6000;
        this.zkSessionTimeout = 6000;
        this.zookeeperConnect = "localhost:" + zookeeperPort;
    }

    /**
     * Returns a client for communicating with the Zookeeper service.
     *
     * @return A Zookeeper client.
     *
     * @throws IllegalStateException
     *             if Zookeeper has not yet been {@link #setUp()}, or has already been {@link #tearDown() torn down}.
     */
    public ZkClient getZkClient() {
        if (zkUtils == null) {
            throw new IllegalStateException("Zookeeper service is not active");
        }
        return zkUtils.zkClient();
    }

    public ZkUtils getZkUtils() {
        return zkUtils;
    }

    /**
     * Startup Zookeeper.
     *
     * @throws IOException if an error occurs during Zookeeper initialization.
     */
    public void setUp() throws IOException {
        zookeeper = new EmbeddedZookeeper(zookeeperConnect);
        ZkClient zkClient = new ZkClient(zookeeperConnect, zkSessionTimeout, zkConnectionTimeout, ZKStringSerializer$.MODULE$);
        ZkConnection connection = new ZkConnection(zookeeperConnect, zkSessionTimeout);
        zkUtils = new ZkUtils(zkClient, connection, false);
    }

    /**
     * Shutdown Zookeeper.
     *
     * @throws IOException if an error occurs during Zookeeper shutdown.
     */
    public void tearDown() throws IOException {
        if (zkUtils != null) {
            zkUtils.close();
            zkUtils = null;
        }
        if (zookeeper != null) {
            zookeeper.shutdown();
            zookeeper = null;
        }
    }
}

