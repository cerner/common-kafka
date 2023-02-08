package com.cerner.common.kafka.testing;

import kafka.utils.EmptyTestInfo;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.Properties;

/**
 * An abstract class for coordinating a suite of tests requiring an internal Kafka + Zookeeper cluster to be started up.
 * <p>
 * Typical usage:
 * <pre>
 *     &#64;RunWith(Suite.class)
 *     &#64;SuiteClasses({
 *         MyClass.class, MyOtherClass.class
 *     })
 *     public class MySuiteOfKafkaTests extends AbstractKafkaTests {
 *     }
 * </pre>
 * <p>
 * Each member of the suite should contain code like,
 * <pre>
 *     &#64;BeforeClass
 *     public static void startup() throws Exception {
 *         MySuiteOfKafkaTests.startTest();
 *     }
 *
 *     &#64;AfterClass
 *     public static void shutdown() throws Exception {
 *         MySuiteOfKafkaTests.endTest();
 *     }
 * </pre>
 * <p>
 * A similar standalone test suite that does not extend this class can also be created if applicable, i.e. if there are tests
 * that do not require a running Kafka + Zookeeper cluster.
 * <p>
 * The test suites are then configured to run with Maven build configuration like,
 * <pre>
 *     &lt;build&gt;
 *         &lt;plugins&gt;
 *             &lt;plugin&gt;
 *                 &lt;groupId&gt;org.apache.maven.plugins&lt;/groupId&gt;
 *                 &lt;artifactId&gt;maven-surefire-plugin&lt;/artifactId&gt;
 *                 &lt;configuration&gt;
 *                     &lt;includes&gt;
 *                         &lt;include&gt;&#42;&#42;/MySuiteOfKafkaTests.java&lt;/include&gt;
 *                         &lt;include&gt;&#42;&#42;/MySuiteOfStandaloneTests.java&lt;/include&gt;
 *                     &lt;/includes&gt;
 *                 &lt;/configuration&gt;
 *             &lt;/plugin&gt;
 *         &lt;/plugins&gt;
 *     &lt;/build&gt;
 * </pre>
 *
 * @author Brandon Inman
 */
@Timeout(600)
public abstract class AbstractKafkaTests {

    private static KafkaBrokerTestHarness kafka = null;
    private static boolean runAsSuite = false;

    @BeforeAll
    public static void startSuite() throws IOException {
        runAsSuite = true;
        startKafka();
    }

    @AfterAll
    public static void endSuite() throws IOException {
        stopKafka();
    }

    public static void startTest() throws IOException {
        if (!runAsSuite)
            startKafka();
    }

    public static void startTest(Properties props) throws IOException {
        if (!runAsSuite)
            startKafka(props);
    }

    public static void endTest() throws IOException {
        if (!runAsSuite)
            stopKafka();
    }

    public static void startKafka() throws IOException {
        Properties properties = new Properties();
        startKafka(properties);
    }

    public static void startKafka(Properties props) throws IOException {
        kafka = new KafkaBrokerTestHarness(1,"kafka", props);
        kafka.setUp(new EmptyTestInfo());
    }

    public static void stopKafka() throws IOException {
        if (kafka != null) {
            kafka.tearDown();
        }
    }

    public static void startOnlyKafkaBrokers(){
        kafka.startKafkaCluster();
    }

    public static void stopOnlyKafkaBrokers() throws IOException {
        kafka.stopKafkaCluster();
    }

    public static Properties getProps() {
        return (kafka != null) ? kafka.getProps() : null;
    }
}