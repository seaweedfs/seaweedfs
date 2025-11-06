import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.ListTopicsResult;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class JavaAdminClientTest {
    public static void main(String[] args) {
        // Set uncaught exception handler to catch AdminClient thread errors
        Thread.setDefaultUncaughtExceptionHandler((t, e) -> {
            System.err.println("UNCAUGHT EXCEPTION in thread " + t.getName() + ":");
            e.printStackTrace();
        });

        String bootstrapServers = args.length > 0 ? args[0] : "localhost:9093";

        System.out.println("Testing Kafka wire protocol with broker: " + bootstrapServers);

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 10000);
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 10000);
        props.put(AdminClientConfig.CLIENT_ID_CONFIG, "java-admin-test");
        props.put(AdminClientConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, 120000);
        props.put(AdminClientConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG, 10000);
        props.put(AdminClientConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG, 30000);
        props.put(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, "PLAINTEXT");
        props.put(AdminClientConfig.RECONNECT_BACKOFF_MS_CONFIG, 50);
        props.put(AdminClientConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, 1000);

        System.out.println("Creating AdminClient with config:");
        props.forEach((k, v) -> System.out.println("  " + k + " = " + v));

        try (AdminClient adminClient = AdminClient.create(props)) {
            System.out.println("AdminClient created successfully");
            Thread.sleep(2000); // Give it time to initialize

            // Test 1: Describe Cluster (uses Metadata API internally)
            System.out.println("\n=== Test 1: Describe Cluster ===");
            try {
                DescribeClusterResult clusterResult = adminClient.describeCluster();
                String clusterId = clusterResult.clusterId().get(10, TimeUnit.SECONDS);
                int nodeCount = clusterResult.nodes().get(10, TimeUnit.SECONDS).size();
                System.out.println("Cluster ID: " + clusterId);
                System.out.println("Nodes: " + nodeCount);
            } catch (Exception e) {
                System.err.println("Describe Cluster failed: " + e.getMessage());
                e.printStackTrace();
            }

            // Test 2: List Topics
            System.out.println("\n=== Test 2: List Topics ===");
            try {
                ListTopicsResult topicsResult = adminClient.listTopics();
                int topicCount = topicsResult.names().get(10, TimeUnit.SECONDS).size();
                System.out.println("Topics: " + topicCount);
            } catch (Exception e) {
                System.err.println("List Topics failed: " + e.getMessage());
                e.printStackTrace();
            }

            System.out.println("\nAll tests completed!");

        } catch (Exception e) {
            System.err.println("AdminClient creation failed: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}
