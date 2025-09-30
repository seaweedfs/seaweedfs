import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.common.Node;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Simple test to check what broker addresses are returned in metadata responses
 */
public class MetadataTest {
    
    private static final String BOOTSTRAP_SERVERS = "localhost:9093";
    private static final int TIMEOUT_MS = 15000;
    
    public static void main(String[] args) {
        System.out.println("=== Metadata Test ===");
        
        MetadataTest test = new MetadataTest();
        try {
            test.runTest();
        } catch (Exception e) {
            System.err.println("Test failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    public void runTest() throws Exception {
        System.out.println("Creating AdminClient to test metadata response...");
        
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        adminProps.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, TIMEOUT_MS);
        
        try (AdminClient admin = AdminClient.create(adminProps)) {
            System.out.println("Getting cluster metadata...");
            
            DescribeClusterResult clusterResult = admin.describeCluster();
            
            // Get cluster nodes (brokers)
            var nodes = clusterResult.nodes().get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
            
            System.out.println("✅ Cluster metadata retrieved successfully!");
            System.out.println("Found " + nodes.size() + " broker(s):");
            
            for (Node node : nodes) {
                System.out.println("  Broker ID: " + node.id());
                System.out.println("  Host: " + node.host());
                System.out.println("  Port: " + node.port());
                System.out.println("  Full Address: " + node.host() + ":" + node.port());
                System.out.println("  Rack: " + node.rack());
                System.out.println();
                
                // Check if the returned address matches what we expect
                if ("localhost".equals(node.host())) {
                    System.out.println("✅ SUCCESS: Broker returns 'localhost' as expected!");
                } else if ("kafka-gateway".equals(node.host())) {
                    System.out.println("❌ ISSUE: Broker returns 'kafka-gateway' instead of 'localhost'");
                    System.out.println("   This will cause DNS resolution issues for external clients");
                } else {
                    System.out.println("⚠️  UNEXPECTED: Broker returns '" + node.host() + "'");
                }
            }
            
            // Get controller info
            Node controller = clusterResult.controller().get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
            System.out.println("Controller: " + controller.host() + ":" + controller.port() + " (ID: " + controller.id() + ")");
            
        } catch (Exception e) {
            System.err.println("Failed to get cluster metadata: " + e.getMessage());
            throw e;
        }
    }
}


