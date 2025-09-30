import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * This program reproduces the Schema Registry issue:
 * - Registers 10 schemas successfully
 * - Attempts to verify them (should return 404/not found)
 * 
 * Run this in a Docker container with JVM to reproduce the exact issue
 * seen in quick-test where schemas register but cannot be retrieved.
 */
public class SchemaRegistryReproducer {
    
    private static final String SCHEMA_REGISTRY_URL = "http://schema-registry:8081";
    private static final int TIMEOUT_SECONDS = 30;
    
    private static final String VALUE_SCHEMA = """
        {
            "type": "record",
            "name": "LoadTestValue",
            "namespace": "com.seaweedfs.test",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "timestamp", "type": "long"},
                {"name": "data", "type": "string"}
            ]
        }
        """;
    
    private static final String KEY_SCHEMA = """
        {
            "type": "record",
            "name": "LoadTestKey",
            "namespace": "com.seaweedfs.test",
            "fields": [
                {"name": "key_id", "type": "long"}
            ]
        }
        """;
    
    private static final HttpClient client = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(TIMEOUT_SECONDS))
            .build();
    
    public static void main(String[] args) throws Exception {
        System.out.println("=== Schema Registry Issue Reproducer ===\n");
        
        // Wait for Schema Registry to be ready
        waitForSchemaRegistry();
        
        // Step 1: Register schemas (should succeed)
        System.out.println("Step 1: Registering schemas...");
        List<RegistrationResult> registrations = registerSchemas();
        
        // Step 2: Verify schemas (this is where the issue occurs)
        System.out.println("\nStep 2: Verifying schemas...");
        verifySchemas(registrations);
        
        // Step 3: Additional debugging - check _schemas topic directly
        System.out.println("\nStep 3: Checking _schemas topic metadata...");
        checkSchemasTopicMetadata();
        
        System.out.println("\n=== Test Complete ===");
    }
    
    private static void waitForSchemaRegistry() throws Exception {
        System.out.println("Waiting for Schema Registry to be ready...");
        int attempts = 0;
        int maxAttempts = 30;
        
        while (attempts < maxAttempts) {
            try {
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(SCHEMA_REGISTRY_URL + "/subjects"))
                        .GET()
                        .timeout(Duration.ofSeconds(5))
                        .build();
                
                HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                if (response.statusCode() == 200) {
                    System.out.println("✅ Schema Registry is ready!\n");
                    return;
                }
            } catch (Exception e) {
                // Ignore and retry
            }
            
            attempts++;
            Thread.sleep(1000);
        }
        
        throw new RuntimeException("Schema Registry did not become ready within " + maxAttempts + " seconds");
    }
    
    private static List<RegistrationResult> registerSchemas() throws Exception {
        List<RegistrationResult> results = new ArrayList<>();
        
        // Register schemas for 5 topics (10 schemas total: 5 value + 5 key)
        for (int i = 0; i < 5; i++) {
            String topicName = "loadtest-topic-" + i;
            
            // Register value schema
            String valueSubject = topicName + "-value";
            int valueSchemaId = registerSchema(valueSubject, VALUE_SCHEMA);
            results.add(new RegistrationResult(valueSubject, valueSchemaId));
            System.out.printf("  ✅ Registered %s with ID: %d%n", valueSubject, valueSchemaId);
            
            // Register key schema
            String keySubject = topicName + "-key";
            int keySchemaId = registerSchema(keySubject, KEY_SCHEMA);
            results.add(new RegistrationResult(keySubject, keySchemaId));
            System.out.printf("  ✅ Registered %s with ID: %d%n", keySubject, keySchemaId);
        }
        
        System.out.printf("%nRegistration Summary: %d/10 schemas registered successfully%n", results.size());
        return results;
    }
    
    private static int registerSchema(String subject, String schema) throws Exception {
        String jsonPayload = String.format("{\"schema\":\"%s\"}", 
                schema.replace("\"", "\\\"").replace("\n", "\\n"));
        
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(SCHEMA_REGISTRY_URL + "/subjects/" + subject + "/versions"))
                .header("Content-Type", "application/vnd.schemaregistry.v1+json")
                .POST(HttpRequest.BodyPublishers.ofString(jsonPayload))
                .timeout(Duration.ofSeconds(TIMEOUT_SECONDS))
                .build();
        
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        
        if (response.statusCode() != 200) {
            throw new RuntimeException("Failed to register schema for " + subject + 
                    ": " + response.statusCode() + " - " + response.body());
        }
        
        // Parse schema ID from response
        // Response format: {"id":11,"version":1,...}
        String body = response.body();
        int idStart = body.indexOf("\"id\":") + 5;
        int idEnd = body.indexOf(",", idStart);
        if (idEnd == -1) {
            idEnd = body.indexOf("}", idStart);
        }
        return Integer.parseInt(body.substring(idStart, idEnd).trim());
    }
    
    private static void verifySchemas(List<RegistrationResult> registrations) throws Exception {
        int successCount = 0;
        int failCount = 0;
        
        for (RegistrationResult reg : registrations) {
            boolean verified = verifySchema(reg.subject, reg.schemaId);
            if (verified) {
                successCount++;
                System.out.printf("  ✅ Verified %s (ID: %d)%n", reg.subject, reg.schemaId);
            } else {
                failCount++;
                System.out.printf("  ❌ Failed to verify %s (ID: %d)%n", reg.subject, reg.schemaId);
            }
        }
        
        System.out.printf("%nVerification Summary: %d/%d schemas verified%n", successCount, registrations.size());
        
        if (failCount > 0) {
            System.out.printf("%n⚠️  ISSUE REPRODUCED: %d schemas registered but cannot be retrieved!%n", failCount);
            System.out.println("This is the exact issue seen in quick-test.");
        }
    }
    
    private static boolean verifySchema(String subject, int schemaId) throws Exception {
        // Method 1: Get latest version
        HttpRequest request1 = HttpRequest.newBuilder()
                .uri(URI.create(SCHEMA_REGISTRY_URL + "/subjects/" + subject + "/versions/latest"))
                .GET()
                .timeout(Duration.ofSeconds(TIMEOUT_SECONDS))
                .build();
        
        HttpResponse<String> response1 = client.send(request1, HttpResponse.BodyHandlers.ofString());
        
        if (response1.statusCode() == 200) {
            return true;
        }
        
        // Method 2: Get by schema ID
        HttpRequest request2 = HttpRequest.newBuilder()
                .uri(URI.create(SCHEMA_REGISTRY_URL + "/schemas/ids/" + schemaId))
                .GET()
                .timeout(Duration.ofSeconds(TIMEOUT_SECONDS))
                .build();
        
        HttpResponse<String> response2 = client.send(request2, HttpResponse.BodyHandlers.ofString());
        
        if (response2.statusCode() == 200) {
            return true;
        }
        
        // Both methods failed
        System.out.printf("    Debug: GET /subjects/%s/versions/latest -> %d%n", subject, response1.statusCode());
        System.out.printf("    Debug: GET /schemas/ids/%d -> %d%n", schemaId, response2.statusCode());
        
        return false;
    }
    
    private static void checkSchemasTopicMetadata() throws Exception {
        // This is a bonus check - we can try to use Kafka admin client to check the _schemas topic
        System.out.println("  Note: _schemas topic should contain all registered schemas");
        System.out.println("  Use 'weed sql' to query: SELECT count(*) FROM kafka._schemas;");
        System.out.println("  Expected: 12 messages (2 NOOP + 10 SCHEMA)");
    }
    
    static class RegistrationResult {
        String subject;
        int schemaId;
        
        RegistrationResult(String subject, int schemaId) {
            this.subject = subject;
            this.schemaId = schemaId;
        }
    }
}
