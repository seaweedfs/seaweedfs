import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.TopicConfig;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class CreateTopicTest {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-gateway:9093");
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 10000);
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 10000);
        
        try (AdminClient admin = AdminClient.create(props)) {
            System.out.println("Creating topic 'schema-test-topic' with schema support...");
            
            NewTopic topic = new NewTopic("schema-test-topic", 1, (short) 1);
            CreateTopicsResult result = admin.createTopics(Collections.singletonList(topic));
            result.all().get(10, TimeUnit.SECONDS);
            
            System.out.println("✅ Topic 'schema-test-topic' created successfully!");
        }
    }
}
