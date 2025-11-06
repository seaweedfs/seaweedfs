import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.Future;

public class JavaProducerTest {
    public static void main(String[] args) {
        String bootstrapServers = args.length > 0 ? args[0] : "localhost:9093";
        String topicName = args.length > 1 ? args[1] : "test-topic";

        System.out.println("Testing Kafka Producer with broker: " + bootstrapServers);
        System.out.println("    Topic: " + topicName);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "java-producer-test");
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 10000);

        System.out.println("Creating Producer with config:");
        props.forEach((k, v) -> System.out.println("  " + k + " = " + v));

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            System.out.println("Producer created successfully");

            // Try to send a test message
            System.out.println("\n=== Test: Send Message ===");
            try {
                ProducerRecord<String, String> record = new ProducerRecord<>(topicName, "key1", "value1");
                System.out.println("Sending record to topic: " + topicName);
                Future<RecordMetadata> future = producer.send(record);

                RecordMetadata metadata = future.get(); // This will block and wait for response
                System.out.println("Message sent successfully!");
                System.out.println("  Topic: " + metadata.topic());
                System.out.println("  Partition: " + metadata.partition());
                System.out.println("  Offset: " + metadata.offset());
            } catch (Exception e) {
                System.err.println("Send failed: " + e.getMessage());
                e.printStackTrace();

                // Print cause chain
                Throwable cause = e.getCause();
                int depth = 1;
                while (cause != null && depth < 5) {
                    System.err.println(
                            "  Cause " + depth + ": " + cause.getClass().getName() + ": " + cause.getMessage());
                    cause = cause.getCause();
                    depth++;
                }
            }

            System.out.println("\nTest completed!");

        } catch (Exception e) {
            System.err.println("Producer creation or operation failed: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}
