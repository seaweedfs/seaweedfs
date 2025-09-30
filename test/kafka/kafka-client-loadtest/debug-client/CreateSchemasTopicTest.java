import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Java client that creates the _schemas topic and tests Schema Registry operations.
 * This simulates exactly what Schema Registry does during initialization.
 */
public class CreateSchemasTopicTest {
    
    private static final String BOOTSTRAP_SERVERS = "kafka-gateway:9093";
    private static final String TOPIC = "_schemas";
    private static final String GROUP_ID = "create-schemas-test-group";
    private static final int TIMEOUT_MS = 10000;
    
    public static void main(String[] args) {
        System.out.println("=== Create _schemas Topic Test ===");
        
        CreateSchemasTopicTest test = new CreateSchemasTopicTest();
        try {
            test.runTest();
        } catch (Exception e) {
            System.err.println("Test failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    public void runTest() throws Exception {
        System.out.println("Creating Producer and Consumer...");
        KafkaProducer<byte[], byte[]> producer = createProducer();
        KafkaConsumer<byte[], byte[]> consumer = createConsumer();
        
        try {
            System.out.println("\nStep 1: Creating _schemas topic by producing to it...");
            
            // Create the topic by producing a message to it (Kafka auto-creates topics)
            byte[] noopKey = createSchemaRegistryNoopKey();
            ProducerRecord<byte[], byte[]> noopRecord = new ProducerRecord<>(TOPIC, 0, noopKey, null);
            
            System.out.println("Producing Noop record to create topic...");
            System.out.println("Noop key: " + new String(noopKey));
            
            Future<RecordMetadata> future = producer.send(noopRecord);
            RecordMetadata metadata = future.get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
            
            System.out.println("✅ Topic created and Noop record produced!");
            System.out.println("   Offset: " + metadata.offset());
            System.out.println("   Partition: " + metadata.partition());
            System.out.println("   Topic: " + metadata.topic());
            System.out.println("   Timestamp: " + metadata.timestamp());
            
            System.out.println("\nStep 2: Testing consumer position (Schema Registry initialization)...");
            
            // Now test consumer position like Schema Registry does
            TopicPartition partition = new TopicPartition(TOPIC, 0);
            consumer.assign(Collections.singletonList(partition));
            consumer.seekToBeginning(Collections.singletonList(partition));
            
            long position = consumer.position(partition);
            System.out.println("✅ Consumer position determined: " + position);
            
            System.out.println("\nStep 3: Consuming records to verify...");
            
            // Consume records to verify everything works
            ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(5000));
            System.out.println("Found " + records.count() + " records");
            
            for (ConsumerRecord<byte[], byte[]> record : records) {
                System.out.println("📨 Record:");
                System.out.println("   Offset: " + record.offset());
                System.out.println("   Key: " + (record.key() != null ? new String(record.key()) : "null"));
                System.out.println("   Value: " + (record.value() != null ? new String(record.value()) : "null"));
                System.out.println("   Timestamp: " + record.timestamp());
            }
            
            if (records.count() > 0) {
                System.out.println("\n🎉 SUCCESS: _schemas topic creation and Schema Registry simulation completed!");
                System.out.println("   - Topic was created successfully");
                System.out.println("   - Consumer position was determined");
                System.out.println("   - Records were consumed successfully");
                System.out.println("   - This proves the Kafka Gateway can handle Schema Registry operations!");
            } else {
                System.out.println("\n⚠️  WARNING: Topic created but no records were consumed");
                System.out.println("   - This may indicate an issue with message fetching");
            }
            
        } finally {
            producer.close();
            consumer.close();
        }
    }
    
    private KafkaProducer<byte[], byte[]> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, TIMEOUT_MS);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, TIMEOUT_MS * 2);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        
        return new KafkaProducer<>(props);
    }
    
    private KafkaConsumer<byte[], byte[]> createConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, TIMEOUT_MS);
        
        return new KafkaConsumer<>(props);
    }
    
    private byte[] createSchemaRegistryNoopKey() {
        // Create the exact Noop key that Schema Registry uses
        String noopKeyJson = "{\"keytype\":\"NOOP\",\"magic\":0}";
        return noopKeyJson.getBytes();
    }
}
