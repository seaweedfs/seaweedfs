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
 * Simplified Java client that directly tests Schema Registry operations
 * without using AdminClient (which has timeout issues).
 */
public class SimpleSchemaRegistryTest {
    
    private static final String BOOTSTRAP_SERVERS = "kafka-gateway:9093";
    private static final String TOPIC = "_schemas";
    private static final String GROUP_ID = "simple-schema-test-group";
    private static final int TIMEOUT_MS = 15000;
    
    public static void main(String[] args) {
        System.out.println("=== Simple Schema Registry Test ===");
        
        SimpleSchemaRegistryTest test = new SimpleSchemaRegistryTest();
        try {
            test.runTest();
        } catch (Exception e) {
            System.err.println("Test failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    public void runTest() throws Exception {
        System.out.println("Creating Producer and Consumer (bypassing AdminClient)...");
        KafkaProducer<byte[], byte[]> producer = createProducer();
        KafkaConsumer<byte[], byte[]> consumer = createConsumer();
        
        try {
            System.out.println("\nStep 1: Testing Schema Registry initialization sequence...");
            testSchemaRegistrySequence(producer, consumer);
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
    
    private void testSchemaRegistrySequence(KafkaProducer<byte[], byte[]> producer, 
                                          KafkaConsumer<byte[], byte[]> consumer) throws Exception {
        
        // Step 1: Subscribe consumer to _schemas topic
        TopicPartition partition = new TopicPartition(TOPIC, 0);
        consumer.assign(Collections.singletonList(partition));
        consumer.seekToBeginning(Collections.singletonList(partition));
        
        long initialOffset = consumer.position(partition);
        System.out.println("Initial consumer position: " + initialOffset);
        
        // Step 2: Produce a Noop record (exactly like Schema Registry does)
        byte[] noopKey = createSchemaRegistryNoopKey();
        ProducerRecord<byte[], byte[]> noopRecord = new ProducerRecord<>(TOPIC, 0, noopKey, null);
        
        System.out.println("Producing Schema Registry Noop record...");
        System.out.println("Noop key: " + new String(noopKey));
        System.out.println("Noop key hex: " + bytesToHex(noopKey));
        
        Future<RecordMetadata> future = producer.send(noopRecord);
        RecordMetadata metadata = future.get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        
        long producedOffset = metadata.offset();
        System.out.println("✅ Noop record produced successfully!");
        System.out.println("   Produced offset: " + producedOffset);
        System.out.println("   Partition: " + metadata.partition());
        System.out.println("   Topic: " + metadata.topic());
        System.out.println("   Timestamp: " + metadata.timestamp());
        
        // Step 3: Try to consume the record we just produced
        System.out.println("\nStep 2: Consuming the produced record...");
        
        // Seek back to beginning to read all records
        consumer.seekToBeginning(Collections.singletonList(partition));
        
        boolean foundRecord = false;
        long startTime = System.currentTimeMillis();
        
        while (System.currentTimeMillis() - startTime < TIMEOUT_MS && !foundRecord) {
            ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(1000));
            
            System.out.println("Poll returned " + records.count() + " records");
            
            for (ConsumerRecord<byte[], byte[]> record : records) {
                System.out.println("📨 Consumed record:");
                System.out.println("   Offset: " + record.offset());
                System.out.println("   Key: " + (record.key() != null ? new String(record.key()) : "null"));
                System.out.println("   Key hex: " + (record.key() != null ? bytesToHex(record.key()) : "null"));
                System.out.println("   Value: " + (record.value() != null ? new String(record.value()) : "null"));
                System.out.println("   Timestamp: " + record.timestamp());
                
                if (record.offset() == producedOffset) {
                    foundRecord = true;
                    System.out.println("✅ Found the record we just produced!");
                }
            }
            
            if (records.count() == 0) {
                System.out.println("No records returned, checking current position...");
                long currentPosition = consumer.position(partition);
                System.out.println("Current position: " + currentPosition);
            }
        }
        
        if (foundRecord) {
            System.out.println("\n🎉 SUCCESS: Schema Registry simulation completed successfully!");
            System.out.println("   - Produced Noop record at offset " + producedOffset);
            System.out.println("   - Successfully consumed the record back");
            System.out.println("   - This simulates successful Schema Registry initialization!");
        } else {
            System.out.println("\n❌ FAILURE: Could not consume the record we just produced");
            System.out.println("   - This explains why Schema Registry fails to initialize");
            System.out.println("   - The Kafka Gateway may have issues with message persistence or fetching");
        }
    }
    
    private byte[] createSchemaRegistryNoopKey() {
        // Create the exact Noop key that Schema Registry uses
        // Based on Confluent's source: {"keytype":"NOOP","magic":0}
        String noopKeyJson = "{\"keytype\":\"NOOP\",\"magic\":0}";
        return noopKeyJson.getBytes();
    }
    
    private String bytesToHex(byte[] bytes) {
        if (bytes == null) return "null";
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }
}
