import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Simplified Java client that simulates Schema Registry operations
 * to debug the Noop record offset synchronization issue.
 */
public class SchemaRegistrySimulator {
    
    private static final String BOOTSTRAP_SERVERS = "kafka-gateway:9093";
    private static final String TOPIC = "_schemas";
    private static final String GROUP_ID = "schema-registry-debug-group";
    private static final int TIMEOUT_MS = 30000;
    
    public static void main(String[] args) {
        System.out.println("=== Schema Registry Simulator - Debug Tool ===");
        
        SchemaRegistrySimulator simulator = new SchemaRegistrySimulator();
        try {
            simulator.runSimulation();
        } catch (Exception e) {
            System.err.println("Simulation failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    public void runSimulation() throws Exception {
        System.out.println("Step 1: Creating AdminClient and verifying topic...");
        verifyTopic();
        
        System.out.println("\nStep 2: Creating Producer and Consumer...");
        KafkaProducer<byte[], byte[]> producer = createProducer();
        KafkaConsumer<byte[], byte[]> consumer = createConsumer();
        
        try {
            System.out.println("\nStep 3: Simulating Schema Registry initialization sequence...");
            simulateSchemaRegistryInit(producer, consumer);
        } finally {
            producer.close();
            consumer.close();
        }
    }
    
    private void verifyTopic() throws Exception {
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        adminProps.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, TIMEOUT_MS);
        
        try (AdminClient admin = AdminClient.create(adminProps)) {
            // List topics to verify connectivity
            var topics = admin.listTopics().names().get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
            System.out.println("Available topics: " + topics);
            
            if (!topics.contains(TOPIC)) {
                System.out.println("Creating topic: " + TOPIC);
                NewTopic newTopic = new NewTopic(TOPIC, 1, (short) 1);
                admin.createTopics(Collections.singletonList(newTopic)).all().get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
                System.out.println("Topic created successfully");
            } else {
                System.out.println("Topic already exists: " + TOPIC);
            }
        }
    }
    
    private KafkaProducer<byte[], byte[]> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, TIMEOUT_MS);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, TIMEOUT_MS * 2);
        
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
    
    private void simulateSchemaRegistryInit(KafkaProducer<byte[], byte[]> producer, 
                                          KafkaConsumer<byte[], byte[]> consumer) throws Exception {
        
        // Step 1: Subscribe consumer and get initial position
        TopicPartition partition = new TopicPartition(TOPIC, 0);
        consumer.assign(Collections.singletonList(partition));
        consumer.seekToBeginning(Collections.singletonList(partition));
        
        long initialOffset = consumer.position(partition);
        System.out.println("Initial consumer position: " + initialOffset);
        
        // Step 2: Produce a Noop record (simulating Schema Registry behavior)
        byte[] noopKey = createNoopKey();
        ProducerRecord<byte[], byte[]> noopRecord = new ProducerRecord<>(TOPIC, 0, noopKey, null);
        
        System.out.println("Producing Noop record...");
        System.out.println("Noop key bytes: " + bytesToHex(noopKey));
        
        Future<RecordMetadata> future = producer.send(noopRecord);
        RecordMetadata metadata = future.get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        
        long producedOffset = metadata.offset();
        System.out.println("Noop record produced successfully!");
        System.out.println("Produced offset: " + producedOffset);
        System.out.println("Partition: " + metadata.partition());
        System.out.println("Topic: " + metadata.topic());
        
        // Step 3: Wait for consumer to reach the produced offset (Schema Registry logic)
        System.out.println("\nWaiting for consumer to reach offset: " + producedOffset);
        
        long targetOffset = producedOffset + 1; // Consumer should reach the next offset after the produced record
        boolean reachedTarget = waitForOffset(consumer, partition, targetOffset, TIMEOUT_MS);
        
        if (reachedTarget) {
            System.out.println("✅ SUCCESS: Consumer reached target offset " + targetOffset);
            System.out.println("This simulates successful Schema Registry initialization!");
        } else {
            System.out.println("❌ TIMEOUT: Consumer did not reach target offset " + targetOffset);
            System.out.println("This explains the Schema Registry initialization timeout!");
            
            // Debug: Check current consumer position
            long currentPosition = consumer.position(partition);
            System.out.println("Current consumer position: " + currentPosition);
            System.out.println("Gap: " + (targetOffset - currentPosition) + " offsets");
        }
        
        // Step 4: Read any available records to see what's actually in the topic
        System.out.println("\nReading available records from topic...");
        consumer.seekToBeginning(Collections.singletonList(partition));
        
        ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(5000));
        System.out.println("Found " + records.count() + " records in topic");
        
        for (ConsumerRecord<byte[], byte[]> record : records) {
            System.out.println("Record - Offset: " + record.offset() + 
                             ", Key: " + (record.key() != null ? bytesToHex(record.key()) : "null") +
                             ", Value: " + (record.value() != null ? bytesToHex(record.value()) : "null"));
        }
    }
    
    private byte[] createNoopKey() {
        // Simulate Schema Registry's NoopKey serialization
        // Based on the source code: {"keytype": "NOOP", "magic": 0}
        String noopKeyJson = "{\"keytype\":\"NOOP\",\"magic\":0}";
        return noopKeyJson.getBytes();
    }
    
    private boolean waitForOffset(KafkaConsumer<byte[], byte[]> consumer, 
                                TopicPartition partition, 
                                long targetOffset, 
                                int timeoutMs) {
        long startTime = System.currentTimeMillis();
        long endTime = startTime + timeoutMs;
        
        while (System.currentTimeMillis() < endTime) {
            try {
                // Poll to trigger offset updates
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(1000));
                
                long currentPosition = consumer.position(partition);
                System.out.println("Current position: " + currentPosition + ", Target: " + targetOffset);
                
                if (currentPosition >= targetOffset) {
                    return true;
                }
                
                // Process any records we received
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    System.out.println("Consumed record at offset: " + record.offset());
                }
                
            } catch (Exception e) {
                System.err.println("Error while waiting for offset: " + e.getMessage());
                break;
            }
        }
        
        return false;
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
