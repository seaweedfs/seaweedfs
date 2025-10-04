import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Simple Java client that bypasses InitProducerId by disabling idempotence
 * to test basic produce functionality without transactional features.
 */
public class SimpleProduceTest {

    private static final String BOOTSTRAP_SERVERS = "kafka-gateway:9093";
    private static final String TOPIC = "_schemas";
    private static final int TIMEOUT_MS = 30000;

    public static void main(String[] args) {
        System.out.println("=== Simple Produce Test (No InitProducerId) ===");

        SimpleProduceTest test = new SimpleProduceTest();
        try {
            test.runTest();
        } catch (Exception e) {
            System.err.println("Test failed: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public void runTest() throws Exception {
        System.out.println("Creating Producer without idempotence...");
        KafkaProducer<byte[], byte[]> producer = createProducer();

        try {
            System.out.println("\nStep 1: Testing basic produce operation...");
            testBasicProduce(producer);
        } finally {
            producer.close();
        }
    }

    private KafkaProducer<byte[], byte[]> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, TIMEOUT_MS);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, TIMEOUT_MS * 2);
        
        // CRITICAL: Disable idempotence to avoid InitProducerId
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false");
        props.put(ProducerConfig.ACKS_CONFIG, "1"); // Require leader acknowledgment
        props.put(ProducerConfig.RETRIES_CONFIG, "0"); // No retries to keep it simple

        return new KafkaProducer<>(props);
    }

    private void testBasicProduce(KafkaProducer<byte[], byte[]> producer) throws Exception {
        // Create a simple test message
        byte[] key = createTestKey();
        byte[] value = createTestValue();

        System.out.println("Producing test message...");
        System.out.println("Key: " + bytesToHex(key));
        System.out.println("Value: " + bytesToHex(value));

        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(TOPIC, 0, key, value);

        Future<RecordMetadata> future = producer.send(record);
        RecordMetadata metadata = future.get(TIMEOUT_MS, TimeUnit.MILLISECONDS);

        System.out.println("✅ SUCCESS: Message produced successfully!");
        System.out.println("Offset: " + metadata.offset());
        System.out.println("Partition: " + metadata.partition());
        System.out.println("Topic: " + metadata.topic());
        System.out.println("Timestamp: " + metadata.timestamp());
    }

    private byte[] createTestKey() {
        String testKeyJson = "{\"keytype\":\"TEST\",\"magic\":1}";
        return testKeyJson.getBytes();
    }

    private byte[] createTestValue() {
        String testValueJson = "{\"test\":\"simple_produce_test\",\"timestamp\":" + System.currentTimeMillis() + "}";
        return testValueJson.getBytes();
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
