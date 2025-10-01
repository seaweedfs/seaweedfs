import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.time.Duration;
import java.util.*;

/**
 * Reproduces Schema Registry's KafkaStoreReaderThread behavior
 * 
 * This client mimics how Schema Registry reads from the _schemas topic:
 * 1. Uses manual partition assignment (not consumer group subscription)
 * 2. Seeks to beginning (or specific offset)
 * 3. Polls for records continuously
 * 4. Processes NOOP messages (key only, empty value)
 * 5. Processes actual schema messages
 * 
 * Expected behavior:
 * - Should read all messages from offset 0 onwards
 * - Should advance offset as messages are consumed
 * 
 * Actual behavior (bug):
 * - Gets stuck at offset 0
 * - High water mark returns 0
 * - Fetch returns empty
 */
public class SchemaReaderThreadReproducer {
    
    private static final String TOPIC = "_schemas";
    private static final int PARTITION = 0;
    private static final String BOOTSTRAP_SERVERS = "kafka-gateway:9093";
    
    public static void main(String[] args) throws Exception {
        System.out.println("=== Schema Registry Reader Thread Reproducer ===");
        System.out.println("Connecting to: " + BOOTSTRAP_SERVERS);
        System.out.println("Topic: " + TOPIC);
        System.out.println("Partition: " + PARTITION);
        System.out.println();
        
        // Configure consumer exactly like Schema Registry's KafkaStoreReaderThread
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "schema-reader-reproducer");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "KafkaStore-reader-_schemas");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        
        // Add debug settings
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "1");
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "500");
        
        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props);
        
        try {
            // Manual partition assignment (like Schema Registry)
            TopicPartition topicPartition = new TopicPartition(TOPIC, PARTITION);
            consumer.assign(Collections.singletonList(topicPartition));
            
            // Seek to beginning (like Schema Registry on startup)
            System.out.println("Seeking to beginning of topic...");
            consumer.seekToBeginning(Collections.singletonList(topicPartition));
            
            // Get current position
            long currentPosition = consumer.position(topicPartition);
            System.out.println("Current position after seekToBeginning: " + currentPosition);
            
            // Get earliest and latest offsets
            Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(Collections.singletonList(topicPartition));
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(Collections.singletonList(topicPartition));
            
            System.out.println("Beginning offset: " + beginningOffsets.get(topicPartition));
            System.out.println("End offset (HWM): " + endOffsets.get(topicPartition));
            System.out.println();
            
            // Poll for records (like Schema Registry's doWork() method)
            int pollCount = 0;
            int totalRecordsRead = 0;
            long lastOffset = -1;
            
            System.out.println("Starting to poll for records...");
            System.out.println("--------------------------------------------");
            
            while (pollCount < 20) {  // Limit to 20 polls for testing
                pollCount++;
                
                System.out.println("\n[Poll #" + pollCount + "]");
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(1000));
                
                System.out.println("  Received " + records.count() + " records");
                
                if (records.isEmpty()) {
                    // Check current position and end offsets
                    long pos = consumer.position(topicPartition);
                    Map<TopicPartition, Long> latestEndOffsets = consumer.endOffsets(Collections.singletonList(topicPartition));
                    long hwm = latestEndOffsets.get(topicPartition);
                    
                    System.out.println("  No records. Position: " + pos + ", HWM: " + hwm);
                    
                    if (pos >= hwm) {
                        System.out.println("  Caught up to HWM. Waiting for new messages...");
                    }
                    continue;
                }
                
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    totalRecordsRead++;
                    lastOffset = record.offset();
                    
                    boolean isNoop = (record.value() == null || record.value().length == 0);
                    String keyStr = record.key() != null ? new String(record.key()) : "null";
                    String valueInfo = isNoop ? "[NOOP]" : 
                                      ("value_len=" + (record.value() != null ? record.value().length : 0));
                    
                    System.out.println("  Record " + totalRecordsRead + ":");
                    System.out.println("    Offset: " + record.offset());
                    System.out.println("    Key: " + keyStr);
                    System.out.println("    Value: " + valueInfo);
                    System.out.println("    Timestamp: " + record.timestamp());
                    
                    if (isNoop) {
                        System.out.println("    → Processing NOOP message (key only)");
                    } else {
                        System.out.println("    → Processing actual schema message");
                    }
                }
                
                // Update position
                long newPosition = consumer.position(topicPartition);
                System.out.println("  Position after poll: " + newPosition);
            }
            
            System.out.println("\n============================================");
            System.out.println("Summary:");
            System.out.println("  Total polls: " + pollCount);
            System.out.println("  Total records read: " + totalRecordsRead);
            System.out.println("  Last offset read: " + lastOffset);
            System.out.println("  Final position: " + consumer.position(topicPartition));
            
            Map<TopicPartition, Long> finalEndOffsets = consumer.endOffsets(Collections.singletonList(topicPartition));
            System.out.println("  Final HWM: " + finalEndOffsets.get(topicPartition));
            
            if (totalRecordsRead == 0) {
                System.out.println("\n⚠️  BUG REPRODUCED: Reader thread stuck at offset 0!");
                System.out.println("  - No records were read");
                System.out.println("  - HWM is likely 0 (Gateway bug)");
                System.out.println("  - This is why Schema Registry can't verify schemas");
            } else if (lastOffset == 0 && totalRecordsRead == 1) {
                System.out.println("\n⚠️  PARTIAL BUG: Only read offset 0, can't advance!");
                System.out.println("  - Only first message was read");
                System.out.println("  - HWM not updating after writes");
            } else {
                System.out.println("\n✓ SUCCESS: Reader thread is working!");
                System.out.println("  - Multiple records read");
                System.out.println("  - Offset advancing normally");
            }
            
        } finally {
            consumer.close();
            System.out.println("\nConsumer closed.");
        }
    }
}
