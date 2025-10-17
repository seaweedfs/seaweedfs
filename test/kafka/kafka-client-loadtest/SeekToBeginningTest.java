import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.consumer.internals.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.errors.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;

/**
 * Enhanced test program to reproduce and diagnose the seekToBeginning() hang issue
 * 
 * This test:
 * 1. Adds detailed logging of Kafka client operations
 * 2. Captures exceptions and timeouts
 * 3. Shows what the consumer is waiting for
 * 4. Tracks request/response lifecycle
 */
public class SeekToBeginningTest {
    private static final Logger log = LoggerFactory.getLogger(SeekToBeginningTest.class);
    
    public static void main(String[] args) throws Exception {
        String bootstrapServers = "localhost:9093";
        String topicName = "_schemas";

        if (args.length > 0) {
            bootstrapServers = args[0];
        }

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-seek-group");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "test-seek-client");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "45000");
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "60000");
        
        // Add comprehensive debug logging
        props.put("log4j.logger.org.apache.kafka.clients.consumer.internals", "DEBUG");
        props.put("log4j.logger.org.apache.kafka.clients.producer.internals", "DEBUG");
        props.put("log4j.logger.org.apache.kafka.clients.Metadata", "DEBUG");
        
        // Add shorter timeouts to fail faster
        props.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "10000"); // 10 seconds instead of 60
        
        System.out.println("\n╔════════════════════════════════════════════════════════════╗");
        System.out.println("║         SeekToBeginning Diagnostic Test                      ║");
        System.out.println(String.format("║     Connecting to: %-42s║", bootstrapServers));
        System.out.println("╚════════════════════════════════════════════════════════════╝\n");

        System.out.println("[TEST] Creating KafkaConsumer...");
        System.out.println("[TEST] Bootstrap servers: " + bootstrapServers);
        System.out.println("[TEST] Group ID: test-seek-group");
        System.out.println("[TEST] Client ID: test-seek-client");
        
        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props);

        TopicPartition tp = new TopicPartition(topicName, 0);
        List<TopicPartition> partitions = Arrays.asList(tp);

        System.out.println("\n[STEP 1] Assigning to partition: " + tp);
        consumer.assign(partitions);
        System.out.println("[STEP 1] ✓ Assigned successfully");

        System.out.println("\n[STEP 2] Calling seekToBeginning()...");
        long startTime = System.currentTimeMillis();
        try {
            consumer.seekToBeginning(partitions);
            long seekTime = System.currentTimeMillis() - startTime;
            System.out.println("[STEP 2] ✓ seekToBeginning() completed in " + seekTime + "ms");
        } catch (Exception e) {
            System.out.println("[STEP 2] ✗ EXCEPTION in seekToBeginning():");
            e.printStackTrace();
            consumer.close();
            return;
        }

        System.out.println("\n[STEP 3] Starting poll loop...");
        System.out.println("[STEP 3] First poll will trigger offset lookup (ListOffsets)");
        System.out.println("[STEP 3] Then will fetch initial records\n");
        
        int successfulPolls = 0;
        int failedPolls = 0;
        int totalRecords = 0;

        for (int i = 0; i < 3; i++) {
            System.out.println("═══════════════════════════════════════════════════════════");
            System.out.println("[POLL " + (i + 1) + "] Starting poll with 15-second timeout...");
            long pollStart = System.currentTimeMillis();
            
            try {
                System.out.println("[POLL " + (i + 1) + "] Calling consumer.poll()...");
                ConsumerRecords<byte[], byte[]> records = consumer.poll(java.time.Duration.ofSeconds(15));
                long pollTime = System.currentTimeMillis() - pollStart;
                
                System.out.println("[POLL " + (i + 1) + "] ✓ Poll completed in " + pollTime + "ms");
                System.out.println("[POLL " + (i + 1) + "] Records received: " + records.count());
                
                if (records.count() > 0) {
                    successfulPolls++;
                    totalRecords += records.count();
                    for (ConsumerRecord<byte[], byte[]> record : records) {
                        System.out.println("  [RECORD] offset=" + record.offset() + 
                                         ", key.len=" + (record.key() != null ? record.key().length : 0) +
                                         ", value.len=" + (record.value() != null ? record.value().length : 0));
                    }
                } else {
                    System.out.println("[POLL " + (i + 1) + "] ℹ No records in this poll (but no error)");
                    successfulPolls++;
                }
            } catch (TimeoutException e) {
                long pollTime = System.currentTimeMillis() - pollStart;
                failedPolls++;
                System.out.println("[POLL " + (i + 1) + "] ✗ TIMEOUT after " + pollTime + "ms");
                System.out.println("[POLL " + (i + 1) + "] This means consumer is waiting for something from broker");
                System.out.println("[POLL " + (i + 1) + "] Possible causes:");
                System.out.println("         - ListOffsetsRequest never sent");
                System.out.println("         - ListOffsetsResponse not received");
                System.out.println("         - Broker metadata parsing failed");
                System.out.println("         - Connection issue");
                
                // Print current position info if available
                try {
                    long position = consumer.position(tp);
                    System.out.println("[POLL " + (i + 1) + "] Current position: " + position);
                } catch (Exception e2) {
                    System.out.println("[POLL " + (i + 1) + "] Could not get position: " + e2.getMessage());
                }
            } catch (Exception e) {
                failedPolls++;
                long pollTime = System.currentTimeMillis() - pollStart;
                System.out.println("[POLL " + (i + 1) + "] ✗ EXCEPTION after " + pollTime + "ms:");
                System.out.println("[POLL " + (i + 1) + "] Exception type: " + e.getClass().getSimpleName());
                System.out.println("[POLL " + (i + 1) + "] Message: " + e.getMessage());
                
                // Print stack trace for first exception
                if (i == 0) {
                    System.out.println("[POLL " + (i + 1) + "] Stack trace:");
                    e.printStackTrace();
                }
            }
        }

        System.out.println("\n═══════════════════════════════════════════════════════════");
        System.out.println("[RESULTS] Test Summary:");
        System.out.println("  Successful polls: " + successfulPolls);
        System.out.println("  Failed polls: " + failedPolls);
        System.out.println("  Total records received: " + totalRecords);
        
        if (failedPolls > 0) {
            System.out.println("\n[DIAGNOSIS] Consumer is BLOCKED during poll()");
            System.out.println("  This indicates the consumer cannot:");
            System.out.println("  1. Send ListOffsetsRequest to determine offset 0, OR");
            System.out.println("  2. Receive/parse ListOffsetsResponse from broker, OR");
            System.out.println("  3. Parse broker metadata for partition leader lookup");
        } else if (totalRecords == 0) {
            System.out.println("\n[DIAGNOSIS] Consumer is working but NO records found");
            System.out.println("  This might mean:");
            System.out.println("  1. Topic has no messages, OR");
            System.out.println("  2. Fetch is working but broker returns empty");
        } else {
            System.out.println("\n[SUCCESS] Consumer working correctly!");
            System.out.println("  Received " + totalRecords + " records");
        }

        System.out.println("\n[CLEANUP] Closing consumer...");
        try {
            consumer.close();
            System.out.println("[CLEANUP] ✓ Consumer closed successfully");
        } catch (Exception e) {
            System.out.println("[CLEANUP] ✗ Error closing consumer: " + e.getMessage());
        }
        
        System.out.println("\n[TEST] Done!\n");
    }
}
