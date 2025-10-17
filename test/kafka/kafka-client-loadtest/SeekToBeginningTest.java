import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import java.util.*;

/**
 * Test program to reproduce the seekToBeginning() hang issue
 * 
 * This simulates what Schema Registry does:
 * 1. Create KafkaConsumer
 * 2. Assign to partition
 * 3. Call seekToBeginning()
 * 4. Poll for records
 * 
 * Expected behavior: Consumer should send ListOffsets and then Fetch requests
 * Observed behavior: Consumer sends InitProducerId but not ListOffsets, then
 * hangs
 */
public class SeekToBeginningTest {
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

        System.out.println("[TEST] Creating KafkaConsumer connecting to " + bootstrapServers);
        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props);

        TopicPartition tp = new TopicPartition(topicName, 0);
        List<TopicPartition> partitions = Arrays.asList(tp);

        System.out.println("[TEST] Assigning to partition: " + tp);
        consumer.assign(partitions);

        System.out.println("[TEST] Calling seekToBeginning()...");
        long startTime = System.currentTimeMillis();
        consumer.seekToBeginning(partitions);
        long seekTime = System.currentTimeMillis() - startTime;
        System.out.println("[TEST] seekToBeginning() completed in " + seekTime + "ms");

        System.out.println("[TEST] Starting poll loop (30 second timeout per poll)...");
        for (int i = 0; i < 3; i++) {
            System.out.println("[POLL " + (i + 1) + "] Polling for records...");
            long pollStart = System.currentTimeMillis();
            ConsumerRecords<byte[], byte[]> records = consumer.poll(java.time.Duration.ofSeconds(30));
            long pollTime = System.currentTimeMillis() - pollStart;
            System.out.println("[POLL " + (i + 1) + "] Got " + records.count() + " records in " + pollTime + "ms");

            for (ConsumerRecord<byte[], byte[]> record : records) {
                System.out.println("  [RECORD] offset=" + record.offset() + ", key.len=" +
                        (record.key() != null ? record.key().length : 0) +
                        ", value.len=" + (record.value() != null ? record.value().length : 0));
            }
        }

        System.out.println("[TEST] Closing consumer...");
        consumer.close();
        System.out.println("[TEST] Done!");
    }
}
