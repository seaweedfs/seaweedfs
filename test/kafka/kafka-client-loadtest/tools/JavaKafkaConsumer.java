import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class JavaKafkaConsumer {
    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: java JavaKafkaConsumer <broker> <topic>");
            System.exit(1);
        }

        String broker = args[0];
        String topic = args[1];

        System.out.println("Connecting to Kafka broker: " + broker);
        System.out.println("Topic: " + topic);

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "java-test-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "1");
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "1000");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));

        System.out.println("Starting to consume messages...");

        int messageCount = 0;
        int errorCount = 0;
        long startTime = System.currentTimeMillis();

        try {
            while (true) {
                try {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                    for (ConsumerRecord<String, String> record : records) {
                        messageCount++;
                        System.out.printf("Message #%d: topic=%s partition=%d offset=%d key=%s value=%s%n",
                                messageCount, record.topic(), record.partition(), record.offset(),
                                record.key(), record.value());
                    }

                    // Stop after 100 messages or 60 seconds
                    if (messageCount >= 100 || (System.currentTimeMillis() - startTime) > 60000) {
                        long duration = System.currentTimeMillis() - startTime;
                        System.out.printf("%nSuccessfully consumed %d messages in %dms%n", messageCount, duration);
                        System.out.printf("Success rate: %.1f%% (%d/%d including errors)%n",
                                (double) messageCount / (messageCount + errorCount) * 100, messageCount,
                                messageCount + errorCount);
                        break;
                    }
                } catch (Exception e) {
                    errorCount++;
                    System.err.printf("Error during poll #%d: %s%n", errorCount, e.getMessage());
                    e.printStackTrace();

                    // Stop after 10 consecutive errors or 60 seconds
                    if (errorCount > 10 || (System.currentTimeMillis() - startTime) > 60000) {
                        long duration = System.currentTimeMillis() - startTime;
                        System.err.printf("%nStopping after %d errors in %dms%n", errorCount, duration);
                        break;
                    }
                }
            }
        } finally {
            consumer.close();
        }
    }
}
