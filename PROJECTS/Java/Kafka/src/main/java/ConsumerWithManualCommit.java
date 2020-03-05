import org.apache.kafka.clients.consumer.*;

import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.*;

public class ConsumerWithManualCommit {
    public static void main(String[] args) throws IOException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092, localhost:9093, localhost:9094");
        props.put("group.id", "second-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        String topics[] = {"numbers"};

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList(topics));

        final int minBatchSize = 200;
        List<ConsumerRecord<String, String>> buffer = new ArrayList<>();
        // Change path here to match path on your computer
        FileWriter fileWriter = new FileWriter("/Users/bogdan/Desktop/numbers.txt",true);

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    buffer.add(record);
                    String message = String.format("offset = %d, key = %s, value = %s, partition = %s%n", record.offset(), record.key(), record.value(), record.partition());
                    System.out.println(message);
                }
                if (buffer.size() >= minBatchSize) {
                    // Write to file
                    fileWriter.append(buffer.toString());
                    consumer.commitSync();
                    buffer.clear();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
            fileWriter.close();
        }
    }
}
