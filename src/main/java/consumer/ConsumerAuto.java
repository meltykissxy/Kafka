package consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import utils.KafkaUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

/**
 * 自动提交位移
 */
public class ConsumerAuto {
    public static void main(String[] args) throws IOException {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(KafkaUtils.loadConsumerProperties(true));

        // 可以订阅多个topic
        consumer.subscribe(Arrays.asList(KafkaUtils.getTopic()));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);

            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
            }
        }
    }
}
