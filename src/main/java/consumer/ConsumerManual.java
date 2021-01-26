package consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import utils.KafkaUtils;

import java.io.IOException;
import java.util.Arrays;
/**
 * 手动提交位移
 */
public class ConsumerManual {
    public static void main(String[] args) throws IOException {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(KafkaUtils.loadConsumerProperties(false));
        consumer.subscribe(Arrays.asList("first"));//消费者订阅主题

        while (true) {
            //消费者拉取数据
            ConsumerRecords<String, String> records = consumer.poll(100);

            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
            }

            //同步提交，当前线程会阻塞直到offset提交成功
            consumer.commitSync();
        }
    }
}
