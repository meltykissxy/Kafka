package producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import utils.KafkaUtils;

import java.io.IOException;
import java.util.Properties;

/**
 * 不带回调函数的API
 */
public class ProducerWithoutCallback {
    public static void main(String[] args) throws IOException {
        Producer<String, String> producer = new KafkaProducer<>(KafkaUtils.getProperties());

        // 测试：生产数据
        for (int i = 0; i < 100; i++) {
            producer.send(new ProducerRecord<String, String>("first", Integer.toString(i), Integer.toString(i)));
        }

        producer.close();

    }
}
