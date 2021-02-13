package producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import utils.KafkaUtils;

import java.io.IOException;

/**
 * 不带回调函数的API
 */
public class ProducerWithoutCallback {
    public static void main(String[] args) throws IOException {
        // Terminal启动Kafka消费者来测试，注意Topic的名字要一致哦
        // /opt/module/kafka/bin/kafka-console-consumer.sh --bootstrap-server hadoop102:9092  --topic first

        // 第一步，获取配置
        Producer<String, String> producer = new KafkaProducer<>(KafkaUtils.loadProducerProperties());

        // 第二步：生产数据
        for (int i = 0; i < 100; i++) {
            producer.send(new ProducerRecord<String, String>(KafkaUtils.getTopic(), Integer.toString(i), Integer.toString(i)));
        }

        // 第三步：关闭
        producer.close();

    }
}
