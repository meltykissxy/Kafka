package producer;

import org.apache.kafka.clients.producer.*;
import utils.KafkaUtils;

import java.io.IOException;

/**
 * 带回调函数的API
 */
public class ProducerWithCallback {
    public static void main(String[] args) throws IOException {
        // Terminal启动Kafka消费者来测试，注意Topic的名字要一致哦
        // /opt/module/kafka/bin/kafka-console-consumer.sh --bootstrap-server hadoop102:9092  --topic first

        // 第一步，获取配置
        Producer<String, String> producer = new KafkaProducer<>(KafkaUtils.loadProducerProperties());
        // 第二步：生产数据
        for (int i = 0; i < 100; i++) {
            producer.send(new ProducerRecord<String, String>(KafkaUtils.getTopic(), Integer.toString(i), "Apple" + Integer.toString(i)), new Callback() {
                // 回调函数，该方法会在Producer收到ack时调用，为异步调用
                // 回调函数会在producer收到ack时调用，为异步调用，该方法有两个参数，分别是RecordMetadata和Exception，如果Exception为null，说明消息发送成功，如果Exception不为null，说明消息发送失败。
                // 注意：消息发送失败会自动重试，不需要我们在回调函数中手动重试
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        System.out.println("success->" + metadata.offset());
                    } else {
                        exception.printStackTrace();
                    }
                }
            });
        }
        // 第三步：关闭
        producer.close();
    }
}
