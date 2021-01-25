package utils;

import org.apache.kafka.clients.producer.ProducerConfig;

import java.io.IOException;
import java.util.Properties;

public class KafkaUtils {
    public static Properties getProperties() throws IOException {
        Properties prop = PropertiesUtil.load("Properties.properties");

        Properties props = new Properties();

        //kafka集群，broker-list
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, prop.getProperty("ProducerConfig.BOOTSTRAP_SERVERS_CONFIG"));

        props.put(ProducerConfig.ACKS_CONFIG, prop.getProperty("ProducerConfig.ACKS_CONFIG"));

        //重试次数（收不到ack时request.timeout.ms默认30秒就重发消息）
        props.put(ProducerConfig.RETRIES_CONFIG, prop.getProperty("ProducerConfig.RETRIES_CONFIG"));

        //批次大小
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, prop.getProperty("ProducerConfig.BATCH_SIZE_CONFIG"));

        //等待时间（跟批次大小一起用）
        props.put(ProducerConfig.LINGER_MS_CONFIG, prop.getProperty("ProducerConfig.LINGER_MS_CONFIG"));

        //RecordAccumulator缓冲区大小
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, prop.getProperty("ProducerConfig.BUFFER_MEMORY_CONFIG"));

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        return props;
    }
}
