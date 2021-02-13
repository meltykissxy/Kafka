package utils;

import org.apache.kafka.clients.producer.ProducerConfig;

import java.io.IOException;
import java.util.Properties;

public class KafkaUtils {
    public static Properties loadProducerProperties() throws IOException {
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

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, prop.getProperty("ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG"));
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, prop.getProperty("ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG"));

        return props;
    }

    public static Properties loadConsumerProperties(boolean autoCommit) throws IOException {
        Properties prop = PropertiesUtil.load("Properties.properties");

        Properties props = new Properties();

        //可以写多个，看老师视频
        props.put("bootstrap.servers", prop.getProperty("bootstrap.servers"));

        props.put("group.id", prop.getProperty("group.id"));

        props.put("enable.auto.commit", autoCommit);
        if (autoCommit) {
            //每个多久提交一次偏移量，默认5秒
            props.put("auto.commit.interval.ms", prop.getProperty("auto.commit.interval.ms"));
        }

        //反序列化
        props.put("key.deserializer", prop.getProperty("key.deserializer"));
        props.put("value.deserializer", prop.getProperty("value.deserializer"));
        return props;
    }

    public static String getTopic() throws IOException {
        return PropertiesUtil.load("Properties.properties").getProperty("Kafka.topic");
    }
}
