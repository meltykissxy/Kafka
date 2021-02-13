package flume;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.List;
import java.util.Map;

public class FlumeKafkaInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    /**
     * 如果包含"apple"的数据，发送到first主题
     * 如果包含"google"的数据，发送到second主题
     * 其他的数据发送到third主题
     * @param event
     * @return
     */
    @Override
    public Event intercept(Event event) {
        //1.获取event的header
        Map<String, String> headers = event.getHeaders();
        //2.获取event的body
        String body = new String(event.getBody());
        if(body.contains("apple")){
            headers.put("topic","first");
        }else if(body.contains("google")){
            headers.put("topic","second");
        }
        return event;

    }

    @Override
    public List<Event> intercept(List<Event> events) {
        for (Event event : events) {
            intercept(event);
        }
        return events;
    }

    @Override
    public void close() {

    }
    public static class MyBuilder implements  Builder{

        @Override
        public Interceptor build() {
            return  new FlumeKafkaInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}

/**
 * # Name the components on this agent
 * a1.sources = r1
 * a1.sinks = k1
 * a1.channels = c1
 *
 * # Describe/configure the source
 * a1.sources.r1.type = netcat
 * a1.sources.r1.bind = 0.0.0.0
 * a1.sources.r1.port = 6666
 *
 * # Describe the sink
 * a1.sinks.k1.type = org.apache.flume.sink.kafka.KafkaSink
 * a1.sinks.k1.kafka.topic = third
 * a1.sinks.k1.kafka.bootstrap.servers = hadoop102:9092,hadoop103:9092,hadoop104:9092
 * a1.sinks.k1.kafka.flumeBatchSize = 20
 * a1.sinks.k1.kafka.producer.acks = 1
 * a1.sinks.k1.kafka.producer.linger.ms = 1
 *
 * #Interceptor
 * a1.sources.r1.interceptors = i1
 * a1.sources.r1.interceptors.i1.type = com.atguigu.kafka.flumeInterceptor.FlumeKafkaInterceptor$MyBuilder
 *
 * # # Use a channel which buffers events in memory
 * a1.channels.c1.type = memory
 * a1.channels.c1.capacity = 1000
 * a1.channels.c1.transactionCapacity = 100
 *
 * # Bind the source and sink to the channel
 * a1.sources.r1.channels = c1
 * a1.sinks.k1.channel = c1
 */
