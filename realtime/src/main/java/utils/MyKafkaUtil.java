package utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

/**
 * @author liufengting
 * @date 2021/12/27
 */
public class MyKafkaUtil {
    private static String brokers = "node1:9092, node2:9092: node3:9092";
    private static String DEAFAULT_TOPIC = "DWD_DEFAULT_TOPIC";

    /**
     * 获得一个指定 kafka topic name 的生产者对象
     * @param topic
     * @return
     */
    public static FlinkKafkaProducer<String> getKafkaProducer(String topic) {
        return new FlinkKafkaProducer<String>(brokers, topic, new SimpleStringSchema());
    }
}
