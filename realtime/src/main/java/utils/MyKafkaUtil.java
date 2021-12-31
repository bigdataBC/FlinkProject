package utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * @author liufengting
 * @date 2021/12/27
 */
public class MyKafkaUtil {
    private static String brokers = "node1:9092,node2:9092,node3:9092";
    private static String DEAFAULT_TOPIC = "DWD_DEFAULT_TOPIC";

    /**
     * 获得一个指定 kafka topic name 的生产者对象
     * @param topic
     * @return
     */
    public static FlinkKafkaProducer<String> getKafkaProducer(String topic) {
        return new FlinkKafkaProducer<String>(brokers, topic, new SimpleStringSchema());
    }

    public static <T> FlinkKafkaProducer<T> getKafkaProducer(KafkaSerializationSchema<T> kafkaSerializationSchema) {

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);

        return new FlinkKafkaProducer<T>(DEAFAULT_TOPIC,
                kafkaSerializationSchema,
                properties,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
    }

    /**
     * 获得一个指定 kafka topic name 和 消费者组ID 的消费者对象
     * @param topic
     * @param groupId
     * @return
     */
    public static FlinkKafkaConsumer<String> getKafkaConsumer(String topic, String groupId) {

        Properties properties = new Properties();
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);

        return new FlinkKafkaConsumer<String>(topic,
                new SimpleStringSchema(),
                properties);
    }

    /**
     * 拼接Kafka相关属性到DDL
     * @param topic
     * @param groupId
     * @return
     */
    public static String getKafkaDDL(String topic, String groupId) {
        return  " 'connector' = 'kafka', " +
                " 'topic' = '" + topic + "'," +
                " 'properties.bootstrap.servers' = '" + brokers + "', " +
                " 'properties.group.id' = '" + groupId + "', " +
                " 'format' = 'json', " +
                " 'scan.startup.mode' = 'latest-offset'  ";
    }
}
