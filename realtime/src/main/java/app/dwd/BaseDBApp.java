package app.dwd;

import app.function.CustomerDeserialization;
import app.function.DimSinkFunction;
import app.function.TableProcessFunction;
import bean.TableProcess;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;
import utils.MyKafkaUtil;

import javax.annotation.Nullable;

/**
 * 准备业务数据 DWD 层
 * 1、接收 Kafka 数据，过滤空值数据
 * 2、实现动态分流功能
 * 3、把分好的流保存到对应表、主题中
 * 数据流：web/app -> nginx -> SpringBoot -> Mysql -> FlinkApp -> Kafka(ods) -> FlinkApp -> Kafka(dwd)/Phoenix(dim)
 * 程  序：                        mockDb -> Mysql -> FlinkCDC -> Kafka(ZK) -> BaseDBApp -> Kafka/Phoenix(hbase,zk,hdfs)
 * @author liufengting
 * @date 2021/12/28
 */
public class BaseDBApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //1.1 设置CK&状态后端
        //env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall-flink-210325/ck"));
        //env.enableCheckpointing(5000L);
        //env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //env.getCheckpointConfig().setCheckpointTimeout(10000L);
        //env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        //env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000);

        //env.setRestartStrategy(RestartStrategies.fixedDelayRestart());

        //TODO 2.消费Kafka ods_base_db 主题数据创建流
        String sourceTopic = "ods_base_db";
        String groupId = "base_db_app_210325";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaConsumer(sourceTopic, groupId));

        //TODO 3.将每行数据转换为JSON对象并过滤(delete) 主流
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject)
                .filter(new FilterFunction<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        String type = value.getString("type");
                        return !type.equals("delete");
                    }
                });
        //TODO 4.使用FlinkCDC消费配置表并处理成         广播流
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("node1")
                .port(3306)
                .username("lft")
                .password("123456")
                .databaseList("gmall2021_realtime")
                .tableList("gmall2021_realtime.table_process")
                .startupOptions(StartupOptions.initial())
                .deserializer(new CustomerDeserialization())
                .build();
        DataStreamSource<String> tableProcessStrDS = env.addSource(sourceFunction);
        tableProcessStrDS.print("tableProcessStrDS>>>");
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>("map-state", String.class, TableProcess.class);
        BroadcastStream<String> broadcastStream = tableProcessStrDS.broadcast(mapStateDescriptor);

        //TODO 5.连接主流和广播流
        BroadcastConnectedStream<JSONObject, String> connectedStream = jsonObjDS.connect(broadcastStream);

        //TODO 6.分流  处理数据  广播流数据,主流数据(根据广播流数据进行处理)
        OutputTag<JSONObject> hbaseTag = new OutputTag<JSONObject>("hbase-tag") {
        };
        SingleOutputStreamOperator<JSONObject> kafkaStream = connectedStream.process(new TableProcessFunction(hbaseTag, mapStateDescriptor));
        //TODO 7.提取Kafka流数据和HBase流数据
        DataStream<JSONObject> hbaseStream = kafkaStream.getSideOutput(hbaseTag);
        kafkaStream.print("Kafka>>>>>>>>");
        hbaseStream.print("HBase>>>>>>>>");

        //TODO 8.将Kafka数据写入Kafka主题,将HBase数据写入Phoenix表
        hbaseStream.addSink(new DimSinkFunction());
        kafkaStream.addSink(MyKafkaUtil.getKafkaProducer(new KafkaSerializationSchema<JSONObject>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject element, @Nullable Long timestamp) {
                return new ProducerRecord<byte[], byte[]>(element.getString("sinkTable"),
                        element.getString("after").getBytes());
            }
        }));

        //TODO 9.启动任务
        env.execute("BaseDBApp");
    }
}
