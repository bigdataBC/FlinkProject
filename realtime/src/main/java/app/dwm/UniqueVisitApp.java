package app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import utils.MyKafkaUtil;

import java.text.SimpleDateFormat;

/**
 * 访客 UV 计算
 * 数据流：web/app -> Nginx -> SpringBoot -> Kafka(ods) -> FlinkApp -> Kafka(dwd) -> FlinkApp -> Kafka(dwm)
 * 程  序：mockLog -> Nginx -> Logger.sh  -> Kafka(ZK)  -> BaseLogApp -> kafka -> UniqueVisitApp -> Kafka
 * @author liufengting
 * @date 2021/12/29
 */
public class UniqueVisitApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //1.1 设置CK&状态后端
        //env.setStateBackend(new FsStateBackend("hdfs://node1:8020/gmall-flink-210325/ck"));
        //env.enableCheckpointing(5000L);
        //env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //env.getCheckpointConfig().setCheckpointTimeout(10000L);
        //env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        //env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000);

        //env.setRestartStrategy(RestartStrategies.fixedDelayRestart());

        //TODO 2.读取Kafka dwd_page_log 主题的数据
        String groupId = "unique_visit_app_210325";
        String sourceTopic = "dwd_page_log";
        String sinkTopic = "dwm_unique_visit";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaConsumer(sourceTopic, groupId));

        //TODO 3.将每行数据转换为JSON对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject);

        //TODO 4.过滤数据  状态编程  只保留每个mid每天第一次登陆的数据
        KeyedStream<JSONObject, String> jsonObjKeyedStream = jsonObjDS.keyBy(data -> data.getJSONObject("common").getString("mid"));
        SingleOutputStreamOperator<JSONObject> uvDS = jsonObjKeyedStream.filter(new RichFilterFunction<JSONObject>() {
            ValueState<String> dateState;
            SimpleDateFormat simpleDateFormat;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<String>("date-state", String.class);
                //设置状态的超时时间以及更新时间的方式
                StateTtlConfig stateTtlConfig = new StateTtlConfig
                        .Builder(Time.hours(24))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();
                valueStateDescriptor.enableTimeToLive(stateTtlConfig);
                dateState = getRuntimeContext().getState(valueStateDescriptor);

                simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
            }

            @Override
            public boolean filter(JSONObject value) throws Exception {
                String lastPageId = value.getJSONObject("page").getString("last_page_id");

                if (lastPageId == null || lastPageId.length() <= 0) {
                    // 取出当前时间和状态里的时间相比较,相同则过滤掉，否则更新状态值
                    String curDate = simpleDateFormat.format(value.getLong("ts"));
                    String lastDate = dateState.value();
                    if (!curDate.equals(lastDate)) {
                        dateState.update(curDate);
                        return true;
                    }
                }
                return false;
            }
        });
        //TODO 5.将数据写入Kafka
        uvDS.print("uvDS>>>");
        uvDS.map(JSONAware::toJSONString)
                .addSink(MyKafkaUtil.getKafkaProducer(sinkTopic));

        //TODO 6.启动任务
        env.execute("UniqueVisitApp>>>");
    }
}
