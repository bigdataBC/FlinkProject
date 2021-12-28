package app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import utils.MyKafkaUtil;

/**
 * 准备用户行为日志 DWD 层
 * 1、接收 Kafka 数据，并进行转换
 * 2、识别新老用户（访客）
 * 3、利用侧输出流实现数据拆分
 * 4、将不同流的数据推送到下游 kafka 的不同 Topic（分流）
 * 数据流：web/app -> Nginx -> SpringBoot -> Kafka(ods) -> FlinkApp -> Kafka(dwd)
 * 程  序：mockLog -> Nginx -> Logger.sh  -> Kafka(ZK)  -> BaseLogApp -> kafka
 * @author liufengting
 * @date 2021/12/20
 */
public class BaseLogApp {
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

        //TODO 2.消费 ods_base_log 主题数据创建流
        String sourceTopic = "ods_base_log";
        String groupId = "base_log_app_210325";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaConsumer(sourceTopic, groupId));

        //TODO 3.将每行数据转换为JSON对象,将脏数据弄到测输出流
        OutputTag<String> dirtyOutputTag = new OutputTag<String>("dirtyData"){
        };
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    //发生异常,将数据写入侧输出流
                    ctx.output(dirtyOutputTag, value);
                }
            }
        });
        //打印脏数据
        jsonObjDS.getSideOutput(dirtyOutputTag).print("dirtyData>>>");

        // 按照 mid 分组
        KeyedStream<JSONObject, String> jsonObjKeyedStream = jsonObjDS.keyBy(data -> data.getJSONObject("common").getString("mid"));
        //TODO 4.新老用户校验  状态编程
        SingleOutputStreamOperator<JSONObject> jsonObjWithNewFlagDS = jsonObjKeyedStream.map(new RichMapFunction<JSONObject, JSONObject>() {
            private ValueState<String> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                valueState = getRuntimeContext().getState(new ValueStateDescriptor<String>("value-state", String.class));
            }

            @Override
            public JSONObject map(JSONObject value) throws Exception {
                //获取数据中的"is_new"标记
                String isNew = value.getJSONObject("common").getString("is_new");
                //判断isNew标记是否为"1",是的话获取状态，若状态为null，则更新状态的值为1，否则修改value的isNew值为0
                if (isNew.equals("1")) {
                    String stateValue = valueState.value();
                    if (stateValue != null) {
                        value.getJSONObject("common").put("is_new", "0");
                    } else {
                        valueState.update("1");
                    }
                }
                return value;
            }
        });
        jsonObjWithNewFlagDS.print("jsonObjWithNewFlagDS>>>");

        //TODO 5.分流  侧输出流  页面：主流  启动：侧输出流  曝光：侧输出流
        OutputTag<String> startOutputTag = new OutputTag<String>("startDS") {
        };
        OutputTag<String> displayOutputTag = new OutputTag<String>("displayDS") {
        };
        SingleOutputStreamOperator<String> pageDS = jsonObjWithNewFlagDS.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject value, ProcessFunction<JSONObject, String>.Context ctx, Collector<String> out) throws Exception {
                // 获取启动日志字段，是启动的话输出到startDS，否则输出到主流，并分出曝光日志
                String startStr = value.getString("start");
                if (startStr != null && startStr.length() > 0) {
                    ctx.output(startOutputTag, value.toJSONString());
                } else {
                    // 输出到主流
                    out.collect(value.toJSONString());

                    JSONArray displays = value.getJSONArray("displays");
                    if (displays != null && displays.size() > 0) {
                        // 添加页面ID到 曝光流
                        String pageId = value.getJSONObject("page").getString("page_id");
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject displaysJSONObject = displays.getJSONObject(i);
                            displaysJSONObject.put("page_id", pageId);
                            ctx.output(displayOutputTag, displaysJSONObject.toJSONString());
                        }
                    }
                }
            }
        });
        //TODO 6.提取侧输出流
        DataStream<String> startSideOutput = pageDS.getSideOutput(startOutputTag);
        DataStream<String> displaySideOutput = pageDS.getSideOutput(displayOutputTag);
        //TODO 7.将三个流进行打印并输出到对应的Kafka主题中
        startSideOutput.print("启动日志>>>");
        pageDS.print("页面日志>>>");
        displaySideOutput.print("曝光日志");

        startSideOutput.addSink(MyKafkaUtil.getKafkaProducer("dwd_start_log"));
        pageDS.addSink(MyKafkaUtil.getKafkaProducer("dwd_page_log"));
        displaySideOutput.addSink(MyKafkaUtil.getKafkaProducer("dwd_display_log"));

        //TODO 8.启动任务
        env.execute();
    }
}