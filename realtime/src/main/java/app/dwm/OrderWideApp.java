package app.dwm;

import app.function.DimAsyncFunction;
import bean.OrderDetail;
import bean.OrderInfo;
import bean.OrderWide;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import utils.MyKafkaUtil;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.concurrent.TimeUnit;

/**
 * //数据流：web/app -> nginx -> SpringBoot -> Mysql -> FlinkApp -> Kafka(ods) -> FlinkApp -> Kafka/Phoenix(dwd-dim) -> FlinkApp(redis) -> Kafka(dwm)
 * //程  序：         MockDb               -> Mysql -> FlinkCDC -> Kafka(ZK) -> BaseDbApp -> Kafka/Phoenix(zk/hdfs/hbase) -> OrderWideApp(Redis) -> Kafka
 * @author liufengting
 * @date 2021/12/29
 */
public class OrderWideApp {
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

        //TODO 2.读取Kafka 主题的数据 并转换为JavaBean对象&提取时间戳生成WaterMark
        String orderInfoSourceTopic = "dwd_order_info";
        String orderDetailSourceTopic = "dwd_order_detail";
        String orderWideSinkTopic = "dwm_order_wide";
        String groupId = "order_wide_group_0325";

        DataStreamSource<String> orderInfoKafkaDS = env.addSource(MyKafkaUtil.getKafkaConsumer(orderInfoSourceTopic, groupId));
        DataStreamSource<String> orderDetailKafkaDS = env.addSource(MyKafkaUtil.getKafkaConsumer(orderDetailSourceTopic, groupId));

//        orderInfoKafkaDS.print("orderInfoKafkaDS>>");
//        orderDetailKafkaDS.print("orderDetailKafkaDS");

        // 将每行数据转换为 JavaBean,提取时间戳生成 WaterMark
        WatermarkStrategy<OrderInfo> orderInfoWatermarkStrategy = WatermarkStrategy.<OrderInfo>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
                    @Override
                    public long extractTimestamp(OrderInfo element, long recordTimestamp) {
                        return element.getCreate_ts();
                    }
                });
        WatermarkStrategy<OrderDetail> orderDetailWatermarkStrategy = WatermarkStrategy.<OrderDetail>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderDetail>() {
                    @Override
                    public long extractTimestamp(OrderDetail element, long recordTimestamp) {
                        return element.getCreate_ts();
                    }
                });

        SingleOutputStreamOperator<OrderInfo> orderInfoDS = orderInfoKafkaDS.map(line -> {
            OrderInfo orderInfo = JSON.parseObject(line, OrderInfo.class);

            String create_time = orderInfo.getCreate_time();
            String[] dateTimeArr = create_time.split(" ");
            orderInfo.setCreate_date(dateTimeArr[0]);
            orderInfo.setCreate_hour(dateTimeArr[1].split(":")[0]);

            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            orderInfo.setCreate_ts(sdf.parse(create_time).getTime());

            return orderInfo;
        }).assignTimestampsAndWatermarks(orderInfoWatermarkStrategy);

        SingleOutputStreamOperator<OrderDetail> orderDetailDS = orderDetailKafkaDS.map(line -> {
            OrderDetail orderDetail = JSON.parseObject(line, OrderDetail.class);
            String create_time = orderDetail.getCreate_time();

            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            orderDetail.setCreate_ts(sdf.parse(create_time).getTime());

            return orderDetail;
        }).assignTimestampsAndWatermarks(orderDetailWatermarkStrategy);

        //TODO 3.双流JOIN
        SingleOutputStreamOperator<OrderWide> orderWideWithNoDimDS = orderInfoDS.keyBy(OrderInfo::getId)
                .intervalJoin(orderDetailDS.keyBy(OrderDetail::getOrder_id))
                .between(Time.seconds(-5), Time.seconds(5))//生产环境,为了不丢数据,设置时间为最大网络延迟
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo left, OrderDetail right, ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>.Context ctx, Collector<OrderWide> out) throws Exception {
                        out.collect(new OrderWide(left, right));
                    }
                });
//        打印测试
//        orderWideWithNoDimDS.print("orderWideWithNoDimDS>>>>>>>>>");

        //TODO 4.关联维度信息  HBase Phoenix
//        orderWideWithNoDimDS.map(orderWide -> {
//            //关联用户维度
//            Long user_id = orderWide.getUser_id();
//            //根据user_id查询Phoenix用户信息
//            //将用户信息补充至orderWide
//            //地区
//            //SKU
//            //SPU
//            //。。。
//            //返回结果
//            return orderWide;
//        });

        //4.1 关联用户维度
        SingleOutputStreamOperator<OrderWide> orderWideWithUserDS = AsyncDataStream.unorderedWait(
                orderWideWithNoDimDS,
                new DimAsyncFunction<OrderWide>("DIM_USER_INFO") {
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getUser_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) throws ParseException {
                        orderWide.setUser_gender(dimInfo.getString("GENDER"));

                        String birthday = dimInfo.getString("BIRTHDAY");
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

                        long currentTs = System.currentTimeMillis();
                        long ts = sdf.parse(birthday).getTime();

                        long age = (currentTs - ts) / (1000 * 60 * 60 * 24 * 365L);

                        orderWide.setUser_age((int) age);
                    }
                },
                60,
                TimeUnit.SECONDS);

        // 打印测试
//        orderWideWithUserDS.print("orderWideWithUserDS");

        //4.2 关联地区维度
        SingleOutputStreamOperator<OrderWide> orderWideWithProvinceDS = AsyncDataStream.unorderedWait(orderWideWithUserDS,
                new DimAsyncFunction<OrderWide>("DIM_BASE_PROVINCE") {
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getProvince_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) throws ParseException {
                        orderWide.setProvince_name(dimInfo.getString("NAME"));
                        orderWide.setProvince_area_code(dimInfo.getString("AREA_CODE"));
                        orderWide.setProvince_iso_code(dimInfo.getString("ISO_CODE"));
                        orderWide.setProvince_3166_2_code(dimInfo.getString("ISO_3166_2"));
                    }
                }, 60, TimeUnit.SECONDS);
//        orderWideWithProvinceDS.print("orderWideWithProvinceDS>>>");


        //4.3 关联SKU维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSkuDS = AsyncDataStream.unorderedWait(
                orderWideWithProvinceDS, new DimAsyncFunction<OrderWide>("DIM_SKU_INFO") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws ParseException {
                        orderWide.setSku_name(jsonObject.getString("SKU_NAME"));
                        orderWide.setCategory3_id(jsonObject.getLong("CATEGORY3_ID"));
                        orderWide.setSpu_id(jsonObject.getLong("SPU_ID"));
                        orderWide.setTm_id(jsonObject.getLong("TM_ID"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getSku_id());
                    }
                }, 60, TimeUnit.SECONDS);

        //4.4 关联SPU维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSpuDS = AsyncDataStream.unorderedWait(
                orderWideWithSkuDS, new DimAsyncFunction<OrderWide>("DIM_SPU_INFO") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws ParseException {
                        orderWide.setSpu_name(jsonObject.getString("SPU_NAME"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getSpu_id());
                    }
                }, 60, TimeUnit.SECONDS);

        //4.5 关联TM维度
        SingleOutputStreamOperator<OrderWide> orderWideWithTmDS = AsyncDataStream.unorderedWait(
                orderWideWithSpuDS, new DimAsyncFunction<OrderWide>("DIM_BASE_TRADEMARK") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws ParseException {
                        orderWide.setTm_name(jsonObject.getString("TM_NAME"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getTm_id());
                    }
                }, 60, TimeUnit.SECONDS);

        //4.6 关联Category维度
        SingleOutputStreamOperator<OrderWide> orderWideWithCategory3DS = AsyncDataStream.unorderedWait(
                orderWideWithTmDS, new DimAsyncFunction<OrderWide>("DIM_BASE_CATEGORY3") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws ParseException {
                        orderWide.setCategory3_name(jsonObject.getString("NAME"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getCategory3_id());
                    }
                }, 60, TimeUnit.SECONDS);

        orderWideWithCategory3DS.print("orderWideWithCategory3DS>>>>>>>>>>>");

        //TODO 5.将数据写入Kafka
        orderWideWithCategory3DS
                .map(JSONObject::toJSONString)
                .addSink(MyKafkaUtil.getKafkaProducer(orderWideSinkTopic));

        //TODO 6.启动任务
        env.execute("OrderWideApp");
    }
}