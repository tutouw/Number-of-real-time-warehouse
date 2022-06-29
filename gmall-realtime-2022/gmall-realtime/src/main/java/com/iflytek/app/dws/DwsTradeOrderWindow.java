package com.iflytek.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.iflytek.bean.TradeOrderBean;
import com.iflytek.utils.DateFormatUtil;
import com.iflytek.utils.MyClickHouseUtil;
import com.iflytek.utils.MyKafkaUtil;
import com.iflytek.utils.TimestampLtz3CompareUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.apache.phoenix.schema.ValueBitSet;

import java.time.Duration;

/**
 * 交易域
 * 下单各窗口汇总表
 * <p>
 * 从 Kafka 订单明细主题读取数据，过滤 null 数据并去重，
 * 统计当日下单独立用户数和新增下单用户数，封装为实体类，写入 ClickHouse。
 *
 * @author Aaron
 * @date 2022/6/29 10:09
 */

public class DwsTradeOrderWindow {
    public static void main(String[] args) throws Exception {
        // TODO 1、获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // env.setStateBackend(new HashMapStateBackend());
        // env.enableCheckpointing(5000L);
        // env.getCheckpointConfig().setCheckpointTimeout(10000L);
        // env.getCheckpointConfig().setCheckpointStorage("hdfs:hadoop101:9000//xxx/xx");

        // TODO 2、获取Kafka页面日志主题，创建流
        String topic = "dwd_trade_order_detail";
        String group_id = "dws_trade_order_window";
        DataStreamSource<String> orderDetailStrDS = env.addSource(MyKafkaUtil.getKafkaConsumer(topic, group_id));

        // TODO 3、过滤&转换数据为JSON数据 空值过滤，保留insert数据
        SingleOutputStreamOperator<JSONObject> jsonObjDS = orderDetailStrDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                if (!value.equals("")) {
                    JSONObject jsonObject = JSON.parseObject(value);
                    String type = jsonObject.getString("type");
                    if (type.equals("insert")) {
                        out.collect(jsonObject);
                    }
                }
            }
        });

        // TODO 4、按照order_detail_id分组
        KeyedStream<JSONObject, String> keyedByOrderDetailIdStream = jsonObjDS.keyBy(data -> data.getString("order_detail_id"));

        // TODO 5、去重
        SingleOutputStreamOperator<JSONObject> orderDetailJsonObjDS = keyedByOrderDetailIdStream.process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {
            private ValueState<JSONObject> orderDetailState;

            @Override
            public void open(Configuration parameters) throws Exception {
                orderDetailState = getRuntimeContext().getState(new ValueStateDescriptor<JSONObject>("order_detail", JSONObject.class));
            }

            @Override
            public void processElement(JSONObject value, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                // 获取状态数据，并判断是否有数据
                JSONObject orderDetail = orderDetailState.value();

                if (orderDetail == null) {
                    // 把当前数据设置设置进状态，并注册定时器
                    orderDetailState.update(value);
                    ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 2000L);
                } else {
                    // 比较当前数据时间和跟状态里面数据的ts字段的大小，大的保留，相等的保修后到的
                    String stateTs = orderDetail.getString("ts");
                    String curTs = value.getString("ts");
                    // 将两个时间使用工具类进行比较
                    int compare = TimestampLtz3CompareUtil.compare(stateTs, curTs);
                    if (compare <= 0) {
                        // 说明后到的数据时间大或者相等
                        orderDetailState.update(value);
                    }
                }
            }


            @Override
            public void onTimer(long timestamp, KeyedProcessFunction<String, JSONObject, JSONObject>.OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
                // 提取状态数据并输出，定时器响了之后，状态必然不为空
                JSONObject orderDetail = orderDetailState.value();
                out.collect(orderDetail);
            }
        });

        // TODO 6、提取事件时间生成watermark
        SingleOutputStreamOperator<JSONObject> jsonObjWIthWMDS = orderDetailJsonObjDS.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {


                    @Override
                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                        String createTime = element.getString("order_create_time");
                        return DateFormatUtil.toTs(createTime, true);
                    }
                }));

        // TODO 7、按照user_id分组
        KeyedStream<JSONObject, String> keyedByUidStream = jsonObjWIthWMDS.keyBy(data -> data.getString("user_id"));

        // TODO 8、提取下单独立用户 状态编程 并转换为JavaBean对象
        SingleOutputStreamOperator<TradeOrderBean> tradeOrderDS = keyedByUidStream.flatMap(new RichFlatMapFunction<JSONObject, TradeOrderBean>() {
            private ValueState<String> orderLastDt;

            @Override
            public void open(Configuration parameters) throws Exception {
                orderLastDt = getRuntimeContext().getState(new ValueStateDescriptor<String>("last_order", String.class));
            }

            @Override
            public void flatMap(JSONObject value, Collector<TradeOrderBean> out) throws Exception {
                // 取出状态时间
                String orderLastTime = orderLastDt.value();
                // 取出当前下单时间
                String curTime = value.getString("order_create_time").split(" ")[0];
                // 定义下单数和新增下单数
                long orderUniqueUserCount = 0L;
                long orderNewUserCount = 0L;

                if (orderLastTime == null) {
                    orderUniqueUserCount = 1L;
                    orderNewUserCount = 1L;
                    orderLastDt.update(curTime);
                } else if (!curTime.equals(orderLastTime)) {
                    // 如果这两个日期不相等
                    orderUniqueUserCount = 1L;
                    orderLastDt.update(curTime);
                }

                Double activity_reduce_amount = value.getDouble("activity_reduce_amount");
                if (activity_reduce_amount == null) {
                    activity_reduce_amount = 0.0D;
                }

                Double coupon_reduce_amount = value.getDouble("coupon_reduce_amount");
                if (coupon_reduce_amount == null) {
                    coupon_reduce_amount = 0.0D;
                }

                // 输出数据
                out.collect(new TradeOrderBean("", "",
                        orderUniqueUserCount,
                        orderNewUserCount,
                        activity_reduce_amount,
                        coupon_reduce_amount,
                        value.getDouble("original_total_amount"),
                        System.currentTimeMillis()));
            }
        });

        // TODO 9、开窗聚合
        SingleOutputStreamOperator<TradeOrderBean> resultDS = tradeOrderDS.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<TradeOrderBean>() {
                    @Override
                    public TradeOrderBean reduce(TradeOrderBean value1, TradeOrderBean value2) throws Exception {
                        value1.setOrderActivityReduceAmount(value1.getOrderActivityReduceAmount() + value2.getOrderActivityReduceAmount());
                        value1.setOrderCouponReduceAmount(value1.getOrderCouponReduceAmount() + value2.getOrderCouponReduceAmount());
                        value1.setOrderOriginalTotalAmount(value1.getOrderOriginalTotalAmount() + value2.getOrderOriginalTotalAmount());
                        value1.setOrderUniqueUserCount(value1.getOrderUniqueUserCount() + value2.getOrderUniqueUserCount());
                        value1.setOrderNewUserCount(value1.getOrderNewUserCount() + value2.getOrderNewUserCount());

                        return value1;
                    }
                }, new AllWindowFunction<TradeOrderBean, TradeOrderBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<TradeOrderBean> values, Collector<TradeOrderBean> out) throws Exception {
                        TradeOrderBean next = values.iterator().next();
                        next.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                        next.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));

                        out.collect(next);
                    }
                });

        // TODO 10、将数据输出到clickHouse
        resultDS.print(">>>>>>>");
        resultDS.addSink(MyClickHouseUtil.getClickHouseSink(
                "insert into dws_trade_order_window(stt,edt,order_unique_user_count,order_new_user_count,order_activity_reduce_amount,order_coupon_reduce_amount,order_origin_total_amount,ts) values(?,?,?,?,?,?,?,?)"
        ));

        // TODO 11、启动任务
        env.execute("DwsTradeOrderWindow");

    }
}

