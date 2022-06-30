package com.iflytek.app.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.iflytek.utils.MyKafkaUtil;
import com.iflytek.utils.TimestampLtz3CompareUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author Aaron
 * @date 2022/6/29 18:39
 */

public class OrderDetailFilterFunction {
    public static SingleOutputStreamOperator<JSONObject> getDwdOrderDetail(StreamExecutionEnvironment env, String group_id) {
        // TODO 2、获取Kafka页面日志主题，创建流
        String topic = "dwd_trade_order_detail";
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
        return keyedByOrderDetailIdStream.process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {
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
    }
}
