package com.iflytek.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.iflytek.bean.CartAddUuBean;
import com.iflytek.utils.DateFormatUtil;
import com.iflytek.utils.MyClickHouseUtil;
import com.iflytek.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.apache.phoenix.schema.ValueSchema;

/**
 * 交易域
 * 加购各窗口汇总表
 * 从 Kafka 读取用户加购明细数据，统计各窗口加购独立用户数，写入 ClickHouse。
 *
 * @author Aaron
 * @date 2022/6/28 19:32
 */

public class DwsTradeCartAddUuWindow {
    public static void main(String[] args) throws Exception {
        // TODO 1、获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // env.setStateBackend(new HashMapStateBackend());
        // env.enableCheckpointing(5000L);
        // env.getCheckpointConfig().setCheckpointTimeout(10000L);
        // env.getCheckpointConfig().setCheckpointStorage("hdfs:hadoop101:9000//xxx/xx");

        // TODO 2、获取Kafka页面日志主题，创建流
        String topic = "dwd_trade_cart_add";
        String group_id = "dws_trade_cart_add_uu_window";
        DataStreamSource<String> cartAddDS = env.addSource(MyKafkaUtil.getKafkaConsumer(topic, group_id));

        // TODO 3、将数据转换为JSONObject
        SingleOutputStreamOperator<JSONObject> jsonObjDS = cartAddDS.map(JSON::parseObject);

        // TODO 4、设置水位线
        SingleOutputStreamOperator<JSONObject> jsonObjWithWMDS = jsonObjDS.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {

                    @Override
                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                        String dataTime = element.getString("operate_time");
                        if (dataTime == null){
                            dataTime = element.getString("create_time");
                        }

                        return DateFormatUtil.toTs(dataTime);
                    }
                }));

        // TODO 5、按照user_id分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjWithWMDS.keyBy(data -> data.getString("user_id"));

        // TODO 6、过滤独立用户加购记录
        SingleOutputStreamOperator<CartAddUuBean> cartAddDs = keyedStream.flatMap(new RichFlatMapFunction<JSONObject, CartAddUuBean>() {
            // 状态里面存放添加购物车时间
            private ValueState<String> addCartDtState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> add_cart_state = new ValueStateDescriptor<>("add_cart_state", String.class);
                add_cart_state.enableTimeToLive(new StateTtlConfig.Builder(Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build());

                addCartDtState = getRuntimeContext().getState(add_cart_state);
            }

            @Override
            public void flatMap(JSONObject value, Collector<CartAddUuBean> out) throws Exception {

                String create_time = value.getString("create_time");
                String curDt = create_time.split(" ")[0];
                String addCartDt = addCartDtState.value();


                // 如果状态里面不为空，判断两个时间是否相等，如果不相等，则是另外一天的数据
                if (addCartDt == null || !addCartDt.equals(curDt)) {
                    addCartDtState.update(curDt);
                    long cartAddUuCt = 1L;
                    out.collect(new CartAddUuBean("", "", cartAddUuCt, System.currentTimeMillis()));
                }
            }
        });

        // TODO 7、开窗聚合
        SingleOutputStreamOperator<CartAddUuBean> resultDS = cartAddDs.windowAll(TumblingProcessingTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)))
                .reduce(new ReduceFunction<CartAddUuBean>() {
                    @Override
                    public CartAddUuBean reduce(CartAddUuBean value1, CartAddUuBean value2) throws Exception {
                        value1.setCartAddUuCt(value1.getCartAddUuCt() + value2.getCartAddUuCt());
                        return value1;
                    }
                }, new AllWindowFunction<CartAddUuBean, CartAddUuBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<CartAddUuBean> values, Collector<CartAddUuBean> out) throws Exception {
                        CartAddUuBean next = values.iterator().next();
                        next.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                        next.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));

                        out.collect(next);
                    }
                });

        // TODO 8、写入clickHouse
        resultDS.print(">>>>>>>>");
        resultDS.addSink(MyClickHouseUtil.getClickHouseSink(
                "insert into dws_trade_cart_add_uu_window(stt,edt,cart_add_uu_ct,ts) values(?,?,?,?)"
        ));
        // TODO 9、执行
        env.execute("DwsTradeCartAddUuWindow");
    }
}
