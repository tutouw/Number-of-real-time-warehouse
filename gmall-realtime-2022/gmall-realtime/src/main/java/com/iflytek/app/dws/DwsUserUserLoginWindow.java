package com.iflytek.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.iflytek.bean.UserLoginBean;
import com.iflytek.utils.DateFormatUtil;
import com.iflytek.utils.MyClickHouseUtil;
import com.iflytek.utils.MyKafkaUtil;
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
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 用户域
 * 用户登陆各窗口汇总表
 *
 * @author Aaron
 * @date 2022/6/28 15:08
 */

public class DwsUserUserLoginWindow {

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
        String topic = "dwd_traffic_page_log";
        String group_id = "dws_user_user_login_window";
        DataStreamSource<String> pageViewDS = env.addSource(MyKafkaUtil.getKafkaConsumer(topic, group_id));

        // TODO 3、将数据流转换为JSON类型
        // TODO 4、过滤数据 uid!=null && last_page_id为null
        //  数据：
        //  {"common":{"ar":"110000","uid":"99","os":"Android 11.0","ch":"oppo","is_new":"0","md":"Xiaomi 10 Pro ",
        //             "mid":"mid_982318","vc":"v2.0.1","ba":"Xiaomi"},
        //   "page":{"page_id":"good_detail","item":"17","during_time":2690,"item_type":"sku_id","last_page_id":"good_list",
        //          "source_type":"promotion"},
        //   "ts":1592193680000
        //  }
        SingleOutputStreamOperator<JSONObject> flatMapDS = pageViewDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                String uid = jsonObject.getJSONObject("common").getString("uid");
                String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");

                if (uid != null && lastPageId == null) {
                    out.collect(jsonObject);
                }
            }
        });

        // TODO 5、提取事件时间，生成watermark
        SingleOutputStreamOperator<JSONObject> watermarkDS = flatMapDS.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {

                    @Override
                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                        return element.getLong("ts");
                    }
                }));

        // TODO 6、按照uid分组
        KeyedStream<JSONObject, String> keyedStream = watermarkDS.keyBy(data -> data.getJSONObject("common").getString("uid"));

        // TODO 7、使用状态编程，实现回流以及独立用户的提取
        SingleOutputStreamOperator<UserLoginBean> userAndBackUserDS = keyedStream.flatMap(new RichFlatMapFunction<JSONObject, UserLoginBean>() {

            private ValueState<String> lastLoginDt;

            @Override
            public void open(Configuration parameters) throws Exception {
                lastLoginDt = getRuntimeContext().getState(new ValueStateDescriptor<String>("last_login_dt", String.class));
            }

            @Override
            public void flatMap(JSONObject value, Collector<UserLoginBean> out) throws Exception {
                Long ts = value.getLong("ts");
                String loginDt = DateFormatUtil.toYmdHms(ts);

                String lastLogin = lastLoginDt.value();

                Long uuCt = 0L;
                Long backCt = 0L;

                if (lastLogin == null) {
                    uuCt = 1L;
                    lastLoginDt.update(loginDt);
                } else {

                    if (!loginDt.equals(lastLogin)) {
                        uuCt = 1L;
                        lastLoginDt.update(loginDt);

                        Long days = (ts - DateFormatUtil.toTs(lastLogin)) / 1000 / 3600 / 24;
                        System.out.println(days);
                        if (days >= 8L) {
                            backCt = 1L;
                        }

                    }
                }


                if (uuCt == 1L) {
                    out.collect(new UserLoginBean("", "", backCt, uuCt, ts));
                }
            }
        });

        // TODO 8、开窗聚合
        SingleOutputStreamOperator<UserLoginBean> resultDS = userAndBackUserDS.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<UserLoginBean>() {
                    @Override
                    public UserLoginBean reduce(UserLoginBean value1, UserLoginBean value2) throws Exception {
                        value1.setUuCt(value1.getUuCt() + value2.getUuCt());
                        value1.setBackCt(value1.getBackCt() + value2.getBackCt());
                        return value1;
                    }
                }, new AllWindowFunction<UserLoginBean, UserLoginBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<UserLoginBean> values, Collector<UserLoginBean> out) throws Exception {
                        // 取出数据
                        UserLoginBean userLoginBean = values.iterator().next();
                        // 补充数据
                        userLoginBean.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                        userLoginBean.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                        // 输出数据
                        out.collect(userLoginBean);
                    }
                });
        // TODO 9、将数据写入到clickHouse
        resultDS.print(">>>>>>>>>>>>>");
        resultDS.addSink(MyClickHouseUtil.getClickHouseSink(
                "insert into dws_user_user_login_window(stt,edt,back_ct,uu_ct,ts) values(?,?,?,?,?)"));

        // TODO 10、执行
        env.execute("DwsUserUserLoginWindow");

    }
}
