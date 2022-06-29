package com.iflytek.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.iflytek.bean.UserRegisterBean;
import com.iflytek.utils.DateFormatUtil;
import com.iflytek.utils.MyClickHouseUtil;
import com.iflytek.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
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
 * 用户注册各窗口汇总表
 *
 * @author Aaron
 * @date 2022/6/28 18:37
 */

public class DwsUserUserRegisterWindow {
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
        String topic = "dwd_user_register";
        String group_id = "dws_user_user_register_window";
        DataStreamSource<String> pageViewDS = env.addSource(MyKafkaUtil.getKafkaConsumer(topic, group_id));

        // TODO 3、将数据转换为javaBean格式
        SingleOutputStreamOperator<UserRegisterBean> userRegisterDS = pageViewDS.map(data -> new UserRegisterBean("", "", 1L, JSON.parseObject(data).getLong("ts") * 1000));

        // TODO 4、生成水位线
        SingleOutputStreamOperator<UserRegisterBean> userRegisterWithWMDS = userRegisterDS.assignTimestampsAndWatermarks(WatermarkStrategy.<UserRegisterBean>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<UserRegisterBean>() {

                    @Override
                    public long extractTimestamp(UserRegisterBean element, long recordTimestamp) {
                        return element.getTs();
                    }
                }));

        // TODO 5、开窗聚合
        SingleOutputStreamOperator<UserRegisterBean> resultDs = userRegisterWithWMDS.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<UserRegisterBean>() {
                    @Override
                    public UserRegisterBean reduce(UserRegisterBean value1, UserRegisterBean value2) throws Exception {
                        value1.setRegisterCt(value1.getRegisterCt() + value2.getRegisterCt());
                        return value1;
                    }
                }, new AllWindowFunction<UserRegisterBean, UserRegisterBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<UserRegisterBean> values, Collector<UserRegisterBean> out) throws Exception {
                        UserRegisterBean next = values.iterator().next();
                        next.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                        next.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                        out.collect(next);
                    }
                });

        // TODO 6、将数据写出到ClickHouse
        resultDs.print(">>>>>>>>>");
        resultDs.addSink(MyClickHouseUtil.getClickHouseSink(
                "insert into dws_user_user_register_window(stt,edt,register_ct,ts) values(?,?,?,?)"));

        // TODO 7、执行
        env.execute("DwsUserUserRegisterWindow");

    }
}
