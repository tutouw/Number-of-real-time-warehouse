package com.iflytek.app.dws;

import akka.japi.tuple.Tuple4;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.iflytek.bean.TrafficPageViewBean;
import com.iflytek.utils.DateFormatUtil;
import com.iflytek.utils.MyClickHouseUtil;
import com.iflytek.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 流量域
 * 版本-渠道-地区-访客类别粒度 页面浏览各窗口汇总表
 * Vc  Ch   Ar  IsNew     PageView
 * @author Aaron
 * @date 2022/6/27 22:34
 */

public class DwsTrafficVcChArIsNewPageViewWindow {
    public static void main(String[] args) throws Exception {
        // TODO 1、获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // env.setStateBackend(new HashMapStateBackend());
        // env.enableCheckpointing(5000L);
        // env.getCheckpointConfig().setCheckpointTimeout(10000L);
        // env.getCheckpointConfig().setCheckpointStorage("hdfs:hadoop101:9000//xxx/xx");

        // TODO 2、读取3个主题的数据创建三个流
        String page_topic ="dwd_traffic_page_log";
        String uv_topic ="dwd_traffic_unique_visitor_detail";
        String uj_topic ="dwd_traffic_user_jump_detail";
        String group_id = "dws_traffic_vc_ch_ar_isNew_pageView_window";

        DataStreamSource<String> pageStringDS = env.addSource(MyKafkaUtil.getKafkaConsumer(page_topic, group_id));
        DataStreamSource<String> uvStringDS = env.addSource(MyKafkaUtil.getKafkaConsumer(uv_topic, group_id));
        DataStreamSource<String> ujStringDS = env.addSource(MyKafkaUtil.getKafkaConsumer(uj_topic, group_id));

        // TODO 3、将三个流统一格式 JavaBean 为了合并流的时候方便

        //  处理uj数据
        SingleOutputStreamOperator<TrafficPageViewBean> trafficPageViewWithUjDS = ujStringDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            JSONObject common = jsonObject.getJSONObject("common");

            return new TrafficPageViewBean("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    1L, 0L, 0L, 0L, 0L,
                    jsonObject.getLong("ts")
            );
        });

        //  处理uv数据
        SingleOutputStreamOperator<TrafficPageViewBean> trafficPageViewWithUvDS = uvStringDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            JSONObject common = jsonObject.getJSONObject("common");

            return new TrafficPageViewBean("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L, 0L, 0L, 0L, 1L,
                    jsonObject.getLong("ts")
            );
        });

        //  处理page数据
        SingleOutputStreamOperator<TrafficPageViewBean> trafficPageViewWithPageDS = pageStringDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            JSONObject common = jsonObject.getJSONObject("common");
            Long during_time = jsonObject.getJSONObject("page").getLong("during_time");
            String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
            long sv = 0L;
            if (lastPageId == null){
                sv = 1L;
            }


            return new TrafficPageViewBean("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L,
                    sv,
                    1L,
                    during_time,
                    0L,
                    jsonObject.getLong("ts")
            );
        });

        // TODO 4、合并三个流，提取事件时间生成watermark
        DataStream<TrafficPageViewBean> unionDS = trafficPageViewWithPageDS
                .union(trafficPageViewWithUjDS)
                .union(trafficPageViewWithUvDS)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<TrafficPageViewBean>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<TrafficPageViewBean>(){

                            @Override
                            public long extractTimestamp(TrafficPageViewBean element, long recordTimestamp) {
                                return element.getTs();
                            }
                        }));

        // TODO 5、分组、开窗、聚合
        KeyedStream<TrafficPageViewBean, Tuple4<String, String, String, String>> keyedStream = unionDS.keyBy(new KeySelector<TrafficPageViewBean, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> getKey(TrafficPageViewBean value) throws Exception {
                return new Tuple4<>(value.getAr(), value.getCh(), value.getVc(), value.getIsNew());
            }
        });

        WindowedStream<TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow> windowedStream = keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));

        SingleOutputStreamOperator<TrafficPageViewBean> resultDS = windowedStream.reduce(new ReduceFunction<TrafficPageViewBean>() {
            @Override
            public TrafficPageViewBean reduce(TrafficPageViewBean value1, TrafficPageViewBean value2) throws Exception {
                value1.setUvCt(value1.getUvCt() + value2.getUvCt());
                value1.setSvCt(value1.getSvCt() + value2.getSvCt());
                value1.setPvCt(value1.getPvCt() + value2.getPvCt());
                value1.setDurSum(value1.getDurSum() + value2.getDurSum());
                value1.setUjCt(value1.getUjCt() + value2.getUjCt());
                return value1;
            }
        }, new WindowFunction<TrafficPageViewBean, TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow>() {

            @Override
            public void apply(Tuple4<String, String, String, String> key, TimeWindow window, Iterable<TrafficPageViewBean> input, Collector<TrafficPageViewBean> out) throws Exception {
                // 获取数据
                TrafficPageViewBean trafficPageView = input.iterator().next();
                // 获取窗口信息
                long start = window.getStart();
                long end = window.getEnd();

                // 补充窗口信息
                trafficPageView.setStt(DateFormatUtil.toYmdHms(start));
                trafficPageView.setEdt(DateFormatUtil.toYmdHms(end));

                // 输出数据
                out.collect(trafficPageView);
            }
        });

        // TODO 6、将数据写出到clickHouse
        resultDS.print(">>>>>>>>");
        resultDS.addSink(MyClickHouseUtil.getClickHouseSink("" +
                "insert into dws_traffic_vc_ch_ar_is_new_page_view_window " +
                "values(?,?,?,?,?,?,?,?,?,?,?,?)"));

        // TODO 7、启动任务
        env.execute("DwsTrafficVcChArIsNewPageViewWindow");
    }

}
