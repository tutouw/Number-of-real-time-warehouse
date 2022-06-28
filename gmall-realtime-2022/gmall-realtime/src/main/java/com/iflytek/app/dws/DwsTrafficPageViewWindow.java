package com.iflytek.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.iflytek.bean.TrafficHomeDetailPageViewBean;
import com.iflytek.utils.DateFormatUtil;
import com.iflytek.utils.MyClickHouseUtil;
import com.iflytek.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
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

import java.time.Duration;

/**
 * 流量域
 * 页面浏览各窗口汇总表
 *
 * @author Aaron
 * @date 2022/6/28 12:11
 */

public class DwsTrafficPageViewWindow {
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
        String group_id = "dws_traffic_pageView_window";
        DataStreamSource<String> pageViewDS = env.addSource(MyKafkaUtil.getKafkaConsumer(topic, group_id));

        // TODO 3、将数据流转换成JSON格式
        // SingleOutputStreamOperator<JSONObject> jsonObjDS = pageViewDS.map(JSON::parseObject);

        // TODO 4、过滤数据，只需要访问主页跟详情页的数据
        /* SingleOutputStreamOperator<JSONObject> homeAndGoodDetailDS = jsonObjDS.filter(data -> {
            String page_id = data.getJSONObject("page").getString("page_id");
            return page_id.equals("home") || page_id.equals("good_detail");
        });*/

        // TODO 使用flatMap代替3，4
        SingleOutputStreamOperator<JSONObject> homeAndGoodDetailDS = pageViewDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                String page_id = jsonObject.getJSONObject("page").getString("page_id");
                if (page_id.equals("home") || page_id.equals("good_detail")) {
                    out.collect(jsonObject);
                }
            }
        });

        // TODO 5、提取事件时间生成watermark
        SingleOutputStreamOperator<JSONObject> homeAndGoodDetailWithWMDS = homeAndGoodDetailDS.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                        return element.getLong("ts");
                    }
                }));

        // TODO 6、按照mid分组
        KeyedStream<JSONObject, String> keyedStream = homeAndGoodDetailWithWMDS.keyBy(json -> json.getJSONObject("common").getString("mid"));

        // TODO 7、使用状态编程计算主页以及商品详情页的独立访客记录
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> trafficHomeDetailDS = keyedStream.flatMap(new RichFlatMapFunction<JSONObject, TrafficHomeDetailPageViewBean>() {
            // 状态里面存放日期，每天日期不一样就会算到第二天的数据里面
            private ValueState<String> homeLastVisitDt;
            private ValueState<String> detailLastVisitDt;

            @Override
            public void open(Configuration parameters) throws Exception {
                // 由于时间是按天计算的，但是状态会一直保存，所以我们把状态生存时间保存为一天，写入更新
                ValueStateDescriptor<String> home_last_visit_dt = new ValueStateDescriptor<>("home_last_visit_dt", String.class);
                ValueStateDescriptor<String> detail_last_visit_dt = new ValueStateDescriptor<>("detail_last_visit_dt", String.class);

                // 对状态生存时间进行设置
                StateTtlConfig stateTtlConfig = new StateTtlConfig.Builder(Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();

                // 将状态设置作用到具体的状态上
                home_last_visit_dt.enableTimeToLive(stateTtlConfig);
                // 对good_detail页面的状态生存时间进行设置
                home_last_visit_dt.enableTimeToLive(stateTtlConfig);

                homeLastVisitDt = getRuntimeContext().getState(home_last_visit_dt);
                detailLastVisitDt = getRuntimeContext().getState(detail_last_visit_dt);
            }

            @Override
            public void flatMap(JSONObject value, Collector<TrafficHomeDetailPageViewBean> out) throws Exception {
                // 去除当前页面信息以及时间
                String page_id = value.getJSONObject("page").getString("page_id");
                Long ts = value.getLong("ts");
                String curDt = DateFormatUtil.toDate(ts);

                // 定义主页和商品详情页的访问次数
                Long homeUvCt = 0L;
                Long detailUvCt = 0L;

                // 判断是否为主页日志
                if ("home".equals(page_id)) {
                    // 判断状态，以及和当前日期是否相同
                    String homeLastDt = homeLastVisitDt.value();
                    if (homeLastDt == null || !homeLastDt.equals(curDt)) {
                        homeUvCt = 1L;
                        homeLastVisitDt.update(homeLastDt);
                    }


                } else {
                    // 剩下的就是商品详情页
                    // 判断状态，以及和当前日期是否相同
                    String detailLastDt = detailLastVisitDt.value();
                    if (detailLastDt == null || !detailLastDt.equals(curDt)) {
                        detailUvCt = 1L;
                        detailLastVisitDt.update(detailLastDt);
                    }
                }

                // 封装JavaBean并写出数据
                if (homeUvCt != 0L || detailUvCt != 0L) {
                    out.collect(new TrafficHomeDetailPageViewBean(
                            "",
                            "",
                            homeUvCt,
                            detailUvCt,
                            System.currentTimeMillis()));
                }

            }
        });

        // TODO 8、开窗聚合
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> reduce = trafficHomeDetailDS.windowAll(TumblingProcessingTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)))
                .reduce(new ReduceFunction<TrafficHomeDetailPageViewBean>() {
                    @Override
                    public TrafficHomeDetailPageViewBean reduce(TrafficHomeDetailPageViewBean value1, TrafficHomeDetailPageViewBean value2) throws Exception {
                        // 两个窗口聚合
                        value1.setHomeUvCt(value2.getHomeUvCt() + value1.getHomeUvCt());
                        value1.setGoodDetailUvCt(value2.getGoodDetailUvCt() + value1.getGoodDetailUvCt());
                        return value1;
                    }
                }, new AllWindowFunction<TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<TrafficHomeDetailPageViewBean> values, Collector<TrafficHomeDetailPageViewBean> out) throws Exception {
                        // 获取数据
                        TrafficHomeDetailPageViewBean pageViewBean = values.iterator().next();
                        // 设置窗口信息
                        long start = window.getStart();
                        long end = window.getEnd();

                        pageViewBean.setStt(DateFormatUtil.toYmdHms(start));
                        pageViewBean.setEdt(DateFormatUtil.toYmdHms(end));
                        // 输出数据
                        out.collect(pageViewBean);
                    }
                });

        // TODO 9、将数据写出到clickHouse
        reduce.print(">>>>>>>>>");
        reduce.addSink(MyClickHouseUtil.getClickHouseSink(
                "insert into dws_traffic_page_view_window(stt,edt,home_uv_ct,good_detail_uv_ct,ts) values(?,?,?,?,?) "
        ));

        // TODO 10、启动任务
        env.execute("DwsTrafficPageViewWindow");

    }
}
