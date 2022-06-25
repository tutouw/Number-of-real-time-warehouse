package com.iflytek.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.iflytek.utils.DateFormatUtil;
import com.iflytek.utils.MyKafkaUtil;
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


/**
 * 流量域
 * 独立访客事实表
 *
 * @author Aaron
 * @date 2022/6/23 16:46
 */
// 数据流：web/app -> nginx -> 日志服务器落盘 -> flume -> kafka(ODS) -> flinkApp(BaseLogApp) -> kafka(DWD) -> flinkApp(DwdTrafficUniqueVisitorDetail) -> kafka(DWD)
// 程 序： mock(lg.sh) -> f1.sh(flume) -> kafka(zk) -> BaseLogApp -> kafka(zk) -> DwdTrafficUniqueVisitorDetail -> kafka(zk)
public class DwdTrafficUniqueVisitorDetail {

    public static void main(String[] args) throws Exception {
        // TODO 1、获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);  // 生产环境设置为kafka主题的分区数

        // env.setStateBackend(new HashMapStateBackend());
        // env.enableCheckpointing(5000L);
        // env.getCheckpointConfig().setCheckpointTimeout(10000L);
        // env.getCheckpointConfig().setCheckpointStorage("hdfs:hadoop101:9000//xxx/xx");

        // TODO 2、读取Kafka页面日志主题 dwd_traffic_page_log 主题数据创建流
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaConsumer("dwd_traffic_page_log", "dwd_traffic_unique_visitor_detail"));
        // TODO 3、将每行数据转换为JSON对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject);

        // TODO 4、过滤掉上一跳页面id不等于null的数据 filter
        //  数据：
        //  {"common":{"ar":"110000","uid":"99","os":"Android 11.0","ch":"oppo","is_new":"0","md":"Xiaomi 10 Pro ",
        //             "mid":"mid_982318","vc":"v2.0.1","ba":"Xiaomi"},
        //   "page":{"page_id":"good_detail","item":"17","during_time":2690,"item_type":"sku_id","last_page_id":"good_list",
        //          "source_type":"promotion"},
        //   "ts":1592193680000
        //  }
        SingleOutputStreamOperator<JSONObject> filterDS = jsonObjDS.filter(data -> data.getJSONObject("page").getString("last_page_id") == null);

        // TODO 5、按照mid分组
        KeyedStream<JSONObject, String> keyedStream = filterDS.keyBy(data -> data.getJSONObject("common").getString("mid"));

        // TODO 6、使用状态编程，进行每日登陆数据去重
        SingleOutputStreamOperator<JSONObject> filterDistinctDS = keyedStream.filter(new RichFilterFunction<JSONObject>() {
            // 定义一个状态来保存日期
            private ValueState<String> dateState;

            @Override
            public void open(Configuration parameters) {

                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("dataState", String.class);
                // 对状态设置过期时间，TTL，并且设置状态更新策略，每次更改的时候重置存活时间
                valueStateDescriptor.enableTimeToLive(new StateTtlConfig
                        .Builder(Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build());
                // 初始化状态
                dateState = getRuntimeContext().getState(valueStateDescriptor);
            }

            @Override
            public boolean filter(JSONObject value) throws Exception {
                // 去除当前日期和状态
                String date = DateFormatUtil.toDate(value.getLong("ts"));
                String dateStateValue = dateState.value();
                // if (dateStateValue == null || !date.equals(dateStateValue)) {
                if (!date.equals(dateStateValue)) {
                    // 状态为空，表示第一次来数据，保留数据，状态不为空，且数据日期和状态不一样，都保留数据
                    // 简写：状态为空不为空对判断结果没影响
                    dateState.update(date);
                    return true;
                } else {
                    // 当不是第一次来数据的时候，判断日期是不是和当前状态一样，如果一样，过滤掉数据
                    return false;
                }
            }
        });

        // TODO 7、将数据写出到Kafka
        // filterDistinctDS.print(">>>>>");
        // 我们写好的方法需要的是String类型，而我们提供的是JSONObject，所以需要先转换一下
        filterDistinctDS.map(JSONAware::toJSONString).addSink(MyKafkaUtil.getFlinkKafkaProducer("dwd_traffic_unique_visitor_detail"));

        // TODO 8、启动任务
        env.execute();
    }
}
