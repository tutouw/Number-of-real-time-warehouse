package com.iflytek.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.iflytek.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.apache.flink.cep.pattern.Pattern.begin;

/**
 * 流量域
 * 用户跳出事实表
 *
 * @author Aaron
 * @date 2022/6/24 11:37
 */
// 数据流：web/app -> nginx -> 日志服务器落盘 -> flume -> kafka(ODS) -> flinkApp(BaseLogApp) -> kafka(DWD) -> flinkApp(DwdTrafficUniqueVisitorDetail) -> kafka(DWD)
// 程 序： mock(lg.sh) -> f1.sh(flume) -> kafka(zk) -> BaseLogApp -> kafka(zk) -> DwdTrafficUserJumpDetail -> kafka(zk)
public class DwdTrafficUserJumpDetail {
    public static void main(String[] args) throws Exception {
        // TODO 1、获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);  // 生产环境设置为kafka主题的分区数

        // env.setStateBackend(new HashMapStateBackend());
        // env.enableCheckpointing(5000L);
        // env.getCheckpointConfig().setCheckpointTimeout(10000L);
        // env.getCheckpointConfig().setCheckpointStorage("hdfs:hadoop101:9000//xxx/xx");

        // TODO 2、读取Kafka页面日志主题 dwd_traffic_page_log 主题数据创建流
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaConsumer("dwd_traffic_page_log", "dwd_traffic_user_jump_detail"));

        // TODO 3、将数据转换为JSON对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject);

        // TODO 4、提取事件时间生成watermark，处理乱序数据，一般生产环境中设置为最大延迟时间
        SingleOutputStreamOperator<JSONObject> jsonObjWithWatermarkDS = jsonObjDS.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {

                    @Override
                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                        return element.getLong("ts");
                    }
                }));

        // TODO 5、按照mid分组
        KeyedStream<JSONObject, String> keyedByMidStream = jsonObjWithWatermarkDS.keyBy(data -> data.getJSONObject("common").getString("mid"));
        // keyedByMidStream.print("xxxxxxx");

        // TODO 6、定义模式序列
        //  数据：
        //  {"common":{"ar":"110000","uid":"99","os":"Android 11.0","ch":"oppo","is_new":"0","md":"Xiaomi 10 Pro ",
        //             "mid":"mid_982318","vc":"v2.0.1","ba":"Xiaomi"},
        //   "page":{"page_id":"good_detail","item":"17","during_time":2690,"item_type":"sku_id","last_page_id":"good_list",
        //           "source_type":"promotion"},
        //   "ts":1592193680000
        //  }
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("first", AfterMatchSkipStrategy.skipToNext()).where(new SimpleCondition<JSONObject>() {
                    // Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("first").where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        return value.getJSONObject("page").getString("last_page_id") == null;
                    }
                }).next("second").where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        return value.getJSONObject("page").getString("last_page_id") == null;
                    }
                })
                .within(Time.seconds(10));

        // TODO 7、将模式序列作用到流上
        PatternStream<JSONObject> patternStream = CEP.pattern(keyedByMidStream, pattern);

        // TODO 8、提取超时事件和匹配上的事件
        OutputTag<JSONObject> timeoutTag = new OutputTag<JSONObject>("timeout") {
        };

        SingleOutputStreamOperator<JSONObject> selectDS = patternStream.select(timeoutTag, new PatternTimeoutFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject timeout(Map<String, List<JSONObject>> map, long l) throws Exception {
                return map.get("first").get(0);
            }
        }, new PatternSelectFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject select(Map<String, List<JSONObject>> map) throws Exception {
                return map.get("first").get(0);
            }
        });

        // TODO 9、合并两个事件流
        DataStream<JSONObject> timeoutDS = selectDS.getSideOutput(timeoutTag);
        DataStream<JSONObject> resultDS = selectDS.union(timeoutDS);

        // TODO 10、将数据写出到kafka
        selectDS.print("selectDS>>>>");
        timeoutDS.print("timeoutDS>>>");
        resultDS.map(JSONAware::toJSONString).addSink(MyKafkaUtil.getFlinkKafkaProducer("dwd_traffic_user_jump_detail"));

        // TODO 11、执行
        env.execute("DwdTrafficUserJumpDetail");
    }
}
