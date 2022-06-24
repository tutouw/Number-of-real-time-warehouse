package com.iflytek.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.iflytek.utils.DateFormatUtil;
import com.iflytek.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author Aaron
 * @date 2022/6/22 21:52
 */

// 数据流：web/app -> nginx -> 日志服务器(log) -> flume -> kafka(ODS) -> flinkApp -> kafka(DWD)
// 程  序：                mock(lg.sh)      -> f1.sh(flume) -> kafka(zk) -> BaseLogApp -> kafka(zk)
public class BaseLogApp {
    public static void main(String[] args) throws Exception {
        // TODO 1、获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);  // 生产环境设置为kafka主题的分区数

        // env.setStateBackend(new HashMapStateBackend());
        // env.enableCheckpointing(5000L);
        // env.getCheckpointConfig().setCheckpointTimeout(10000L);
        // env.getCheckpointConfig().setCheckpointStorage("hdfs:hadoop101:9000//xxx/xx");

        // TODO 2、读取Kafka topic_log 主题的数据创建流
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaConsumer("topic_log", "base_log_app"));

        // TODO 3、将数据转换为JSON格式，并过滤掉非JSON格式的数据
        OutputTag<String> dirtyTag = new OutputTag<String>("Dirty:") {
        };
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    ctx.output(dirtyTag, value);
                }
            }
        });
        DataStream<String> dirtyDS = jsonObjDS.getSideOutput(dirtyTag);
        dirtyDS.print("Dirty>>>>>>>>");

        // TODO 4、使用状态编程，做新老用户校验
        // value中的数据
        // {"common":{"ar":"230000", "ba":"Xiaomi", "ch":"xiaomi", "is_new":"1", "md":"Xiaomi 9", "mid":"mid_190245", "os":"Android 11.0", "uid":"625","vc":"v2.1.134"},
        //  "page":{"during_time":18934, "item":"4", "item_type":"sku_ids", "last_page_id":"trade", "page_id":"payment"},
        //  "ts":1592226244000
        // }

        // {"common":{"ar":"230000", "ba":"Xiaomi", "ch":"xiaomi", "is_new":"1", "md":"Xiaomi 9", "mid":"mid_190245", "os":"Android 11.0", "uid":"625", "vc":"v2.1.134"},
        //  "page":{"during_time":9984, "last_page_id":"good_detail", "page_id":"login"},
        //  "ts":1592226240000
        // }

        // {"common":{"ar":"230000", "ba":"Xiaomi","ch":"xiaomi","is_new":"1","md":"Xiaomi 9","mid":"mid_190245","os":"Android 11.0","uid":"625","vc":"v2.1.134"},
        //  "displays":[{"display_type":"activity", "item":"2", "item_type":"activity_id", "order":1, "pos_id":5},
        //              {"display_type":"activity", "item":"1", "item_type":"activity_id", "order":2, "pos_id":5},
        //              {"display_type":"query", "item":"6", "item_type":"sku_id", "order":3, "pos_id":5},
        //              {"display_type":"promotion", "item":"14", "item_type":"sku_id", "order":4, "pos_id":5},
        //              {"display_type":"query", "item":"4", "item_type":"sku_id", "order":5, "pos_id":3},
        //              {"display_type":"promotion", "item":"17", "item_type":"sku_id", "order":6, "pos_id":1},
        //              {"display_type":"query", "item":"17", "item_type":"sku_id", "order":7, "pos_id":3},
        //              {"display_type":"query", "item":"13", "item_type":"sku_id", "order":8, "pos_id":1},
        //              {"display_type":"query", "item":"17", "item_type":"sku_id", "order":9, "pos_id":1},
        //              {"display_type":"promotion", "item":"4", "item_type":"sku_id", "order":10, "pos_id":4},
        //              {"display_type":"query", "item":"24", "item_type":"sku_id", "order":11, "pos_id":1},
        //              {"display_type":"query", "item":"11", "item_type":"sku_id", "order":12, "pos_id":3}],
        //  "err":{"error_code":2030,"msg":" Exception in thread \\  java.net.SocketTimeoutException\\n \\tat com.atgugu.gmall2020.mock.bean.log.AppError.main(AppError.java:xxxxxx)"},
        //  "page":{"during_time":13971,"page_id":"home"},
        //  "ts":1592226236000
        // }
        KeyedStream<JSONObject, String> keyedByMidStream = jsonObjDS.keyBy(json -> json.getJSONObject("common").getString("mid"));

        SingleOutputStreamOperator<JSONObject> jsonObjectWithNewFlag = keyedByMidStream.map(new RichMapFunction<JSONObject, JSONObject>() {
            private ValueState<String> lastVisitDtState;

            @Override
            public void open(Configuration parameters) throws Exception {
                lastVisitDtState = getRuntimeContext().getState(new ValueStateDescriptor<String>("last-visit", String.class));
            }

            @Override
            public JSONObject map(JSONObject value) throws Exception {
                // 获取is_new标记
                String isNew = value.getJSONObject("common").getString("is_new");
                // 获取状态中的数据
                String lastVisitDt = lastVisitDtState.value();
                // 获取日志中"ts"字段，并进行处理
                String eventTime = DateFormatUtil.toDate(value.getLong("ts"));
                String lastDayEventTime = DateFormatUtil.toDate(value.getLong("ts") - 86400000);

                // 判断是否为 "1"
                if ("1".equals(isNew)) {
                    // 判断键控状态是否为null
                    if (lastVisitDt == null) {
                        lastVisitDtState.update(eventTime);
                    } else if (!lastVisitDt.equals(eventTime)) {
                        value.getJSONObject("common").put("is_new", "0");
                    }
                } else {
                    if (lastVisitDt == null) {
                        lastVisitDtState.update(lastDayEventTime);
                    }
                }

                return value;
            }
        });


        // TODO 5、使用侧输出流对数据进行分流处理
        //  页面浏览：主流
        //  启动日志：侧输出流
        //  曝光日志：侧输出流
        //  动作日志：侧输出流
        //  错误日志：侧输出流
        // 为什么要加大括号？加大括号何不加大括号的区别
        // 不加：相当于new了一个对象
        // 加： 相当于建了一个OutputTag匿名子类的对象
        // 如果不加，报错：类型缺失，OutputTag是一个泛型类，在编译的时候会类型擦除
        OutputTag<String> startOutputTag = new OutputTag<String>("start") {
        };
        OutputTag<String> displayOutputTag = new OutputTag<String>("display") {
        };
        OutputTag<String> actionOutputTag = new OutputTag<String>("action") {
        };
        OutputTag<String> errorOutputTag = new OutputTag<String>("error") {
        };

        // 分流操作
        SingleOutputStreamOperator<String> pageDS = jsonObjectWithNewFlag.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject value, ProcessFunction<JSONObject, String>.Context ctx, Collector<String> out) throws Exception {
                String valueString = value.toJSONString();

                // 尝试取出数据中的error字段
                String error = value.getString("err");
                // 如果不为空，则必然是错误信息，将其写到侧输出流
                if (error != null) {
                    ctx.output(errorOutputTag, valueString);
                }

                // 尝试获取启动日志
                String start = value.getString("start");
                if (start != null) {
                    ctx.output(startOutputTag, valueString);
                } else {
                    // 如果不是start，那必然是page
                    // page页面日志里面包含两类，曝光和动作 displays和actions
                    // 在曝光和动作日志中需要往里面加点东西，单个曝光或动作信息里面的可用数据比较少，所以我们把page_id和ts放进去
                    String page_id = value.getJSONObject("page").getString("page_id");
                    Long ts = value.getLong("ts");
                    JSONObject common = value.getJSONObject("common");

                    // 尝试获取displays曝光数据
                    JSONArray displays = value.getJSONArray("displays");
                    Displays_Actions(ctx, page_id, ts, common, displays, displayOutputTag);

                    // 尝试获取actions动作数据 处理方法和displays数据一样
                    JSONArray actions = value.getJSONArray("actions");
                    Displays_Actions(ctx, page_id, ts, common, actions, actionOutputTag);

                    // 已经把曝光和行为日志输出了，就不需要在page日志里面添加这两种了
                    value.remove("displays");
                    value.remove("actions");
                    out.collect(value.toJSONString());
                }
            }
        });

        // TODO 6、提取各个数据流的数据
        DataStream<String> startDS = pageDS.getSideOutput(startOutputTag);
        DataStream<String> displayDS = pageDS.getSideOutput(displayOutputTag);
        DataStream<String> actionDS = pageDS.getSideOutput(actionOutputTag);
        DataStream<String> errorDS = pageDS.getSideOutput(errorOutputTag);

        // TODO 7、将各个输流的数据分别写出到kafka对应的主题中
        pageDS.print("page:>>>>>>");
        startDS.print("start:>>>>>");
        displayDS.print("display:>>>");
        actionDS.print("action:>>>>");
        errorDS.print("error:>>>>>");

        // 定义不同日志输出到 Kafka 的主题名称
        String page_topic = "dwd_traffic_page_log";
        String start_topic = "dwd_traffic_start_log";
        String display_topic = "dwd_traffic_display_log";
        String action_topic = "dwd_traffic_action_log";
        String error_topic = "dwd_traffic_error_log";


        pageDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(page_topic));
        startDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(start_topic));
        displayDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(display_topic));
        actionDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(action_topic));
        errorDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(error_topic));

        // TODO 8、启动
        env.execute("baseLogApp");
    }

    private static void Displays_Actions(ProcessFunction<JSONObject, String>.Context ctx, String page_id, Long ts, JSONObject common, JSONArray jsonArray, OutputTag<String> OutputTag) {
        if (jsonArray!= null && jsonArray.size() > 0) {
            for (int i = 0; i < jsonArray.size(); i++) {
                JSONObject JSONObject = jsonArray.getJSONObject(i);
                // 放入数据
                JSONObject.put("page_id", page_id);
                JSONObject.put("ts", ts);
                JSONObject.put("common", common);

                ctx.output(OutputTag, JSONObject.toJSONString());
            }
        }
    }
}
