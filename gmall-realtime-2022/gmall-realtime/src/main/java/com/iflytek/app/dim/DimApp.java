package com.iflytek.app.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.iflytek.app.func.DimSinkFunction;
import com.iflytek.app.func.TableProcessFunction;
import com.iflytek.bean.TableProcess;
import com.iflytek.utils.MyKafkaUtil;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author Aaron
 * @date 2022/6/18 22:45
 */

// 数据流：web/app -> nginx -> 业务服务器 -> mysql(binlog) -> maxwell -> kafka -> flinkApp -> phoenix(hbase)(DIM)
// 程 序：         mock                 -> mysql(binlog) -> maxwell.sh -> kafka(zk) -> dimApp -> phoenix(hbase,zk/hdfs)
public class DimApp {
    public static void main(String[] args) throws Exception {
        // TODO 1、获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);  // 生产环境设置为kafka主题的分区数

        // env.setStateBackend(new HashMapStateBackend());
        // env.enableCheckpointing(5000L);
        // env.getCheckpointConfig().setCheckpointTimeout(10000L);
        // env.getCheckpointConfig().setCheckpointStorage("hdfs:hadoop101:9000//xxx/xx");

        // TODO 2、读取kafka topic_db主题的数据创建流
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaConsumer("gmall_db", "dim_app"));
        // kafkaDS.print("kafka");

        // TODO 3、过滤掉非JSON格式的数据，并将其写入到侧输出流
        OutputTag<String> DirtyDataTag = new OutputTag<String>("Dirty") {
        };
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    ctx.output(DirtyDataTag, value);
                }
            }
        });
        // jsonObjDS.print("jsonObjDS");

        // 取出脏数据，并打印
        DataStream<String> sideOutput = jsonObjDS.getSideOutput(DirtyDataTag);
        sideOutput.print("Dirty");

        // TODO 4、使用FlinkCDC读取MySQL中的配置信息
        /*DebeziumSourceFunction<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("hadoop101")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmall-config")
                .tableList("gmall-config.table_process")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .build();

        // DataStreamSource<String> mysqlSourceDS = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MysqlSource");
        DataStreamSource<String> mysqlSourceDS = env.addSource(mySqlSource);
        mysqlSourceDS.print();*/
        DataStreamSource<String> mysqlSourceDS = env.addSource(MyKafkaUtil.getKafkaConsumer("gmall-config_db", "dim_app"));

        mysqlSourceDS.print("mysqlSourceDS");
        // {"database":"gmall-config","table":"table_process","type":"insert","ts":1592147978,"xid":68391,"commit":true,"data":{"source_table":"base_category1","sink_table":"dim_base_category1","sink_columns":"id,name","sink_pk":null,"sink_extend":null}}

        // TODO 5、将配置信息流处理成广播流
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>("map-State", String.class, TableProcess.class);

        BroadcastStream<String> broadcastStream = mysqlSourceDS.broadcast(mapStateDescriptor);

        // TODO 6、连接主流和广播流
        BroadcastConnectedStream<JSONObject, String> connectedStream = jsonObjDS.connect(broadcastStream);

        // TODO 7、根据广播流数据处理主流数据
        SingleOutputStreamOperator<JSONObject> hbaseDS = connectedStream.process(new TableProcessFunction(mapStateDescriptor));

        // TODO 8、将数据写出到Phoenix中
        hbaseDS.print("hbase");
        hbaseDS.addSink(new DimSinkFunction());


        // TODO 9、启动任务
        env.execute("DimApp");
    }
}
