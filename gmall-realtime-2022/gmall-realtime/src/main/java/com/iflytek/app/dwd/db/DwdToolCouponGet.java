package com.iflytek.app.dwd.db;


import com.iflytek.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 工具域
 * 优惠券领取事务事实表
 *
 * @author Aaron
 * @date 2022/6/26 14:22
 */
public class DwdToolCouponGet {
    public static void main(String[] args) throws Exception {

        // TODO 1. 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // TODO 2. 状态后端设置
        // env.setStateBackend(new HashMapStateBackend());
        // env.enableCheckpointing(5000L);
        // env.getCheckpointConfig().setCheckpointTimeout(10000L);
        // env.getCheckpointConfig().setCheckpointStorage("hdfs:hadoop101:9000//xxx/xx");

        // TODO 3. 从 Kafka 读取业务数据，封装为 Flink SQL 表
        tableEnv.executeSql("create table `gmall_db`( " +
                "`database` string, " +
                "`table` string, " +
                "`data` map<string, string>, " +
                "`type` string, " +
                "`ts` string " +
                ")" + MyKafkaUtil.getKafkaDDL("gmall_db", "dwd_tool_coupon_get"));

        // TODO 4. 读取优惠券领用数据，封装为表
        Table resultTable = tableEnv.sqlQuery("select " +
                "data['id'], " +
                "data['coupon_id'], " +
                "data['user_id'], " +
                "date_format(data['get_time'],'yyyy-MM-dd') date_id, " +
                "data['get_time'], " +
                "ts " +
                "from gmall_db " +
                "where `table` = 'coupon_use' " +
                "and `type` = 'insert' ");
        tableEnv.createTemporaryView("result_table", resultTable);

        // TODO 5. 建立 Upsert-Kafka dwd_tool_coupon_get 表
        tableEnv.executeSql("create table dwd_tool_coupon_get ( " +
                "id string, " +
                "coupon_id string, " +
                "user_id string, " +
                "date_id string, " +
                "get_time string, " +
                "ts string, " +
                "primary key(id) not enforced " +
                ")" + MyKafkaUtil.getUpsertKafkaDDL("dwd_tool_coupon_get"));

        // TODO 6. 将数据写入 Upsert-Kafka 表
        tableEnv.executeSql("insert into dwd_tool_coupon_get select * from result_table").print();
        env.execute("DwdToolCouponGet");
    }
}
