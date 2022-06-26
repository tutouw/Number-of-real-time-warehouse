package com.iflytek.app.dwd.db;


import com.iflytek.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.ZoneId;

/**
 * 用户域
 * 用户注册事务事实表
 *
 * @author Aaron
 * @date 2022/6/26 15:28
 */
public class DwdUserRegister {
    public static void main(String[] args) throws Exception {

        // TODO 1. 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.getConfig().setLocalTimeZone(ZoneId.of("GMT+8"));

        // TODO 2. 启用状态后端
        // env.setStateBackend(new HashMapStateBackend());
        // env.enableCheckpointing(5000L);
        // env.getCheckpointConfig().setCheckpointTimeout(10000L);
        // env.getCheckpointConfig().setCheckpointStorage("hdfs:hadoop101:9000//xxx/xx");

        // TODO 3. 从 Kafka 读取业务数据，封装为 Flink SQL 表
        tableEnv.executeSql("create table gmall_db(" +
                "`database` string, " +
                "`table` string, " +
                "`type` string, " +
                "`data` map<string, string>, " +
                "`ts` string " +
                ")" + MyKafkaUtil.getKafkaDDL("gmall_db", "dwd_trade_order_detail"));

        // TODO 4. 读取用户表数据
        Table userInfo = tableEnv.sqlQuery("select " +
                "data['id'] user_id, " +
                "data['create_time'] create_time, " +
                "ts " +
                "from gmall_db " +
                "where `table` = 'user_info' " +
                "and `type` = 'insert' ");
        tableEnv.createTemporaryView("user_info", userInfo);

        // TODO 5. 创建 Upsert-Kafka dwd_user_register 表
        tableEnv.executeSql("create table `dwd_user_register`( " +
                "`user_id` string, " +
                "`date_id` string, " +
                "`create_time` string, " +
                "`ts` string, " +
                "primary key(`user_id`) not enforced " +
                ")" + MyKafkaUtil.getUpsertKafkaDDL("dwd_user_register"));

        // TODO 6. 将输入写入 Upsert-Kafka 表
        tableEnv.executeSql("insert into dwd_user_register " +
                "select  " +
                "user_id, " +
                "date_format(create_time, 'yyyy-MM-dd') date_id, " +
                "create_time, " +
                "ts " +
                "from user_info").print();

        env.execute("DwdUserRegister");
    }
}

