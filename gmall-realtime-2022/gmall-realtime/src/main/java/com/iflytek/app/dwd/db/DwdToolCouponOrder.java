package com.iflytek.app.dwd.db;

import com.alibaba.fastjson.JSON;
import com.iflytek.bean.CouponUseOrderBean;
import com.iflytek.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Map;
import java.util.Set;

/**
 * 工具域
 * 优惠券使用（下单）事务事实表
 *
 * @author Aaron
 * @date 2022/6/26 14:30
 */
public class DwdToolCouponOrder {
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
        tableEnv.executeSql("create table `gmall_db` ( " +
                "`database` string, " +
                "`table` string, " +
                "`data` map<string, string>, " +
                "`type` string, " +
                "`old` string, " +
                "`ts` string " +
                ")" + MyKafkaUtil.getKafkaDDL("gmall_db", "dwd_tool_coupon_order"));

        // TODO 4. 读取优惠券领用表数据，封装为流
        Table couponUseOrder = tableEnv.sqlQuery("select " +
                "data['id'] id, " +
                "data['coupon_id'] coupon_id, " +
                "data['user_id'] user_id, " +
                "data['order_id'] order_id, " +
                "date_format(data['using_time'],'yyyy-MM-dd') date_id, " +
                "data['using_time'] using_time, " +
                "`old`, " +
                "ts " +
                "from gmall_db " +
                "where `table` = 'coupon_use' " +
                "and `type` = 'update' ");
        DataStream<CouponUseOrderBean> couponUseOrderDS = tableEnv.toAppendStream(couponUseOrder, CouponUseOrderBean.class);

        // TODO 5. 过滤满足条件的优惠券下单数据，封装为表
        SingleOutputStreamOperator<CouponUseOrderBean> filteredDS = couponUseOrderDS.filter(
                couponUseOrderBean -> {
                    String old = couponUseOrderBean.getOld();
                    if (old != null) {
                        Map oldMap = JSON.parseObject(old, Map.class);
                        Set changeKeys = oldMap.keySet();
                        return changeKeys.contains("using_time");
                    }
                    return true;
                }
        );
        Table resultTable = tableEnv.fromDataStream(filteredDS);
        tableEnv.createTemporaryView("result_table", resultTable);

        // TODO 6. 建立 Upsert-Kafka dwd_tool_coupon_order 表
        tableEnv.executeSql("create table dwd_tool_coupon_order( " +
                "id string, " +
                "coupon_id string, " +
                "user_id string, " +
                "order_id string, " +
                "date_id string, " +
                "order_time string, " +
                "ts string, " +
                "primary key(id) not enforced " +
                ")" + MyKafkaUtil.getUpsertKafkaDDL("dwd_tool_coupon_order"));

        // TODO 7. 将数据写入 Upsert-Kafka 表
        tableEnv.executeSql("" +
                "insert into dwd_tool_coupon_order select " +
                "id, " +
                "coupon_id, " +
                "user_id, " +
                "order_id, " +
                "date_id, " +
                "using_time order_time, " +
                "ts from result_table").print();

        env.execute("DwdToolCouponOrder");
    }
}

