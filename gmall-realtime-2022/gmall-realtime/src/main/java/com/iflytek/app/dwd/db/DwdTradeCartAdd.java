package com.iflytek.app.dwd.db;

import com.iflytek.utils.MyKafkaUtil;
import com.iflytek.utils.MysqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * 交易域
 * 加购事务事实表
 *
 * @author Aaron
 * @date 2022/6/25 9:35
 */

public class DwdTradeCartAdd {
    public static void main(String[] args) throws Exception {
        // TODO 1、获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);  // 生产环境设置为kafka主题的分区数

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 设置状态存储时间
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5));

        // env.setStateBackend(new HashMapStateBackend());
        // env.enableCheckpointing(5000L);
        // env.getCheckpointConfig().setCheckpointTimeout(10000L);
        // env.getCheckpointConfig().setCheckpointStorage("hdfs:hadoop101:9000//xxx/xx");

        // TODO 2、使用DDL方式读取kafka gmall_db 主题的数据
        //  该数据是maxwell监控所有mysql数据写入到Kafka的，数据格式：
        //  {"database":"gmall",
        //   "xid":4058,
        //   "data":{"tm_name":"欧莱雅","logo_url":"/static/default.jpg","id":2},
        //   "commit":true,
        //   "type":"insert",
        //   "table":"base_trademark",
        //   "ts":1592118957
        //  }
        tableEnv.executeSql(MyKafkaUtil.getTopicDbDDL("dwd_trade_cart_add"));

        // 打印测试
        // Table table = tableEnv.sqlQuery("select * from gmall_db");
        // DataStream<Row> rowDataStream = tableEnv.toAppendStream(table, Row.class);
        // rowDataStream.print(">>>>>>>>>>>>>");

        // TODO 3、过滤加购数据
        //  gmall_db表中数据
        //  "database":"gmall",
        //  "table":"cart_info",
        //  "type":"update",
        //  "data":{"is_ordered"=0,
        //          "cart_price"=488.0,
        //          "create_time"="2020-06-14 19:11:03",
        //          "sku_num"=1,
        //          "sku_id"=33,
        //          "source_type"=2402,
        //          "order_time"=null",
        //          "user_id"=5,
        //          "img_url"="http://47.93.148.192:8080/group1/M00/00/02/rBHu8l-00MSABFB2AAHD3bWoRw015.jpg",
        //          "is_checked"=null",
        //          "sku_name"="香奈儿（Chanel）女士香水5号香水 粉邂逅柔情淡香水EDT 粉邂逅淡香水35ml",
        //          "id=35588,
        //          "source_id"=27,
        //          "operate_time"=null
        //         },
        //  "old":{"img_url"="http://47.93.148.192:8080/group1/M00/00/02/rBHu8l-00MSABFB2AAHD3bWoRhw015.jpg"},
        //  "pt":"2022-06-25T03:05:27.464Z"
        Table cartAddTable = tableEnv.sqlQuery("" +
                "select `data`['id'] id, " +
                "       `data`['user_id'] user_id, " +
                "       `data`['sku_id'] sku_id, " +
                "       `data`['cart_price'] cart_price, " +
                "       if(`type`='insert',cast(`data`['sku_num'] as int),cast(`data`['sku_num'] as int)-cast(`old`['sku_num'] as int)) sku_num, " +
                "       `data`['sku_name'] sku_name, " +
                "       `data`['is_checked'] is_checked, " +
                "       `data`['create_time'] create_time, " +
                "       `data`['operate_time'] operate_time, " +
                "       `data`['is_ordered'] is_ordered, " +
                "       `data`['order_time'] order_time, " +
                "       `data`['source_type'] source_type, " +
                "       `data`['source_id'] source_id, " +
                "       pt " +
                "from gmall_db " +
                "where `database`='gmall' " +
                "      and `table`='cart_info' " +
                "      and ((`type`='update' " +
                "                and `old`['sku_num'] is not null " +
                "                and cast(`data`['sku_num'] as int)>cast(`old`['sku_num'] as int) " +
                "            ) " +
                "            or `type`='insert')");

        tableEnv.createTemporaryView("cart_add", cartAddTable);

        // 打印测试
        // DataStream<Row> rowDataStream = tableEnv.toAppendStream(cartAddTable, Row.class);
        // rowDataStream.print(">>>>>>>>>>>>>");

        // TODO 4、读取mysql中的base_dic表构建维表
        tableEnv.executeSql(MysqlUtil.getBaseDicLookUpDDL());

        // 打印测试
        // Table table = tableEnv.sqlQuery("select * from base_dic");
        // DataStream<Row> rowDataStream = tableEnv.toAppendStream(table, Row.class);
        // rowDataStream.print(">>>>>>>>>>>>>");
        // TODO 5、关联两张表 维度退化
        Table resultTable = tableEnv.sqlQuery(
                "select " +
                        "    c.id, " +
                        "    c.user_id, " +
                        "    c.sku_id, " +
                        "    c.cart_price, " +
                        "    c.sku_num, " +
                        "    c.sku_name, " +
                        "    c.is_checked, " +
                        "    c.create_time, " +
                        "    c.operate_time, " +
                        "    c.is_ordered, " +
                        "    c.order_time, " +
                        "    c.source_type, " +
                        "    c.source_id, " +
                        "    b.dic_name " +
                        "from cart_add c " +
                        "join base_dic for system_time as of c.pt as b " +
                        "on c.source_type=b.dic_code");

        tableEnv.createTemporaryView("result_table", resultTable);
        // 打印测试
        // Table table = tableEnv.sqlQuery("select * from result_table");
        // DataStream<Row> rowDataStream = tableEnv.toAppendStream(table, Row.class);
        // rowDataStream.print(">>>>>>>>>>>>>");

        // TODO 6、将数据写入到Kafka DWD层
        String sinkTopic = "dwd_trade_cart_add";
        tableEnv.executeSql("" +
                "create table trade_cart_add( " +
                "    id string, " +
                "    user_id string, " +
                "    sku_id string, " +
                "    cart_price string, " +
                "    sku_num int, " +
                "    sku_name string, " +
                "    is_checked string, " +
                "    create_time string, " +
                "    operate_time string, " +
                "    is_ordered string, " +
                "    order_time string, " +
                "    source_type string, " +
                "    source_id string, " +
                "    dic_name string " +
                ") " + MyKafkaUtil.getKafkaDDL(sinkTopic, ""));
        tableEnv.executeSql("insert into trade_cart_add select * from result_table")
                .print();

        // TODO 7、启动任务
        env.execute("DwdTradeCartAdd");
    }
}
