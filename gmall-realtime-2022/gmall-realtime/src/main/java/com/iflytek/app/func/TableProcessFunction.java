package com.iflytek.app.func;

import com.alibaba.fastjson.JSONObject;
import com.iflytek.bean.TableProcess;
import com.iflytek.common.GmallConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author Aaron
 * @date 2022/6/19 11:31
 */

public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;
    private Connection connection;

    public TableProcessFunction(MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }

    // 在open中获取与phoenix的连接
    @Override
    public void open(Configuration parameters) throws SQLException {
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);

    }

    /**
     * 广播流
     * 1、获取并解析数据为JavaBean对象
     * 2、校验表是否存在，如果不存在则建表
     * 3、将数据写入状态，广播出去
     *
     * @param value The stream element.
     * @param ctx   A {@link Context} that allows querying the timestamp of the element, querying the
     *              current processing/event time and updating the broadcast state. The context is only valid
     *              during the invocation of this method, do not store it.
     * @param out   The collector to emit resulting elements to
     * @throws Exception
     */
    @Override
    public void processBroadcastElement(String value, BroadcastProcessFunction<JSONObject, String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {

        // value数据示例（value的数据来自FlinkCDC）:
        // { "before":null,
        //   "after":{"id":1,"tm_name":"三星","logo_url":"/static/default.jpg"},
        //   "source":{"version":"1.5.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1649744439676,
        //             "snapshot":"false","db":"gamll","sequence":null,"table":"base_trademark","server_id":0,"gtid":null
        //             "file":"","pos":0,"row":0,"thread":null,"query":null},
        //   "op":"r",
        //   "ts_ms":1649744439678,
        //   "transaction":null
        // }

        // TODO 1、获取并解析数据为JavaBean对象
        JSONObject jsonObject = JSONObject.parseObject(value);
        TableProcess tableProcess = JSONObject.parseObject(jsonObject.getString("after"), TableProcess.class);

        // TODO 2、校验表是否存在，如果不存在则建表
        // (表名,字段名,主键,建表扩展字段)
        checkTable(tableProcess.getSinkTable(), tableProcess.getSinkColumns(), tableProcess.getSinkPk(), tableProcess.getSinkExtend());

        // TODO 3、将数据写入状态，广播出去
        String key = tableProcess.getSourceTable();
        // 获取广播状态
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        broadcastState.put(key, tableProcess);
    }

    /**
     * 在phoenix中校验并建表
     * create table if not exists db.tn(id varchar primary key, name varchar,...) xxx
     *
     * @param sinkTable   表名
     * @param sinkColumns 字段名
     * @param sinkPk      主键
     * @param sinkExtend  建表扩展字段
     */
    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {
        PreparedStatement preparedStatement = null;
        try {
            // 处理字段
            if (sinkPk == null || sinkPk.equals("")) {
                sinkPk = "id";
            }
            if (sinkExtend == null) {
                sinkExtend = "";
            }

            // 拼接SQL !!!!注意空格!!!!
            // create table if not exists db.tn(
            StringBuilder sql = new StringBuilder("create table if not exists ")
                    .append(GmallConfig.HBASE_SCHEMA)
                    .append(".")
                    .append(sinkTable)
                    .append("(");

            // (id varchar primary key, name varchar,...)
            String[] columns = sinkColumns.split(",");
            for (int i = 0; i < columns.length; i++) {
                // 获取字段
                String column = columns[i];
                // 判断是否为主键字段,主键字段要加primary key
                if (column.equals(sinkPk)) {
                    sql.append(column).append(" varchar primary key");
                } else {
                    sql.append(column).append(" varchar");
                }
                // 判断是否不是最后一个字段，不是最后一个字段要拼接逗号
                if (i < columns.length - 1) {
                    // 不是最后一个字段，拼接逗号
                    sql.append(",");
                } else {
                    sql.append(") ");
                }
            }

            // 检验sql
            System.out.println(sql);

            // 预编译sql

            preparedStatement = connection.prepareStatement(sql.toString());

            // 执行
            preparedStatement.execute();

            // 资源释放
            preparedStatement.close();
        } catch (SQLException e) {
            // 如果建表失败，那么后续在执行也没什么意义，这时候就要关闭程序，报异常
            throw new RuntimeException("建表 " + sinkTable + " 失败！");
        } finally {
            // 资源释放
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 主流
     * 1、获取广播的配置数据（按照表名过滤）
     * 2、根据配置信息中sinkColumns字段过滤
     * 3、补充SinkTable字段输出
     *
     * @param value The stream element.
     * @param ctx A {@link ReadOnlyContext} that allows querying the timestamp of the element,
     *     querying the current processing/event time and updating the broadcast state. The context
     *     is only valid during the invocation of this method, do not store it.
     * @param out The collector to emit resulting elements to
     * @throws Exception
     */
    @Override
    public void processElement(JSONObject value, BroadcastProcessFunction<JSONObject, String, JSONObject>.ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {

        // TODO 1、获取广播的配置数据（按照表名过滤）

        // TODO 2、根据配置信息中sinkColumns字段过滤

        // TODO 3、补充SinkTable字段输出
    }
}
