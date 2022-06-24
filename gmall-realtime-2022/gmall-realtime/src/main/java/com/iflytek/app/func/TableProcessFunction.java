package com.iflytek.app.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.iflytek.bean.TableProcess;
import com.iflytek.common.GmallConfig;
import lombok.SneakyThrows;
import org.apache.flink.api.common.functions.BroadcastVariableInitializer;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.*;
import java.util.*;
import java.util.function.Predicate;

/**
 * @author Aaron
 * @date 2022/6/19 11:31
 */

public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {
    private MapStateDescriptor<String, TableProcess> stateDescriptor;
    private HashMap<String, TableProcess> initTable = new HashMap<>();
    private Connection connection;

    public TableProcessFunction(MapStateDescriptor<String, TableProcess> stateDescriptor) throws Exception {
        this.stateDescriptor = stateDescriptor;
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        // Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);


        // 查询mysql中的数据
        // Class.forName("com.mysql.jdbc.Driver");
        Connection mysqlConnection = DriverManager.getConnection("jdbc:mysql://hadoop101:3306/gmall-config?useSSL=false", "root", "123456");
        PreparedStatement preparedStatement = mysqlConnection.prepareStatement("select * from table_process");
        preparedStatement.execute();
        ResultSet resultSet = preparedStatement.getResultSet();
        // 将resultSet中的数据存入到MapState中
        while (resultSet.next()) {
            String source_table = resultSet.getString("source_table");
            String sink_table = resultSet.getString("sink_table");
            String sink_pk = resultSet.getString("sink_pk");
            String sink_columns = resultSet.getString("sink_columns");
            String sink_extend = resultSet.getString("sink_extend");
            initTable.put(source_table, new TableProcess(source_table, sink_table, sink_columns, sink_pk, sink_extend));
        }

        // 遍历initTable中的数据
        /*for (Map.Entry<String, TableProcess> entry : initTable.entrySet()) {

            // 获取表名
            String tableName = entry.getKey();
            mapState.put(tableName, new TableProcess());
        }*/
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

        // value 中数据的格式(数据来自Kafka，由Kafka的上游maxwell封装)
        //{"database":"gmall",
        // "xid":4058,
        // "data":{"tm_name":"欧莱雅","logo_url":"/static/default.jpg","id":2},
        // "commit":true,
        // "type":"insert",
        // "table":"base_trademark",
        // "ts":1592118957}
        // TODO 1、获取并解析数据为JavaBean对象
        JSONObject jsonObject = JSON.parseObject(value);
        TableProcess tableProcess = JSON.parseObject(jsonObject.getString("data"), TableProcess.class);
        // System.out.println("==================="+tableProcess.getSinkPk());

        // TODO 2、校验表是否存在，如果不存在则建表
        // (表名,字段名,主键,建表扩展字段)
        checkTable(tableProcess.getSinkTable(),
                tableProcess.getSinkColumns(),
                tableProcess.getSinkPk(),
                tableProcess.getSinkExtend());
        // TODO 3、将数据写入状态，广播出去
        String key = tableProcess.getSourceTable();
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(stateDescriptor);

        for (Map.Entry<String, TableProcess> entry : initTable.entrySet()) {

            // 获取表名
            String tableName = entry.getKey();
            TableProcess entryValue = entry.getValue();
            broadcastState.put(tableName, entryValue);
        }
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
            //处理字段
            if (sinkPk == null || sinkPk.equals("")) {
                sinkPk = "id";
            }
            if (sinkExtend == null) {
                sinkExtend = "";
            }

            StringBuilder sql = new StringBuilder("create table if not exists ")
                    .append(GmallConfig.HBASE_SCHEMA)
                    .append(".")
                    .append(sinkTable)
                    .append("(");

            String[] columns = sinkColumns.split(",");
            for (int i = 0; i < columns.length; i++) {

                //获取字段
                String column = columns[i];

                //判断是否为主键字段
                if (sinkPk.equals(column)) {
                    sql.append(column).append(" varchar primary key");
                } else {
                    sql.append(column).append(" varchar");
                }

                //不是最后一个字段,则添加","
                if (i < columns.length - 1) {
                    sql.append(",");
                }
            }

            sql.append(")").append(sinkExtend);

            // System.out.println("+++++++++++++++++++");
            System.out.println(sql);
            // System.out.println("+++++++++++++++++++");

            //预编译SQL
            preparedStatement = connection.prepareStatement(sql.toString());

            //执行
            preparedStatement.execute();
        } catch (Exception e) {
            throw new RuntimeException("建表" + sinkTable + "失败！");
        } finally {
            //资源释放
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
     * 主数据流
     * 1、获取广播的配置数据（按照表名过滤）
     * 2、根据配置信息中sinkColumns字段过滤
     * 3、补充SinkTable字段输出
     *
     * @param value The stream element.
     * @param ctx   A {@link ReadOnlyContext} that allows querying the timestamp of the element,
     *              querying the current processing/event time and updating the broadcast state. The context
     *              is only valid during the invocation of this method, do not store it.
     * @param out   The collector to emit resulting elements to
     * @throws Exception
     */
    @Override
    public void processElement(JSONObject value, BroadcastProcessFunction<JSONObject, String, JSONObject>.ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {


        // 广播变量中的数据示例
        // "表名":"TableProcess"

        // value 中数据的格式(数据来自Kafka，由Kafka的上游maxwell封装)
        //{"database":"gmall",
        // "xid":4058,
        // "data":{"tm_name":"欧莱雅","logo_url":"/static/default.jpg","id":2},
        // "commit":true,
        // "type":"insert",
        // "table":"base_trademark",
        // "ts":1592118957}
        // TODO 1、获取广播的配置数据（按照表名过滤）
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(stateDescriptor);

        // 取出这个消息对应的表 所对应的 广播流 中的 value：tableProcess
        TableProcess tableProcess = broadcastState.get(value.getString("table"));
        // 取出这个消息对应的消息类型
        String type = value.getString("type");
        // 由于主流中的数据为全部的业务数据，但是广播流中的数据只有维表数据，所以可能 tableProcess 可能为null！！！
        // 如果为null，将其过滤掉，舍弃，达到过滤目的
        // 另外删除数据的操作不需要，因为在mysql中删除的数据在以后在维度建模的时候肯定关联不到这个维度，所以不需要收集删除记录
        if (tableProcess != null && ("bootstrap-insert".equals(type) || "insert".equals(type) || "update".equals(type))) {


            // TODO 2、根据配置信息中 sinkColumns 字段过滤
            // 过滤掉不需要的字段，需要的字段在 sinkColumns 中,所以 filter 字段需要两个参数
            // 1、原始数据，value.data
            // 2、需要的字段，tableProcess.sinkColumns
            filter(value.getJSONObject("data"), tableProcess.getSinkColumns());


            // TODO 3、补充 SinkTable 字段输出
            // 要将数据写到phoenix中，要根据这个字段找到要写的phoenix表
            value.put("sinkTable", tableProcess.getSinkTable());
            out.collect(value);

        } else {
            // System.out.println(tableProcess);
            // 不是我们需要的数据，过滤掉
            System.out.println("过滤掉：" + value);
        }
    }

    /**
     * 过滤数据
     *
     * @param data        {"id":1,"tm_name","logo_url":"..."}
     * @param sinkColumns id,tm_name
     *                    数据过滤处理后       {"id":1,"tm_name"}
     */
    private void filter(JSONObject data, String sinkColumns) {

        String[] split = sinkColumns.split(",");
        List<String> columnsList = Arrays.asList(split);

        Iterator<Map.Entry<String, Object>> iterator = data.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Object> next = iterator.next();
            if (!columnsList.contains(next.getKey())) {
                iterator.remove();
            }
        }

        // data.entrySet().removeIf(next -> !columnsList.contains(next.getKey()));
    }
}
