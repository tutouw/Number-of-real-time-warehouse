package com.iflytek.app.func;

import com.alibaba.fastjson.JSONObject;
import com.iflytek.common.GmallConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

/**
 * @author Aaron
 * @date 2022/6/22 11:03
 */

public class DimSinkFunction extends RichSinkFunction<JSONObject> {

    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {

        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    // value中的数据：相比与数据流中的数据多了一条sinkTable字段
    //{"sinkTable":"dim_xxx",
    // "database":"gmall",
    // "xid":4058,
    // "data":{"tm_name":"欧莱雅","logo_url":"/static/default.jpg","id":2},
    // "commit":true,
    // "type":"insert",
    // "table":"base_trademark",
    // "ts":1592118957}
    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        PreparedStatement preparedStatement = null;
        try {
            // 拼接SQL upsert into db.tn(id,tm_name) values ('12','test')
            String upsertSql = genUserSql(value.getString("sinkTable"), value.getJSONObject("data"));

            System.out.println(upsertSql);
            // 预编译SQL
            preparedStatement = connection.prepareStatement(upsertSql);

            // 执行写入操作
            boolean execute = preparedStatement.execute();
            connection.commit();
            System.out.println("插入操作执行情况："+execute);
        } catch (SQLException e) {
            System.out.println("插入数据失败！");
        } finally {
            if (preparedStatement != null) {
                preparedStatement.close();
            }
        }

        // 释放资源
    }

    /**
     * @param sinkTable dim_xxx
     * @param data      "tm_name":"欧莱雅","logo_url":"/static/default.jpg","id":2
     * @return          upsert into db.tn(id,tm_name) values ('12','test')
     */
    private String genUserSql(String sinkTable, JSONObject data) {
        Set<String> columns = data.keySet();
        Collection<Object> values = data.values();

        return "upsert into " + GmallConfig.HBASE_SCHEMA + "." + sinkTable + "("
                + StringUtils.join(columns,",")+") values ('"+StringUtils.join(values,"','")+"')";
    }
}
