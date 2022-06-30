package com.iflytek.utils;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.CaseFormat;
import com.iflytek.common.GmallConfig;
import org.apache.commons.beanutils.BeanUtils;

import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Aaron
 * @date 2022/6/29 21:08
 */

public class JDBCUtil {
    // 所有jdbc服务的所有查询
    public static <T> List<T> queryList(Connection connection, String querySql, Class<T> clz, boolean underScoreToCamel) throws Exception {
        // 创建用于存放结果的List集合
        ArrayList<T> list = new ArrayList<>();

        // 预编译sql
        PreparedStatement preparedStatement = connection.prepareStatement(querySql);

        // 执行查询操作
        ResultSet resultSet = preparedStatement.executeQuery();
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();

        // 遍历查询到的结果集，将每行数据封装为T对象，并将其放入集合
        while (resultSet.next()) {
            // 创建T对象
            T t = clz.newInstance();

            for (int i = 0; i < columnCount; i++) {

                String columnName = metaData.getColumnName(i + 1);
                Object value = resultSet.getObject(columnName);

                // 判断是否需要转换列明信息
                if (underScoreToCamel) {
                    columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName.toLowerCase());
                }

                // 给T对象赋值
                BeanUtils.setProperty(t,columnName,value);
            }
            // 将T对象添加到集合
            list.add(t);
        }

        // 释放资源
        resultSet.close();
        preparedStatement.close();

        // 返回结果数据
        return list;
    }

    public static void main(String[] args) throws Exception {
        Connection connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);

        List<JSONObject> jsonObjects = queryList(connection, "select count(*) ct from GMALL_REALTIME.DIM_BASE_TRADEMARK", JSONObject.class, false);
        System.out.println(jsonObjects);
    }
}
