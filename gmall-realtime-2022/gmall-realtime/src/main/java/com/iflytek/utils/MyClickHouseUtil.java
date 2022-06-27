package com.iflytek.utils;

import com.iflytek.bean.TransientSink;
import com.iflytek.common.GmallConfig;
import lombok.SneakyThrows;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author Aaron
 * @date 2022/6/27 11:52
 */

public class MyClickHouseUtil {
    public static <T> SinkFunction<T> getClickHouseSink(String sql) {
        return JdbcSink.sink(sql, new JdbcStatementBuilder<T>() {
                    @SneakyThrows
                    @Override
                    public void accept(PreparedStatement statement, T t) throws SQLException {
                        // 使用反射的方式提取字段
                        Class<?> clz = t.getClass();
                        Field[] fields = clz.getDeclaredFields();

                        int offset = 0;
                        for (int i = 0; i < fields.length; i++) {
                            Field field = fields[i];
                            field.setAccessible(true);

                            // 判断是否有字段上面@TransientSink注解，如果有这个注解，那么不把这个字段写道sql里面
                            TransientSink fieldAnnotatedType = field.getAnnotation(TransientSink.class);
                            if (fieldAnnotatedType != null) {
                                // 防止错位，属性对齐，另外定义一个字段来保证数据的顺序准确
                                offset++;
                                continue;
                            }

                            // 获取字段给占位符赋值
                            Object value = field.get(t);
                            statement.setObject(i + 1 - offset, value);
                        }

                    }
                }, new JdbcExecutionOptions.Builder().withBatchSize(5).withBatchIntervalMs(1000L).build(),

                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder().withDriverName(GmallConfig.CLICKHOUSE_DRIVER).withUrl(GmallConfig.CLICKHOUSE_URL).build());
    }
}
