package com.iflytek.app.func;


import com.alibaba.fastjson.JSONObject;
import com.iflytek.common.GmallConfig;
import com.iflytek.utils.DimUtil;
import com.iflytek.utils.ThreadPoolUtil;
import lombok.SneakyThrows;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author Aaron
 * @date 2022/6/30 12:22
 */

public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T, T> implements DimJoinFunction<T> {
    private Connection connection;
    private ThreadPoolExecutor threadPoolExecutor;
    private String tableName;

    public DimAsyncFunction(String tableName) {
        this.tableName = tableName;

    }

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);

        threadPoolExecutor = ThreadPoolUtil.getThreadPoolExecutor();
    }

    @Override
    public void asyncInvoke(T input, org.apache.flink.streaming.api.functions.async.ResultFuture<T> resultFuture) throws Exception {
        // 使用线程池进行查询异步IO操作
        threadPoolExecutor.execute(new Runnable() {
            @SneakyThrows
            @Override
            public void run() {
                // 获取input中需要关联的字段
                // 1、查询维表数据
                JSONObject dimInfo = DimUtil.getDimInfo(connection, tableName, getKey(input));
                // 2、将维表数据补充到JavaBean中
                join(input,dimInfo);

                // 3、将补充之后的数据输出
                resultFuture.complete(Collections.singletonList(input));
            }
        });
    }

    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) throws Exception {
        // 方便我们查看
        System.out.println("TimeOut: " + input);
    }
}
