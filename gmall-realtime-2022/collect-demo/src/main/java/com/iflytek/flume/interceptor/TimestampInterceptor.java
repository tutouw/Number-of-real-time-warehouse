package com.iflytek.flume.interceptor;

import com.alibaba.fastjson.JSONObject;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * 1、实现一个接口
 * 2、实现四个方法
 * 3、静态内部类builder实现接口builder
 *
 * @author Aaron
 * @date 2022/6/16 20:46
 */

public class TimestampInterceptor implements Interceptor {

    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        // 修改headers中的时间戳值，改为日志数据的时间（事件事件）
        // 提供给hdfs sink使用，控制输出文件的文件夹名称
        byte[] body = event.getBody();

        String log = new String(body, StandardCharsets.UTF_8);

        JSONObject jsonObject = JSONObject.parseObject(log);

        String timestamp = jsonObject.getString("ts");

        // 获取headers
        Map<String, String> headers = event.getHeaders();
        headers.put("timestamp", timestamp);

        return event;
    }

    @Override
    public List<Event> intercept(List<Event> events) {
        for (Event event : events) {
            intercept(event);
        }
        return events;
    }

    @Override
    public void close() {

    }

    // 3、静态内部类builder实现接口builder
    public static class Builder implements Interceptor.Builder {

        @Override
        public Interceptor build() {
            return new TimestampInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
