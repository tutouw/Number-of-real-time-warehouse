package com.iflytek.flume.interceptor.db;

import com.alibaba.fastjson.JSONObject;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * @author Aaron
 * @date 2022/6/18 13:45
 */

public class TimeStampInterceptor implements Interceptor {

    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {

        // 需求：将event中的数据 里面的时间戳拿出来
        // 放入到headers中，提供给hdfsSink使用，控制输出到文件路径
        Map<String, String> headers = event.getHeaders();
        String log = new String(event.getBody(), StandardCharsets.UTF_8);

        JSONObject jsonObject = JSONObject.parseObject(log);

        Long ts = jsonObject.getLong("ts");

        //Maxwell输出的数据中的ts字段时间戳单位为秒，Flume HDFSSink要求单位为毫秒
        String timeMills = String.valueOf(ts * 1000);

        headers.put("timestamp", timeMills);

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

    public static class Builder implements Interceptor.Builder {

        @Override
        public Interceptor build() {
            return new TimeStampInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
