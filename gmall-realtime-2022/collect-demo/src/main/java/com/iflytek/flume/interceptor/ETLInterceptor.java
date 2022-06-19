package com.iflytek.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;

/**
 * 1、实现一个接口
 * 2、实现四个方法
 * 3、静态内部类builder实现接口builder
 *
 * @author Aaron
 * @date 2022/6/16 16:36
 */


public class ETLInterceptor implements Interceptor {

    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        // 需求：过滤event中的数据是不是json格式
        byte[] body = event.getBody();
        String log = new String(body, StandardCharsets.UTF_8);

        boolean flag = JSONUtils.isJson(log);

        return flag ? event : null;
    }

    @Override
    public List<Event> intercept(List<Event> events) {
        // 将处理过后为null的event删除掉
        // 使用迭代器
        // 使用lambda表达式替代
        Iterator<Event> iterator = events.iterator();
        while (iterator.hasNext()) {
            Event next = iterator.next();
            if (intercept(next) == null) {
                iterator.remove();
            }
        }

        // 处理完返回
        return events;
    }

    @Override
    public void close() {

    }

    // 3、静态内部类builder实现接口builder
    public static class Builder implements Interceptor.Builder {

        @Override
        public Interceptor build() {
            return new ETLInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
