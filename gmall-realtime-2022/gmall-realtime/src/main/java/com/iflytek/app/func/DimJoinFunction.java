package com.iflytek.app.func;

import com.alibaba.fastjson.JSONObject;

/**
 * @author Aaron
 * @date 2022/6/30 15:28
 */

public interface DimJoinFunction<T> {
    String getKey(T input);

    void join(T input, JSONObject dimInfo);
}
