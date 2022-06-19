package com.iflytek.flume.interceptor;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;

/**
 * @author Aaron
 * @date 2022/6/16 16:52
 */

public class JSONUtils {
    /**
     * 判断一个字符串是否为json
     * @param log 传入的字符串
     * @return 是json返回true，不是json返回false
     */
    public static boolean isJson(String log){
        // 判断log是不是json
        try {
            JSON.parse(log);
            return true;
        }catch (JSONException e){
            return false;
        }
    }
}
