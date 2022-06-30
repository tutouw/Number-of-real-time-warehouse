package com.iflytek.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.iflytek.common.GmallConfig;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;

/**
 * @author Aaron
 * @date 2022/6/29 23:42
 */

public class DimUtil {
    public static JSONObject getDimInfo(Connection connection, String tableName, String id) throws Exception {
        // 查询redis，在访问hbase之前，先访问redis，如果有数据就直接返回
        Jedis jedis = JedisUtil.getJedis();
        String redisKey = "DIM:" + tableName + ":" + id;
        String dimInfoStr = jedis.get(redisKey);
        if (dimInfoStr != null) {
            // 重置过期时间
            jedis.expire(redisKey, 24 * 60 * 60);

            // 归还连接
            jedis.close();

            // 返回结果
            return JSON.parseObject(dimInfoStr);
        }

        // 拼接sql
        String querySQL = "select * from " + GmallConfig.HBASE_SCHEMA + "." + tableName + " where id = '" + id + "'";
        // 查询phoenix
        List<JSONObject> list = JDBCUtil.queryList(connection, querySQL, JSONObject.class, false);
        // 将得到的结果拿出来
        JSONObject dimInfo = list.get(0);

        // 将数据写入redis
        jedis.set(redisKey, dimInfo.toJSONString());
        jedis.expire(redisKey, 24 * 60 * 60);
        jedis.close();

        // 返回结果数据
        return list.get(0);
    }

    // 删除数据方法
    public static void delDimInfo(String tableName, String id) {
        Jedis jedis = JedisUtil.getJedis();
        String redisKey = "DIM:" + tableName + ":" + id;

        jedis.del(redisKey);
        jedis.close();
    }


    public static void main(String[] args) throws Exception {
        Connection connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);

        long start = System.currentTimeMillis();
        JSONObject jsonObjects = getDimInfo(connection, "DIM_BASE_CATEGORY1", "18");
        long end = System.currentTimeMillis();
        long start2 = System.currentTimeMillis();
        JSONObject jsonObjects2 = getDimInfo(connection, "DIM_BASE_CATEGORY1", "18");
        long end2 = System.currentTimeMillis();

        System.out.println(end-start);
        System.out.println(end2-start2);
        System.out.println(jsonObjects);
    }
}
