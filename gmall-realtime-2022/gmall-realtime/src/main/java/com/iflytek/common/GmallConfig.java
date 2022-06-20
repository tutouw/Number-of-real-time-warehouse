package com.iflytek.common;

/**
 * @author Aaron
 * @date 2022/6/19 12:14
 */

public class GmallConfig {

    // Phoenix库名
    public static final String HBASE_SCHEMA = "GMALL_REALTIME";

    // Phoenix驱动
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";

    // Phoenix连接参数
    public static final String PHOENIX_SERVER = "jdbc:phoenix:hadoop101,hadoop102,hadoop103:2181";

    // ClickHouse 驱动
    // public static final String CLICKHOUSE_DRIVER = "ru.yandex.clickhouse.ClickHouseDriver";

    // ClickHouse 连接 URL
    // public static final String CLICKHOUSE_URL = "jdbc:clickhouse://hadoop101:8123/gmall";

}