package com.iflytek.utils;

/**
 * @author Aaron
 * @date 2022/6/25 12:23
 */


public class MysqlUtil {
    /**
     * 获取base_dic字典表的创建Lookup表的完整SQL
     * @return 返回完整的创建base_dic的Lookup表的sql语句
     */
    public static String getBaseDicLookUpDDL() {
        return "create table `base_dic` ( " +
                "    `dic_code` string, " +
                "    `dic_name` string ," +
                "    `parent_code` string, " +
                "    `create_time` timestamp, " +
                "    `operate_time` timestamp, " +
                "    primary key(`dic_code`) not enforced " +
                ") "+mysqlLookUpTableDDL("base_dic");
    }

    /**
     * 获取创建Lookup表的参数字符串
     * @param tableName 要创建Lookup表名
     * @return 返回完整的创建Lookup表的参数字符串
     */
    public static String mysqlLookUpTableDDL(String tableName) {
        return "with ( " +
                "    'connector'='jdbc', " +
                "    'url'='jdbc:mysql://hadoop101:3306/gmall?useSSL=false', " +
                "    'username'='root', " +
                "    'password'='123456', " +
                "    'lookup.cache.max-rows'='10', " +
                "    'lookup.cache.ttl'='1 hour', " +
                "    'driver'='com.mysql.cj.jdbc.Driver', " +
                "    'table-name'='" + tableName +"'"+
                ")";
    }
}
