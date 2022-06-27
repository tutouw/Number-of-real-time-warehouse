package com.iflytek.app.dws;

import com.iflytek.app.func.SplitFunction;
import com.iflytek.bean.KeywordBean;
import com.iflytek.utils.MyClickHouseUtil;
import com.iflytek.utils.MyKafkaUtil;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 流量域
 * 来源关键词粒度页面浏览各窗口汇总表
 *
 * @author Aaron
 * @date 2022/6/26 22:45
 */

public class DwsTrafficSourceKeywordPageViewWindow {
    public static void main(String[] args) throws Exception {
        // TODO 1、获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // env.setStateBackend(new HashMapStateBackend());
        // env.enableCheckpointing(5000L);
        // env.getCheckpointConfig().setCheckpointTimeout(10000L);
        // env.getCheckpointConfig().setCheckpointStorage("hdfs:hadoop101:9000//xxx/xx");

        // TODO 2、使用DDL方式读取DWD层页面浏览日志，创建 page_log 表，同时获取事件事件，生成watermark
        tableEnv.executeSql("" +
                "create table page_log( " +
                "    `page` map<string,string>, " +
                "    `ts` bigint, " +
                "    `rt` as TO_TIMESTAMP(FROM_UNIXTIME(ts/1000)), " +
                "    WATERMARK for rt as rt - INTERVAL '2' second " +
                ")" + MyKafkaUtil.getKafkaDDL("dwd_traffic_page_log", "dws_traffic_source_keyword_page_view_window"));

        // TODO 3、过滤出搜索数据 `page`['last_page_id']='search' and `page`['item_type']='keyword'
        Table searchTable = tableEnv.sqlQuery("" +
                "select " +
                "  `page`['item'] key_word, " +
                "   rt " +
                "from page_log " +
                "where `page`['last_page_id']='search' " +
                "  and `page`['item_type']='keyword' " +
                "  and `page`['item'] is not null");

        tableEnv.createTemporaryView("search_table", searchTable);

        // TODO 4、使用自定义函数做分词处理
        //  注册UDTF
        tableEnv.createTemporaryFunction("SplitFunction", SplitFunction.class);
        Table keywordTable = tableEnv.sqlQuery("select word,rt from search_table,LATERAL TABLE(SplitFunction(key_word))");
        tableEnv.createTemporaryView("keyword_table", keywordTable);

        // TODO 5、分组开窗聚合
        Table resultTable = tableEnv.sqlQuery("" +
                "select " +
                "   'search' source, " +
                "   word keyword, " +
                "   count(*) keyword_count, " +
                "   date_format(tumble_start(rt, interval '10' seconds),'yyyy-MM-dd HH:mm:ss') stt, " +
                "   date_format(tumble_end(rt, interval '10' seconds),'yyyy-MM-dd HH:mm:ss') edt, " +
                "   unix_timestamp() ts " +
                "from keyword_table " +
                "group by word," +
                "   tumble(rt, interval '10' seconds)");
        tableEnv.createTemporaryView("result_Table", resultTable);
        // TODO 6、将数据转换为流
        DataStream<KeywordBean> keywordBeanDataStream = tableEnv.toAppendStream(resultTable, KeywordBean.class);
        keywordBeanDataStream.print(">>>>>>>>>");

        // TODO 7、将数据输出到clickHouse
        keywordBeanDataStream.addSink(MyClickHouseUtil.getClickHouseSink("" +
                "insert into " +
                "   dws_traffic_source_keyword_page_view_window(stt,edt,source,keyword,keyword_count,ts) " +
                "values(?,?,?,?,?,?)"));

        // TODO 8、执行
        env.execute("DwsTrafficSourceKeywordPageViewWindow");

    }
}
