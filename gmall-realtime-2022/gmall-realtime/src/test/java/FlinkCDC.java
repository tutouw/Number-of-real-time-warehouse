import com.iflytek.utils.JsonDebeziumDeserializationSchema;
import com.ververica.cdc.connectors.mysql.MySqlSource;

import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.DebeziumSourceFunction;

import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * @author zhouyanjun
 * @create 2021-06-22 17:03
 */
public class FlinkCDC {
    public static void main(String[] args) throws Exception {
//        System.setProperty("HADOOP_USER_NAME", "alibaba" );


        //1.创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 开启Ck，这块我不设置了，先在本地测试运行下


        //2.使用CDC的方式读取MySQL变化数据
/*        DebeziumSourceFunction<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("hadoop101")
                .port(3306)
                // .scanNewlyAddedTableEnabled(true) // eanbel scan the newly added tables fature
                .databaseList("gmall-config") // set captured database
                .tableList("gmall-config.table_process") // set captured tables [product, user, address]
                .username("root")
                .password("123456")
                .deserializer(new StringDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .build();

        // DataStreamSource<String> StreamSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(),"MySqlSource");//得到了一个流
        DataStreamSource<String> StreamSource = env.addSource(mySqlSource);//得到了一个流


        //3.打印
        StreamSource.print();*/

        DebeziumSourceFunction<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("hadoop101")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmall-config")
                .tableList("gmall-config.table_process")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .build();

        // DataStreamSource<String> mysqlSourceDS = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MysqlSource");
        DataStreamSource<String> mysqlSourceDS =  env.addSource(mySqlSource);
        mysqlSourceDS.print();
        //4.启动
        env.execute();
    }
}