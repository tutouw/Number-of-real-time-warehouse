package com.iflytek.app.dws;

import com.alibaba.fastjson.JSONObject;
import com.iflytek.app.func.DimAsyncFunction;
import com.iflytek.app.func.OrderDetailFilterFunction;
import com.iflytek.bean.TradeTrademarkCategoryUserSpuOrderBean;
import com.iflytek.utils.DateFormatUtil;
import com.iflytek.utils.MyClickHouseUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

/**
 * @author Aaron
 * @date 2022/6/29 18:34
 */

public class DwsTradeTrademarkCategoryUserSpuOrderWindow {
    public static void main(String[] args) throws Exception {

        // TODO 1、获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // env.setStateBackend(new HashMapStateBackend());
        // env.enableCheckpointing(5000L);
        // env.getCheckpointConfig().setCheckpointTimeout(10000L);
        // env.getCheckpointConfig().setCheckpointStorage("hdfs:hadoop101:9000//xxx/xx");

        // TODO 2、获取过滤后的OrderDetail表
        String groupId = "dws_trade_trademark_category_user_spu_order_window";
        SingleOutputStreamOperator<JSONObject> orderDetailJsonObjDS = OrderDetailFilterFunction.getDwdOrderDetail(env, groupId);

        // TODO 3、转换数据为JavaBean
        SingleOutputStreamOperator<TradeTrademarkCategoryUserSpuOrderBean> skuUserOrderDS = orderDetailJsonObjDS.map(data ->
                TradeTrademarkCategoryUserSpuOrderBean.builder()
                        .skuId(data.getString("sku_id"))
                        .userId(data.getString("user_id"))
                        .orderCount(1L)
                        .orderAmount(data.getDouble("split_total_amount"))
                        .ts(DateFormatUtil.toTs(data.getString("order_create_time")))
                        .build()
        );

        // TODO 4、关联维表
        //  将维表数据从phoenix取出，然后将其放入TradeTrademarkCategoryUserSpuOrderBean中
/*        skuUserOrderDS.map(new RichMapFunction<TradeTrademarkCategoryUserSpuOrderBean, TradeTrademarkCategoryUserSpuOrderBean>() {

            @Override
            public void open(Configuration parameters) throws Exception {
                // 创建phoenix连接

            }

            @Override
            public TradeTrademarkCategoryUserSpuOrderBean map(TradeTrademarkCategoryUserSpuOrderBean value) throws Exception {
                // 查询sku表
                // DimUtil.getDimInfo(conn,"", "");
                // 查询spu表

                // ... ...

                return value;
            }
        });*/
        /*AsyncDataStream.unorderedWait(
                skuUserOrderDS,
                new DimAsyncFunction<TradeTrademarkCategoryUserSpuOrderBean>(),
                100,
                TimeUnit.SECONDS);*/

        // 4.1 关联SKU表
        SingleOutputStreamOperator<TradeTrademarkCategoryUserSpuOrderBean> withSkuDs = AsyncDataStream.unorderedWait(
                skuUserOrderDS,
                new DimAsyncFunction<TradeTrademarkCategoryUserSpuOrderBean>("DIM_SKU_INFO") {
                    @Override
                    public String getKey(TradeTrademarkCategoryUserSpuOrderBean input) {
                        return input.getSkuId();
                    }

                    @Override
                    public void join(TradeTrademarkCategoryUserSpuOrderBean input, JSONObject dimInfo) {
                        // 获取
                        if (dimInfo != null) {
                            input.setSpuId(dimInfo.getString("SPU_ID"));
                            input.setTrademarkId(dimInfo.getString("TM_ID"));
                            input.setCategory3Id(dimInfo.getString("CATEGORY3_ID"));
                        }
                    }
                },
                100,
                TimeUnit.SECONDS
        );

        // 4.2 关联SPU表
        SingleOutputStreamOperator<TradeTrademarkCategoryUserSpuOrderBean> withSpuDS = AsyncDataStream.unorderedWait(
                withSkuDs,
                new DimAsyncFunction<TradeTrademarkCategoryUserSpuOrderBean>("DIM_SPU_INFO") {
                    @Override
                    public String getKey(TradeTrademarkCategoryUserSpuOrderBean input) {
                        return input.getSpuId();
                    }

                    @Override
                    public void join(TradeTrademarkCategoryUserSpuOrderBean input, JSONObject dimInfo) {
                        // 获取
                        if (dimInfo != null) {
                            input.setSpuName(dimInfo.getString("SPU_NAME"));
                        }
                    }
                },
                100,
                TimeUnit.SECONDS
        );

        // 4.3 关联Trademark表
        SingleOutputStreamOperator<TradeTrademarkCategoryUserSpuOrderBean> withTrademarkDS = AsyncDataStream.unorderedWait(
                withSpuDS,
                new DimAsyncFunction<TradeTrademarkCategoryUserSpuOrderBean>("DIM_BASE_TRADEMARK") {
                    @Override
                    public String getKey(TradeTrademarkCategoryUserSpuOrderBean input) {
                        return input.getTrademarkId();
                    }

                    @Override
                    public void join(TradeTrademarkCategoryUserSpuOrderBean input, JSONObject dimInfo) {
                        // 获取
                        if (dimInfo != null) {
                            input.setTrademarkName(dimInfo.getString("TM_NAME"));
                        }
                    }
                },
                100,
                TimeUnit.SECONDS
        );


        // 4.4 关联category3表
        SingleOutputStreamOperator<TradeTrademarkCategoryUserSpuOrderBean> withCategory3DS = AsyncDataStream.unorderedWait(
                withTrademarkDS,
                new DimAsyncFunction<TradeTrademarkCategoryUserSpuOrderBean>("DIM_BASE_CATEGORY3") {
                    @Override
                    public String getKey(TradeTrademarkCategoryUserSpuOrderBean input) {
                        return input.getCategory3Id();
                    }

                    @Override
                    public void join(TradeTrademarkCategoryUserSpuOrderBean input, JSONObject dimInfo) {
                        // 获取
                        if (dimInfo != null) {
                            input.setCategory3Name(dimInfo.getString("NAME"));
                            input.setCategory2Id(dimInfo.getString("CATEGORY2_ID"));
                        }
                    }
                },
                100,
                TimeUnit.SECONDS
        );

        // 4.5 关联category2表
        SingleOutputStreamOperator<TradeTrademarkCategoryUserSpuOrderBean> withCategory2DS = AsyncDataStream.unorderedWait(
                withCategory3DS,
                new DimAsyncFunction<TradeTrademarkCategoryUserSpuOrderBean>("DIM_BASE_CATEGORY2") {
                    @Override
                    public String getKey(TradeTrademarkCategoryUserSpuOrderBean input) {
                        return input.getCategory2Id();
                    }

                    @Override
                    public void join(TradeTrademarkCategoryUserSpuOrderBean input, JSONObject dimInfo) {
                        // 获取
                        if (dimInfo != null) {
                            input.setCategory2Name(dimInfo.getString("NAME"));
                            input.setCategory1Id(dimInfo.getString("CATEGORY1_ID"));
                        }
                    }
                },
                100,
                TimeUnit.SECONDS
        );

        // 4.6 关联category1表
        SingleOutputStreamOperator<TradeTrademarkCategoryUserSpuOrderBean> withCategory1DS = AsyncDataStream.unorderedWait(
                withCategory2DS,
                new DimAsyncFunction<TradeTrademarkCategoryUserSpuOrderBean>("DIM_BASE_CATEGORY1") {
                    @Override
                    public String getKey(TradeTrademarkCategoryUserSpuOrderBean input) {
                        return input.getCategory1Id();
                    }

                    @Override
                    public void join(TradeTrademarkCategoryUserSpuOrderBean input, JSONObject dimInfo) {
                        // 获取
                        if (dimInfo != null) {
                            input.setCategory1Name(dimInfo.getString("NAME"));
                        }
                    }
                },
                100,
                TimeUnit.SECONDS
        );

        // 打印测试
        withCategory1DS.print("withCategory1DS>>>>>>");

        // TODO 5、提取时间戳生成watermark
        SingleOutputStreamOperator<TradeTrademarkCategoryUserSpuOrderBean> withWMDS = withCategory1DS.assignTimestampsAndWatermarks(WatermarkStrategy.<TradeTrademarkCategoryUserSpuOrderBean>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<TradeTrademarkCategoryUserSpuOrderBean>() {

                    @Override
                    public long extractTimestamp(TradeTrademarkCategoryUserSpuOrderBean element, long recordTimestamp) {
                        return element.getTs();
                    }
                }));

        // TODO 6、分组开窗聚合
        KeyedStream<TradeTrademarkCategoryUserSpuOrderBean, String> keyedStream = withWMDS.keyBy(new KeySelector<TradeTrademarkCategoryUserSpuOrderBean, String>() {
            @Override
            public String getKey(TradeTrademarkCategoryUserSpuOrderBean value) throws Exception {
                return value.getUserId() + "-" +
                        value.getCategory1Id() + "-" +
                        value.getCategory1Name() + "-" +
                        value.getCategory2Id() + "-" +
                        value.getCategory2Name() + "-" +
                        value.getCategory3Id() + "-" +
                        value.getCategory3Name() + "-" +
                        value.getSpuId() + "-" +
                        value.getSpuName() + "-" +
                        value.getTrademarkId() + "-" +
                        value.getTrademarkName();
            }
        });
        SingleOutputStreamOperator<TradeTrademarkCategoryUserSpuOrderBean> resultDS = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<TradeTrademarkCategoryUserSpuOrderBean>() {
                    @Override
                    public TradeTrademarkCategoryUserSpuOrderBean reduce(TradeTrademarkCategoryUserSpuOrderBean value1, TradeTrademarkCategoryUserSpuOrderBean value2) throws Exception {
                        value1.setOrderCount(value1.getOrderCount() + value2.getOrderCount());
                        value2.setOrderAmount(value1.getOrderAmount() + value2.getOrderAmount());

                        return value1;
                    }
                }, new WindowFunction<TradeTrademarkCategoryUserSpuOrderBean, TradeTrademarkCategoryUserSpuOrderBean, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<TradeTrademarkCategoryUserSpuOrderBean> input, Collector<TradeTrademarkCategoryUserSpuOrderBean> out) throws Exception {
                        // 获取数据
                        TradeTrademarkCategoryUserSpuOrderBean next = input.iterator().next();

                        // 补充信息
                        next.setTs(System.currentTimeMillis());
                        next.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                        next.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));

                        // 输出数据
                        out.collect(next);

                    }
                });

        // TODO 7、将数据写出到ClickHouses
        resultDS.print("resultDS>>>>>>>>");
        resultDS.addSink(MyClickHouseUtil.getClickHouseSink(
                "insert into dws_trade_trademark_category_user_spu_order_window values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"));

        // TODO 8、启动任务
        env.execute("DwsTradeTrademarkCategoryUserSpuOrderWindow");
    }
}
