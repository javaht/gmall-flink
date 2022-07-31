package com.zht.app.dws;

import com.alibaba.fastjson.JSONObject;
import com.zht.app.func.OrderDetailFilterFunction;
import com.zht.bean.TradeTrademarkCategoryUserSpuOrderBean;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwsTradeTrademarkCategoryUserSpuOrderWindow {
    public static void main(String[] args) {
        // TODO 1. 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //tableEnv.getConfig().setLocalTimeZone(ZoneId.of("GMT+8"));

        // TODO 2. 启用状态后端
//        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
//        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
//        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(1L), Time.minutes(3L)));
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/ck");
//        System.setProperty("HADOOP_USER_NAME", "atguigu");

          String groupid = "sku_user_order_window";
        SingleOutputStreamOperator<JSONObject> OrderDetailJsonObj = OrderDetailFilterFunction.getDwdOrderDetail(env, groupid);
        SingleOutputStreamOperator<TradeTrademarkCategoryUserSpuOrderBean> skuUserOrderDS = OrderDetailJsonObj.map(json -> TradeTrademarkCategoryUserSpuOrderBean.builder()
                .skuId(json.getString("sku_id"))
                .userId(json.getString("user_id"))
                .orderCount(1L)
                .orderAmount(json.getDouble("split_total_amount"))
                .build());




    }
}
