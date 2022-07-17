package com.zht.app.dwd.db;

import com.alibaba.fastjson.JSON;
import com.zht.bean.CoupouUseOrderBean;
import com.zht.bean.OrderRefundBean;
import com.zht.utils.KafkaUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Map;
import java.util.Set;

public class DwdToolCouponOrder {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        // TODO 2. 状态后端设置
//        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
//        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(1), Time.minutes(1)));
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/ck");
//        System.setProperty("HADOOP_USER_NAME", "atguigu");

        // TODO 3. 从 Kafka 读取业务数据，封装为 Flink SQL 表
        tableEnv.executeSql("create table `topic_db`( " +
                "`database` string, " +
                "`table` string, " +
                "`data` map<string, string>, " +
                "`type` string, " +
                "`ts` string " +
                ")" + KafkaUtils.getKafkaDDL("topic_db", "dwd_tool_coupon_order"));

        // TODO 5. 读取订单表数据，筛选退单数据
        Table couponUseOrderDS = tableEnv.sqlQuery("select   " +
                "data['id'] id,   " +
                "data['province_id'] province_id,   " +
                "`old`   " +
                "from topic_db   " +
                "where `table` = 'coupon_use'   " +
                "and `type` = 'update' ");
        DataStream<CoupouUseOrderBean> orderRefundBeanDataStream = tableEnv.toAppendStream(couponUseOrderDS, CoupouUseOrderBean.class);

        SingleOutputStreamOperator<CoupouUseOrderBean> filterDS = orderRefundBeanDataStream.filter(
                data -> {
                    String old = data.getOld();
                    if (old != null) {
                        Map oldmap = JSON.parseObject(old, Map.class);
                        Set changeKey = oldmap.keySet();
                        return changeKey.contains("order_status");
                    }
                    return false;
                }
        );
        Table resultTable = tableEnv.fromDataStream(filterDS);

        // TODO 5. 建立 Upsert-Kafka dwd_tool_coupon_get 表
        tableEnv.executeSql("create table dwd_tool_coupon_order (   " +
                "id string,   " +
                "coupon_id string,   " +
                "user_id string,   " +
                "order_id string, "+
                "date_id string,   " +
                "order_time string,   " +
                "ts string,   " +
                "primary key(id) not enforced   " +
                ")" + KafkaUtils.getUpsertKafkaDDL("dwd_tool_coupon_order"));

        // TODO 6. 将数据写入 Upsert-Kafka 表
        tableEnv.executeSql("insert into dwd_tool_coupon_order select " +
                "id     " +
                "coupon_id     " +
                "user_id     " +
                "order_id     " +
                "date_id     " +
                "using_time  order_time     " +
                "ts  from result_table");

env.execute("");

    }
}
