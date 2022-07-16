package com.zht.app.dwd.db;

import com.zht.utils.KafkaUtils;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.ZoneId;

public class DwdTradeOrderAdd {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // TODO 2. 启用状态后端
//        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
//        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
//        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(1L), Time.minutes(3L)));
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/ck");
//        System.setProperty("HADOOP_USER_NAME", "atguigu");
//        tableEnv.getConfig().setLocalTimeZone(ZoneId.of("GMT+8"));

        tableEnv.executeSql("" +
                "create table dwd_trade_order_pre_process( " +
                "id string, " +
                "order_id string, " +
                "user_id string, " +
                "order_status string, " +
                "sku_id string, " +
                "sku_name string, " +
                "province_id string, " +
                "activity_id string, " +
                "activity_rule_id string, " +
                "coupon_id string, " +
                "date_id string, " +
                "create_time string, " +
                "operate_date_id string, " +
                "operate_time string, " +
                "source_id string, " +
                "source_type string, " +
                "source_type_name string, " +
                "sku_num string, " +
                "split_original_amount string, " +
                "split_activity_amount string, " +
                "split_coupon_amount string, " +
                "split_total_amount string, " +
                "`type` string, " +
                "`old` map<string,string>, " +
                "od_ts string, " +
                "oi_ts string, " +
                "row_op_ts timestamp_ltz(3) )" + KafkaUtils.getKafkaDDL("dwd_trade_order_pre_process", "dwd_trade_order_detail"));


        // TODO 4. 过滤下单数据
        Table filteredTable = tableEnv.sqlQuery("" +
                "select " +
                "id,  " +
                "order_id,  " +
                "user_id,  " +
                "sku_id,  " +
                "sku_name,  " +
                "province_id,  " +
                "activity_id,  " +
                "activity_rule_id,  " +
                "coupon_id,  " +
                "date_id,  " +
                "create_time,  " +
                "source_id,  " +
                "source_type source_type_code,  " +
                "source_type_name,  " +
                "sku_num,  " +
                "split_original_amount,  " +
                "split_activity_amount,  " +
                "split_coupon_amount,  " +
                "split_total_amount,  " +
                "od_ts ts,  " +
                "row_op_ts  " +
                "from dwd_trade_order_pre_process where `type`='insert'");
        tableEnv.createTemporaryView("filtered_table", filteredTable);

        //创建kafka下单数据表
        tableEnv.executeSql("" +
                "create table dwd_trade_order_detail(   " +
                "id string,   " +
                "order_id string,   " +
                "user_id string,   " +
                "sku_id string,   " +
                "sku_name string,   " +
                "province_id string,   " +
                "activity_id string,   " +
                "activity_rule_id string,   " +
                "coupon_id string,   " +
                "date_id string,   " +
                "create_time string,   " +
                "source_id string,   " +
                "source_type_code string,   " +
                "source_type_name string,   " +
                "sku_num string,   " +
                "split_original_amount string,   " +
                "split_activity_amount string,   " +
                "split_coupon_amount string,   " +
                "split_total_amount string,   " +
                "ts string,   " +
                "row_op_ts timestamp_ltz(3),   " +
                "primary key(id) not enforced   " +
                ")" + KafkaUtils.getUpsertKafkaDDL("dwd_trade_order_detail"));

        // TODO 6. 将数据写出到 Kafka
        tableEnv.executeSql("insert into dwd_trade_order_detail  select * from filtered_table");

    }
}
