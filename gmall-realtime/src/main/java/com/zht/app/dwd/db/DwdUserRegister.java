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

public class DwdUserRegister {
    public static void main(String[] args) {
        // TODO 1. 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // TODO 2. 状态后端设置
        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(1), Time.minutes(1)));
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/ck");
        System.setProperty("HADOOP_USER_NAME", "atguigu");


        // TODO 3. 从 Kafka 读取业务数据，封装为 Flink SQL 表
        tableEnv.executeSql("create table topic_db(" +
                "`database` string, " +
                "`table` string, " +
                "`type` string, " +
                "`data` map<string, string>, " +
                "`ts` string " +
                ")" + KafkaUtils.getKafkaDDL("topic_db", "dwd_trade_order_detail"));

        // TODO 4. 读取用户表数据
        Table userInfo = tableEnv.sqlQuery("select " +
                "data['id'] user_id, " +
                "data['create_time'] create_time, " +
                "ts " +
                "from topic_db " +
                "where `table` = 'user_info' " +
                "and `type` = 'insert' ");
        tableEnv.createTemporaryView("user_info", userInfo);

        // TODO 5. 创建 Upsert-Kafka dwd_user_register 表
        tableEnv.executeSql("create table `dwd_user_register`( " +
                "`user_id` string, " +
                "`date_id` string, " +
                "`create_time` string, " +
                "`ts` string, " +
                "primary key(`user_id`) not enforced " +
                ")" + KafkaUtils.getUpsertKafkaDDL("dwd_user_register"));

        // TODO 6. 将输入写入 Upsert-Kafka 表
        tableEnv.executeSql("insert into dwd_user_register " +
                "select  " +
                "user_id, " +
                "date_format(create_time, 'yyyy-MM-dd') date_id, " +
                "create_time, " +
                "ts " +
                "from user_info");

    }
}
