package com.zht.app.dwd.db;

import com.zht.utils.KafkaUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

public class DwdTradeOrderPreProcess {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //设置状态存储时间
        tableEnv.getConfig().setIdleStateRetention(Duration.ofDays(10));

         //读取kafka topic_db主题的数据
          tableEnv.executeSql(KafkaUtils.getTopicDbDDL("dwd_trade_order_detail"));
          //过滤出订单数据
          tableEnv.sqlQuery("");




          env.execute("DwdTradeOrderPreProcess");

    }
}
