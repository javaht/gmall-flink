package com.zht.app.dwd.db;
/*
 * @Author root
 * @Data  2022/7/13 17:03
 * @Description
 * */
import com.zht.utils.KafkaUtils;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;

public class DwdTradeCartAdd {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
//        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
//        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        env.setRestartStrategy(RestartStrategies.failureRateRestart(10, Time.of(1L, TimeUnit.DAYS), Time.of(3L, TimeUnit.MINUTES)));
//        env.setStateBackend(new HashMapStateBackend());
//        System.setProperty("HADOOP_USER_NAME", "root");
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5));//状态存储时间

        String topic = "topic_db";
        String groupid ="dwd_trade_cart_add";
        tableEnv.executeSql(KafkaUtils.getTopicDbDDL(groupid));
        Table cartAdd = tableEnv.sqlQuery("" +
                "select  " +
                "data['id'] id,  " +
                "data['user_id'] user_id,  " +
                "data['sku_id'] sku_id,  " +
                "data['source_id'] source_id,  " +
                "data['source_type'] source_type,  " +
                "if(`type` = 'insert', data['sku_num'],cast((cast(data['sku_num'] as int) - cast(`old`['sku_num'] as int)) as string)) sku_num, " +
                "ts,  " +
                "proc_time  " +
                "from `topic_db`   " +
                "where `table` = 'cart_info'  " +
                "and (`type` = 'insert' or (`type` = 'update'  and `old`['sku_num'] is not null  and cast(data['sku_num'] as int) > cast(`old`['sku_num'] as int)))");
        tableEnv.createTemporaryView("cart_add", cartAdd);
    }
}
