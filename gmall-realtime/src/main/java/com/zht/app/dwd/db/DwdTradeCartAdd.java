package com.zht.app.dwd.db;
/*
 * @Author root
 * @Data  2022/7/13 17:03
 * @Description
 * */
import com.zht.utils.KafkaUtils;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdTradeCartAdd {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String topic = "";
        String groupid ="";
        DataStreamSource<String> kafkaDs = env.addSource(KafkaUtils.getKafkaConsumer(topic, groupid));





         tableEnv.executeSql("");
    }
}
