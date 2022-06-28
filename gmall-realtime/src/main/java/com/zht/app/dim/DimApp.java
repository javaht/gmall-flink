package com.zht.app.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.source.MySqlSourceBuilder;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.zht.app.func.TableProcessFunction;
import com.zht.bean.TableProcess;
import com.zht.utils.KafkaUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;


import java.util.concurrent.TimeUnit;

public class DimApp {
    public static void main(String[] args) {

         //获取执行环境
        StreamExecutionEnvironment  env = new StreamExecutionEnvironment();
        env.setParallelism(4);
/*      env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setRestartStrategy(RestartStrategies.failureRateRestart(10, Time.of(1L, TimeUnit.DAYS), Time.of(3L, TimeUnit.MINUTES)));
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/gmall/ck");
        System.setProperty("HADOOP_USER_NAME", "atguigu");
 */
        //获取kafka的数据
         String topic = "topic_db";
         String groupid = "dim_sink_app";
        DataStreamSource<String> gmallDS  = env.addSource(KafkaUtils.getKafkaConsumer(topic, groupid));
        OutputTag<String> dirtyTag = new OutputTag<String>("dirty");

        //过滤json格式的数据
        SingleOutputStreamOperator<JSONObject> jsonObj = gmallDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    out.collect(jsonObject);
                } catch (JSONException e) {
                    ctx.output(dirtyTag, value);
                }
            }
        });
        DataStream<String> sideOutput = jsonObj.getSideOutput(dirtyTag);
        sideOutput.print("Dirty>>>>>>>>>>");
        //使用flinkcdc  读取mysql配置信息

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                 .hostname("hadoop102").port(3306)
                .username("root").password("123456").databaseList("gmall_config").tableList("gmall_config.table_process")
                .deserializer(new JsonDebeziumDeserializationSchema()).startupOptions(StartupOptions.initial()).build();
        DataStreamSource<String> mysqlSourceDs = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MysqlSource");


        //将配置信息处理成广播

        MapStateDescriptor<String, TableProcess> mapState = new MapStateDescriptor<>("mapState", String.class, TableProcess.class);//状态描述器
        BroadcastStream<String> broadcastStream = mysqlSourceDs.broadcast(mapState); //广播流


        //连接两个流
        BroadcastConnectedStream<JSONObject, String> connectedStream = jsonObj.connect(broadcastStream);



        // 根据广播数据疏离主流数据
        connectedStream.process();



         //将数据写入phoenix

        //启动
    }
}
