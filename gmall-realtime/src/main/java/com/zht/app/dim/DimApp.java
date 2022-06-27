package com.zht.app.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.gson.JsonObject;
import com.zht.utils.KafkaUtils;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
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


        //定义脏数据
        OutputTag<String> dirty = new OutputTag<>("dirty");

        //过滤json格式的数据
        SingleOutputStreamOperator<JSONObject> jsonObj = gmallDS.process(new ProcessFunction<String, JSONObject>() {

            @Override
            public void processElement(String value, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    ctx.output(dirty, value);
                }
            }
        });

         //取出脏数据并打印
        DataStream<String> sideOutput = jsonObj.getSideOutput(dirty);
        sideOutput.print("sideOutput>>>>>>>>>>>>>");

        //使用flinkcdc  读取mysql配置信息

        //将配置信息处理成广播

        //连接两个流


       // 根据广播数据疏离主流数据


         //将数据写入phoenix

        //启动
    }
}
