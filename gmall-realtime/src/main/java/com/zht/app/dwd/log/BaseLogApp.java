package com.zht.app.dwd.log;

import com.alibaba.fastjson.JSONObject;
import com.zht.utils.DateFormatUtil;
import com.zht.utils.KafkaUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.concurrent.TimeUnit;

public class BaseLogApp {
    public static void main(String[] args) {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setRestartStrategy(RestartStrategies.failureRateRestart(10, Time.of(1L, TimeUnit.DAYS), Time.of(3L, TimeUnit.MINUTES)));
        env.setStateBackend(new HashMapStateBackend());
        System.setProperty("HADOOP_USER_NAME", "root");
        //2.读取kafka topic_log数据主题的数据创建流
        DataStreamSource<String> kafKaDs = env.addSource(KafkaUtils.getKafkaConsumer("topic_log", "base_log_app"));


        OutputTag<String> dirtyTag = new OutputTag<String>("Dirty") {};

        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafKaDs.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    ctx.output(dirtyTag, value);
                }
            }
        });
        DataStream<String> sideOutput = jsonObjDS.getSideOutput(dirtyTag);
        sideOutput.print("Dirty>>>>>>>>>>");
        //3.将数据转换为json格式 并过滤掉非JSon格式的数据
        KeyedStream<JSONObject, String> keyedByMidStream = jsonObjDS.keyBy(json -> json.getJSONObject("common").getString("mid"));
        keyedByMidStream.map(new RichMapFunction<JSONObject, JSONObject>() {
            private ValueState<String> lastVisitDtState;
            @Override
            public void open(Configuration parameters) throws Exception {
                lastVisitDtState = getRuntimeContext().getState(new ValueStateDescriptor<String>("last-vist",String.class));
            }
            @Override
            public JSONObject map(JSONObject value) throws Exception {
                String isNew = value.getJSONObject("common").getString("is_new");
                String lastVisitDt = lastVisitDtState.value();
                //获取当前数据的时间
                Long ts = value.getLong("ts");
                //4.使用状态编程做新老用户校验
                if("1".equals(isNew)){
                    String curDt = DateFormatUtil.toDate(ts);
                    if(lastVisitDt==null){
                        lastVisitDtState.update(curDt);
                    }else if(!lastVisitDt.equals(curDt)){
                      value.getJSONObject("common").put("is_new","0");
                    }
                } else if(lastVisitDt==null){
                    String yesterday = DateFormatUtil.toDate(ts - 24 * 60 * 60 * 1000L);
                    lastVisitDtState.update(yesterday);
                }
                return value;
            }

        });

        //5.使用测输出流  对数据进行分流处理 页面浏览：主流   启动日志、曝光日志、动作日志、错误日志都放测输出流
        OutputTag<String> startTag = new OutputTag<>("start");
        OutputTag<String> displayTag = new OutputTag<>("display");
        OutputTag<String> actionTag = new OutputTag<>("action");
        OutputTag<String> errorTag = new OutputTag<>("error");
        //提取各个数据流的数据

        //将各个流的数据分别写出到kafka中
    }
}
