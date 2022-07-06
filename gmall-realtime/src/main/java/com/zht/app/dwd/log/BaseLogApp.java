package com.zht.app.dwd.log;

import com.alibaba.fastjson.JSONArray;
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
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.concurrent.TimeUnit;

public class BaseLogApp {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
//        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
//        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        env.setRestartStrategy(RestartStrategies.failureRateRestart(10, Time.of(1L, TimeUnit.DAYS), Time.of(3L, TimeUnit.MINUTES)));
//        env.setStateBackend(new HashMapStateBackend());
//        System.setProperty("HADOOP_USER_NAME", "root");
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
        SingleOutputStreamOperator<JSONObject> jsonObjWithNewFlahDS = keyedByMidStream.map(new RichMapFunction<JSONObject, JSONObject>() {
            private ValueState<String> lastVisitDtState;
            @Override
            public void open(Configuration parameters) throws Exception {
                lastVisitDtState = getRuntimeContext().getState(new ValueStateDescriptor<String>("last-vist", String.class));
            }
            @Override
            public JSONObject map(JSONObject value) throws Exception {
                String isNew = value.getJSONObject("common").getString("is_new");
                String lastVisitDt = lastVisitDtState.value();
                //获取当前数据的时间
                Long ts = value.getLong("ts");
                //4.使用状态编程做新老用户校验
                if ("1".equals(isNew)) {
                    String curDt = DateFormatUtil.toDate(ts);
                    if (lastVisitDt == null) {
                        lastVisitDtState.update(curDt);
                    } else if (!lastVisitDt.equals(curDt)) {
                        value.getJSONObject("common").put("is_new", "0");
                    }
                } else if (lastVisitDt == null) {
                    String yesterday = DateFormatUtil.toDate(ts - 24 * 60 * 60 * 1000L);
                    lastVisitDtState.update(yesterday);
                }
                return value;
            }
        });

        //5.使用测输出流  对数据进行分流处理 页面浏览：主流   启动日志、曝光日志、动作日志、错误日志都放测输出流
        OutputTag<String> startTag = new OutputTag<String>("start"){};
        OutputTag<String> displayTag = new OutputTag<String>("display"){};
        OutputTag<String> actionTag = new OutputTag<String>("action"){};
        OutputTag<String> errorTag = new OutputTag<String>("error"){};
        //提取各个数据流的数据

        SingleOutputStreamOperator<String> pageDs = jsonObjWithNewFlahDS.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject value, ProcessFunction<JSONObject, String>.Context ctx, Collector<String> out) throws Exception {
                String jsonString = value.toJSONString();

                String error = value.getString("err");
                if (error != null) {
                    //输出错误日志
                    ctx.output(errorTag, value.toJSONString());
                }

                String start = value.getString("start");
                if (start != null) {
                    //输出数据到启动日志
                    ctx.output(startTag, value.toJSONString());
                } else {
                    //尝试获取曝光数据  取出页面id和时间戳
                    String pageId = value.getJSONObject("page").getString("page_id");
                    Long ts = value.getLong("ts");
                    String common = value.getString("common");

                    JSONArray displays = value.getJSONArray("displays");
                    if (displays!=null && displays.size() > 0) {
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject display = displays.getJSONObject(i);
                            display.put("page_id", pageId);
                            display.put("ts", ts);
                            display.put("common", common);
                            ctx.output(displayTag, display.toJSONString());
                        }
                    }

                    JSONArray actions = value.getJSONArray("actions");
                    if (actions!=null && actions.size() > 0) {
                        for (int i = 0; i < actions.size(); i++) {
                            JSONObject action = actions.getJSONObject(i);
                            action.put("page_id", pageId);
                            action.put("ts", ts);
                            action.put("common", common);
                            ctx.output(actionTag, action.toJSONString());
                        }
                    }

                    value.remove("displays");
                    value.remove("actions");
                    out.collect(value.toJSONString());
                }
            }
        });

        //将各个流的数据分别写出到kafka中
        DataStream<String> startDs = pageDs.getSideOutput(startTag);
        DataStream<String> errorDs = pageDs.getSideOutput(errorTag);
        DataStream<String> displayDs = pageDs.getSideOutput(displayTag);
        DataStream<String> actionDs = pageDs.getSideOutput(actionTag);


        pageDs.print("Page>>>>>>>>>>>>>>>>");
        startDs.print("startDs>>>>>>>>>>>>>>>>");
        errorDs.print("errorDs>>>>>>>>>>>>>>>>");
        displayDs.print("displayDs>>>>>>>>>>>>>>>>");
        actionDs.print("actionDs>>>>>>>>>>>>>>>>");


        String page_topic = "dwd_traffic_page_log";
        String start_topic = "dwd_traffic_start_log";
        String display_topic = "dwd_traffic_display_log";
        String action_topic = "dwd_traffic_action_log";
        String error_topic = "dwd_traffic_error_log";

        pageDs.addSink(KafkaUtils.getKafkaProducer(page_topic));
        pageDs.addSink(KafkaUtils.getKafkaProducer(start_topic));
        pageDs.addSink(KafkaUtils.getKafkaProducer(display_topic));
        pageDs.addSink(KafkaUtils.getKafkaProducer(action_topic));
        pageDs.addSink(KafkaUtils.getKafkaProducer(error_topic));

        env.execute("BaseApp");


    }
}
