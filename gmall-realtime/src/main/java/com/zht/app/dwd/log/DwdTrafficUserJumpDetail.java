package com.zht.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.zht.utils.KafkaUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class DwdTrafficUserJumpDetail {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
         String topic = "dwd_traffic_page_log";
         String groupid = "Dwd_Traffic_User_Jump_Detail";
        DataStreamSource<String> kafkaDs = env.addSource(KafkaUtils.getKafkaConsumer(topic, groupid));
        SingleOutputStreamOperator<JSONObject> jsonObjDs = kafkaDs.map(line -> JSON.parseObject(line));
        SingleOutputStreamOperator<JSONObject> jsonObjWithWatermarkDs = jsonObjDs.assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner((SerializableTimestampAssigner<JSONObject>) (element, recordTimestamp) -> element.getLong("ts")));

        KeyedStream<JSONObject, String> keyedByMidStream = jsonObjWithWatermarkDs.keyBy(json -> json.getJSONObject("common").getString("mid"));

        //开始写cep了
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("first").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                return value.getJSONObject("page").getString("last_page_id") == null;
            }
        }).times(2).consecutive().within(Time.seconds(10));

        //将cep作用到流上
        PatternStream<JSONObject> patternStream = CEP.pattern(keyedByMidStream, pattern);

        //提取事件
        OutputTag<JSONObject> timeoutputTag = new OutputTag<JSONObject>("time-out"){};


        SingleOutputStreamOperator<JSONObject> selectDS = patternStream.select(timeoutputTag,
                new PatternTimeoutFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject timeout(Map<String, List<JSONObject>> map, long l) throws Exception {
                        return map.get("first").get(0);
                    }
                }, new PatternSelectFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject select(Map<String, List<JSONObject>> map) throws Exception {
                        return map.get("first").get(0);//对应上边的times(2)
                    }
                });

        DataStream<JSONObject> timeOutDS = selectDS.getSideOutput(timeoutputTag);
        selectDS.print("SELECT>>>>>>>>>>>>>>>>>");
        timeOutDS.print("TIME>>>>>>>>>>>>>>>>>>>>>>>");
        DataStream<JSONObject> unionDS = selectDS.union(timeOutDS);
        unionDS.map(json->json.toJSONString()).addSink(KafkaUtils.getKafkaProducer("dwd_traffic_user_jump_detail"));

        env.execute("DwdTrafficUserJumpDetail");
    }
}
