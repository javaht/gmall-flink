package com.zht.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.zht.utils.DateFormatUtil;
import com.zht.utils.KafkaUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DwdTrafficUniqueVisitorDetail {
    public static void main(String[] args) throws Exception {
             StreamExecutionEnvironment env =  StreamExecutionEnvironment.getExecutionEnvironment();
             env.setParallelism(1);
            //读取kafka  dwd_traffic_page_log 主题数据创建流
            String topic = "dwd_traffic_page_log";
            String groupid="dwd_traffic_unique_detail";
        DataStreamSource<String> kafkaDs = env.addSource(KafkaUtils.getKafkaConsumer(topic, groupid));
        //将每行数据转换为json对象
        SingleOutputStreamOperator<JSONObject> jsonObjDs = kafkaDs.map(line -> JSON.parseObject(line));
        //过滤掉上一跳页面id不等于null的数据
        SingleOutputStreamOperator<JSONObject> filterDs = jsonObjDs.filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                return value.getJSONObject("page").getString("last_page_id") == null;
            }
        });
        //按照Mid分组
        KeyedStream<JSONObject, String> keyedByMidStream = filterDs.keyBy(json -> json.getJSONObject("common").getString("mid"));
        //使用状态编程进行每日登陆数据去重
        SingleOutputStreamOperator<JSONObject> uvDetailDs = keyedByMidStream.filter(new RichFilterFunction<JSONObject>() {
            private ValueState<String> visitState;
            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("visitDt", String.class);
                //设置状态的TTL
                StateTtlConfig stateTtlConfig = new StateTtlConfig.Builder(Time.days(1)).setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite).build();
                valueStateDescriptor.enableTimeToLive(stateTtlConfig);
                visitState = getRuntimeContext().getState(valueStateDescriptor);
            }
            @Override
            public boolean filter(JSONObject value) throws Exception {
                String dt = visitState.value();
                String curDt = DateFormatUtil.toDate(value.getLong("ts"));
                if (dt == null || !dt.equals(curDt)) {
                    visitState.update(curDt);
                    return true;
                } else {
                    return false;
                }
            }
        });
        //将数据写出到kafka
        uvDetailDs.print("<<<<<<<<<<<<<<<<<");

        String targetTopic ="dwd_traffic_unique_visitor_detail";
        uvDetailDs.map(json->json.toJSONString()).addSink(KafkaUtils.getKafkaProducer(targetTopic));

        env.execute("DwdTrafficUniqueVisitorDetail");
    }
}
