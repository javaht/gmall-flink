package com.zht.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.zht.bean.TrafficHomeDetailPageViewBean;
import com.zht.utils.DateFormatUtil;
import com.zht.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;

/*
 * @Author root
 * @Data  2022/7/21 14:54
 * @Description
 * */
public class DwsTrafficPageViewWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // TODO 2. 状态后端设置
//        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
//        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(1), Time.minutes(1)));
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/ck");
//        System.setProperty("HADOOP_USER_NAME", "root");


     String topic = "dwd_traffic_page_log";
     String groupid = "dws_traffic_page_view_window"; //ctrl+shift+U
        DataStreamSource<String> pageStringDs = env.addSource(MyKafkaUtil.getKafkaConsumer(topic, groupid));

        SingleOutputStreamOperator<JSONObject> jsonObjDs = pageStringDs.map(JSON::parseObject);
        //过滤出我们需要的数据
        SingleOutputStreamOperator<JSONObject> homeAndDetailPageDs = jsonObjDs.filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                String pageId = value.getJSONObject("page").getString("page_id");
                return "good_detail".equals(pageId) || "home".equals(pageId);
            }
        });
//        pageStringDs.flatMap(new FlatMapFunction<String, JSONObject>() {
//            @Override
//            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
//                JSONObject jsonObj = JSON.parseObject(value);
//                String pageId = jsonObj.getJSONObject("page").getString("page_id");
//                if("good_detail".equals(pageId) || "home".equals(pageId)){
//                    out.collect(jsonObj);
//                }
//            }
//        });
//
        //提取时间时间生成watermark
        SingleOutputStreamOperator<JSONObject> homeAndDetailPageWitjDS = homeAndDetailPageDs.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {

                    @Override
                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                        return element.getLong("ts");
                    }
                }));
        //按照MID分组
        KeyedStream<JSONObject, String> keyedStream = homeAndDetailPageWitjDS.keyBy(json -> json.getJSONObject("common").getString("mid"));


        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> trafficHomeDetailPageViewBeanSingleOutputStreamOperator = keyedStream.flatMap(new RichFlatMapFunction<JSONObject, TrafficHomeDetailPageViewBean>() {
            private ValueState<String> homeLastVisitDt;
            private ValueState<String> detailLastVisitDt;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> homeStateDescript = new ValueStateDescriptor<>("home-dt", String.class);
                StateTtlConfig Ttl = new StateTtlConfig.Builder(Time.days(1)).setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite).build();
                homeStateDescript.enableTimeToLive(Ttl);//设置TTL


                ValueStateDescriptor<String> detailStateDescript = new ValueStateDescriptor<String>("detail-dt", String.class);
                detailStateDescript.enableTimeToLive(Ttl);//设置TTl

                homeLastVisitDt = getRuntimeContext().getState(homeStateDescript);
                detailLastVisitDt = getRuntimeContext().getState(detailStateDescript);
            }

            @Override
            public void flatMap(JSONObject value, Collector<TrafficHomeDetailPageViewBean> out) throws Exception {
                String pageId = value.getJSONObject("page").getString("page_id");
                String curDt = DateFormatUtil.toDate(value.getLong("ts"));
                //默认的主页以及商品详情页的访问次数
                long homeUvCt = 0;
                long detailUvCt = 0;
                if ("home".equals(pageId)) {
                    String homeLastDt = homeLastVisitDt.value();
                    if (homeLastDt == null || !homeLastDt.equals(curDt)) { //当前第一次访问
                        homeUvCt = 1L;
                        homeLastVisitDt.update(curDt);
                    }
                } else {
                    String detailLastDt = detailLastVisitDt.value();
                    if (detailLastDt == null || !detailLastDt.equals(curDt)) { //当前第一次访问
                        detailUvCt = 1L;
                        detailLastVisitDt.update(curDt);
                    }

                }
                //封装javabean并写出数据
                if (homeUvCt != 0L || detailUvCt != 0) {
                    out.collect(new TrafficHomeDetailPageViewBean("", "",
                            homeUvCt,
                            detailUvCt,
                            System.currentTimeMillis()));
                }
            }
        });


        env.execute("dws_traffic_page_view_window");

    }
}
