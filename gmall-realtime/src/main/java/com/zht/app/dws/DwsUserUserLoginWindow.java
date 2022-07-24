package com.zht.app.dws;
/*
 * @Author root
 * @Data  2022/7/22 13:26
 * @Description
 * */

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.zht.app.func.MyClickHouseUtil;
import com.zht.bean.UserLoginBean;
import com.zht.utils.DateFormatUtil;
import com.zht.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class DwsUserUserLoginWindow {
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
        String groupId = "dws_user_user_login_window"; //ctrl+shift+U
        DataStreamSource<String> kafkaDs = env.addSource(MyKafkaUtil.getKafkaConsumer(topic, groupId));
        //TODO 这里将数据转换为JSON  并且过滤出我们需要的数据
        SingleOutputStreamOperator<JSONObject> filterDs = kafkaDs.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                JSONObject jsonObj = JSON.parseObject(value);
                String uid = jsonObj.getJSONObject("common").getString("uid");
                String lastPageId = jsonObj.getJSONObject("page").getString("last_page_id");
                if (uid != null && lastPageId == null) {
                    out.collect(jsonObj);
                }
            }
        });

        //TODO 生成watermark
        SingleOutputStreamOperator<JSONObject> watermarkDs = filterDs.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                        return element.getLong("ts");
                    }
                }));

        //TODO  按照Uid分组
        KeyedStream<JSONObject, String> keyedDs = watermarkDs.keyBy(json -> json.getJSONObject("common").getString("uid"));


        SingleOutputStreamOperator<UserLoginBean> uvDs = keyedDs.process(new KeyedProcessFunction<String, JSONObject, UserLoginBean>() {
            private ValueState<String> lastVisitDt;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("last-dt", String.class);
                lastVisitDt = getRuntimeContext().getState(valueStateDescriptor);
            }

            @Override
            public void processElement(JSONObject value, KeyedProcessFunction<String, JSONObject, UserLoginBean>.Context ctx, Collector<UserLoginBean> out) throws Exception {
                String lastDt = lastVisitDt.value();//上一次保存的日期
                Long ts = value.getLong("ts");
                String curDt = DateFormatUtil.toDate(ts);
                long uuCt = 0L; //定义独立用户
                long backCt = 0L; //定义回流用户
                if (lastDt == null) { //表示为新用户
                    uuCt = 1L;
                    lastVisitDt.update(curDt);
                } else {
                    //状态保存日期不为null 且与当前日期不同 则为今天第一条数据
                    if (!lastDt.equals(curDt)) {
                        uuCt = 1L;
                        lastVisitDt.update(curDt);
                        //两个日期相差>7天 就是回流用户
                        long days = (ts- DateFormatUtil.toTs(lastDt)) / (1000L * 60 * 60 * 24);
                        if (days >= 8L) {
                            backCt = 1L;
                            //lastVisitDt.update(curDt);
                        }

                    }
                }
                //如果当日独立用户为1 则需要写出
                if (uuCt == 1L ) {

                    out.collect(new UserLoginBean("",
                            ""
                            , backCt
                            , uuCt,
                            System.currentTimeMillis()
                    ));
                }
            }
        });


        //开窗聚合
        SingleOutputStreamOperator<UserLoginBean> resultDs = uvDs.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<UserLoginBean>() {
                    @Override
                    public UserLoginBean reduce(UserLoginBean value1, UserLoginBean value2) throws Exception {
                        value1.setBackCt(value1.getBackCt() + value2.getBackCt());
                        value1.setUuCt(value1.getUuCt() + value2.getUuCt());
                        return value1;
                    }
                }, new AllWindowFunction<UserLoginBean, UserLoginBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<UserLoginBean> values, Collector<UserLoginBean> out) throws Exception {
                        String start = DateFormatUtil.toYmdHms(window.getStart());
                        String end = DateFormatUtil.toYmdHms(window.getEnd());

                        UserLoginBean userLoginBean = values.iterator().next();
                        userLoginBean.setStt(start);
                        userLoginBean.setEdt(end);
                        out.collect(userLoginBean);

                    }
                });

        resultDs.print(">>>>>>>>>>>>>>>>>>>>>>>>>>>");
         //写出到clickhouse
        resultDs.addSink(MyClickHouseUtil.getClickHouseSink(  "insert into dws_user_user_login_window values(?,?,?,?,?)"));

        env.execute("DwsUserUserLoginWindow");
    }
}
