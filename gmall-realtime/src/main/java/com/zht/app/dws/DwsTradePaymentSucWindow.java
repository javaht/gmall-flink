package com.zht.app.dws;

import com.alibaba.fastjson.JSONObject;
import com.zht.bean.TradePaymentWindowBean;
import com.zht.utils.DateFormatUtil;
import com.zht.utils.MyClickHouseUtil;
import com.zht.utils.MyKafkaUtil;
import com.zht.utils.TimestampLtz3CompareUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import java.time.Duration;

/*
 * @Author root
 * @Data  2022/8/6 15:52
 * @Description
 * */
public class DwsTradePaymentSucWindow {
    public static void main(String[] args) throws Exception {

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1.1 状态后端设置
//        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
//        env.getCheckpointConfig().enableExternalizedCheckpoints(
//                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
//        );
//        env.setRestartStrategy(RestartStrategies.failureRateRestart(
//                3, Time.days(1), Time.minutes(1)
//        ));
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage(
//                "hdfs://hadoop102:8020/ck"
//        );
//        System.setProperty("HADOOP_USER_NAME", "atguigu");


         String  topic = "dwd_trade_pay_detail_suc";
        String  groupid = "dws_trade_payment_suc_window";

        DataStreamSource<String> kafkaDs = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupid));

        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDs.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    System.out.println(">>>>>>>>>>>>>>" + value);
                }
            }
        });
        //按照order_detail_id进行分组
        KeyedStream<JSONObject, String> jsonKeyByDetailId = jsonObjDS.keyBy(json -> json.getString("order_detail_id"));

        SingleOutputStreamOperator<JSONObject> filterDS = jsonKeyByDetailId.process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {

            private ValueState<JSONObject> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                valueState = getRuntimeContext().getState(new ValueStateDescriptor<JSONObject>("value-state", JSONObject.class));
            }

            @Override
            public void processElement(JSONObject value, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                //获取状态中的数据
                JSONObject state = valueState.value();

                if (null == state) {
                    valueState.update(value);
                    //定时器
                    ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 5000L);
                } else {
                    String statRt = state.getString("row_op_ts");
                    String curTs = value.getString("row_op_ts");
                    int compare = TimestampLtz3CompareUtil.compare(statRt, curTs);
                    if (compare != 1) {
                        valueState.update(value);
                    }
                }
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
                //输出并清空状态数据
                JSONObject value = valueState.value();
                out.collect(value);
                valueState.clear();
            }
        });

             //提取事件时间生成watermark
        SingleOutputStreamOperator<JSONObject> jsonObjWaterDS = filterDS.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                        return  DateFormatUtil.toTs(element.getString("callback_time"),true);
                    }
                }));


        KeyedStream<JSONObject, String> keyedByUidDS = jsonObjWaterDS.keyBy(json -> json.getString("user_id"));


        SingleOutputStreamOperator<TradePaymentWindowBean> tradePaymentDS = keyedByUidDS.flatMap(new RichFlatMapFunction<JSONObject, TradePaymentWindowBean>() {
            private ValueState<String> lastDtstate;
            @Override
            public void open(Configuration parameters) throws Exception {
                lastDtstate = getRuntimeContext().getState(new ValueStateDescriptor<String>("last-dt", String.class));
            }
            @Override
            public void flatMap(JSONObject value, Collector<TradePaymentWindowBean> out) throws Exception {
                //取出状态中的日期
                String lastDt = lastDtstate.value();
                String curDt = value.getString("callback_time").split(" ")[0];
                long paymentSucUniqueUserCount = 0L;
                long paymentSucNewUserCount = 0L;
                if (lastDt == null) {
                    paymentSucUniqueUserCount = 1L;
                    paymentSucNewUserCount = 1L;
                    lastDtstate.update(curDt);
                } else if (!lastDt.equals(curDt)) {
                    paymentSucUniqueUserCount = 1L;
                    lastDtstate.update(curDt);
                }

                if (paymentSucNewUserCount == 1) {
                    out.collect(new TradePaymentWindowBean(
                            "",
                            "",
                            paymentSucUniqueUserCount,
                            paymentSucNewUserCount,
                            null
                    ));
                }
            }
        });

         //开窗聚合
        SingleOutputStreamOperator<TradePaymentWindowBean> resultDS = tradePaymentDS.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<TradePaymentWindowBean>() {
                    @Override
                    public TradePaymentWindowBean reduce(TradePaymentWindowBean value1, TradePaymentWindowBean value2) throws Exception {
                        value1.setPaymentSucUniqueUserCount(value1.getPaymentSucUniqueUserCount() + value2.getPaymentSucUniqueUserCount());
                        value1.setPaymentSucNewUserCount(value1.getPaymentSucNewUserCount() + value2.getPaymentSucNewUserCount());
                        return value1;
                    }
                }, new AllWindowFunction<TradePaymentWindowBean, TradePaymentWindowBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<TradePaymentWindowBean> values, Collector<TradePaymentWindowBean> out) throws Exception {

                        TradePaymentWindowBean next = values.iterator().next();
                        next.setTs(System.currentTimeMillis());
                        next.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                        next.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                        out.collect(next);

                    }
                });
        resultDS.print(">>>>>>>>>>>>>>>>>>");

        resultDS.addSink(MyClickHouseUtil.getSinkFunction("insert into dws_trade_payment_suc_window  values(?,?,?,?,?)"));

        env.execute("DwsTradePaymentSucWindow");
    } 
    
      
}
