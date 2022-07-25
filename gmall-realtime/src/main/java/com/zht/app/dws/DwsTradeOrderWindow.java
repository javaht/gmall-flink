package com.zht.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.zht.app.func.MyClickHouseUtil;
import com.zht.bean.CartAddUuBean;
import com.zht.bean.TradeOrderBean;
import com.zht.utils.DateFormatUtil;
import com.zht.utils.MyKafkaUtil;
import com.zht.utils.TimestampLtz3CompareUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
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
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class DwsTradeOrderWindow {
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

        String topic = "dwd_trade_order_detail";
        String groupId = "dws_trade_order_window";
        DataStreamSource<String> orderDetailDs = env.addSource(MyKafkaUtil.getKafkaConsumer(topic, groupId));

        SingleOutputStreamOperator<JSONObject> jsonObj = orderDetailDs.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                if (!"".equals(value)) {
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    if ("insert".equals(jsonObject.getString("type"))) {
                        out.collect(jsonObject);
                    }
                }
            }
        });
        KeyedStream<JSONObject, String> Keyedstream = jsonObj.keyBy(line -> line.getString("order_detail_id"));



        //这里实现只要最新的数据
        SingleOutputStreamOperator<JSONObject> orderDetailJsonObjDs = Keyedstream.process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {
            private ValueState<JSONObject> orderDetailState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<JSONObject> descriptor = new ValueStateDescriptor<>("order-detail", JSONObject.class);
                orderDetailState = getRuntimeContext().getState(descriptor);
            }

            @Override
            public void processElement(JSONObject value, Context ctx, Collector<JSONObject> out) throws Exception {
                JSONObject orderDetail = orderDetailState.value();

                if (orderDetail == null) {
                    //把当前数据设置状态并且注册定时器
                    orderDetailState.update(value);
                    long time = ctx.timerService().currentProcessingTime();
                    ctx.timerService().registerEventTimeTimer(time + 2000L);//延迟2秒
                } else {
                    String stateTs = orderDetail.getString("ts");  //上次时间
                    String curTs = value.getString("ts");  //本次时间

                    int compare = TimestampLtz3CompareUtil.compare(stateTs, curTs);
                    if (compare != -1) {
                        orderDetailState.update(value);
                    }
                }
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
                //提取状态数据并输出
                JSONObject orderDetail = orderDetailState.value();
                out.collect(orderDetail);
            }
        });

        SingleOutputStreamOperator<JSONObject> watermarkDs = orderDetailJsonObjDs.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                        String create_time = element.getString("create_time");
                        return DateFormatUtil.toTs(create_time,true);
                    }
                }));

        KeyedStream<JSONObject, String> keyedByUidStream = watermarkDs.keyBy(line -> line.getString("user_id"));

        SingleOutputStreamOperator<TradeOrderBean> tradeOrderDs = keyedByUidStream.flatMap(new RichFlatMapFunction<JSONObject, TradeOrderBean>() {

            private ValueState<String> lastorderDt;

            @Override
            public void open(Configuration parameters) throws Exception {
                lastorderDt = getRuntimeContext().getState(new ValueStateDescriptor<String>("last-order", String.class));
            }

            @Override
            public void flatMap(JSONObject value, Collector<TradeOrderBean> out) throws Exception {
                String lastOrder = lastorderDt.value();

                long orderUniqueUserCount = 0;
                long orderNewUserCount = 0;
                  //当前数据下单日期
                String curDt = value.getString("order_create_time").split(" ")[0];

                if (lastOrder == null) {
                    orderUniqueUserCount = 1;
                    orderNewUserCount = 1;
                    lastorderDt.update(curDt);
                } else {

                    if (!lastorderDt.equals(curDt)) {
                        orderUniqueUserCount = 1L;
                        lastorderDt.update(curDt);
                    }
                }
                Double activityReduceAmount = value.getDouble("activity_reduce_amount");
                Double couponReduceAmount = value.getDouble("coupon_reduce_amount");
                if (activityReduceAmount == null) {
                    activityReduceAmount = 0.0D;
                }
                if (couponReduceAmount == null) {
                    couponReduceAmount = 0.0D;
                }

                out.collect(new TradeOrderBean(
                        "",
                        "",
                        orderUniqueUserCount,
                        orderNewUserCount,
                        activityReduceAmount,
                        couponReduceAmount,
                        value.getDouble("original_total_amount"),
                        0L
                ));
            }
        });
        SingleOutputStreamOperator<TradeOrderBean> resultDs = tradeOrderDs.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<TradeOrderBean>() {
                    @Override
                    public TradeOrderBean reduce(TradeOrderBean value1, TradeOrderBean value2) throws Exception {

                        value1.setOrderUniqueUserCount(value1.getOrderUniqueUserCount() + value2.getOrderUniqueUserCount());
                        value1.setOrderNewUserCount(value1.getOrderNewUserCount() + value2.getOrderNewUserCount());
                        value1.setOrderActivityReduceAmount(value1.getOrderActivityReduceAmount() + value2.getOrderActivityReduceAmount());
                        value1.setOrderCouponReduceAmount(value1.getOrderCouponReduceAmount() + value2.getOrderCouponReduceAmount());
                        value1.setOrderOriginalTotalAmount(value1.getOrderOriginalTotalAmount() + value2.getOrderOriginalTotalAmount());

                        return value1;
                    }
                }, new AllWindowFunction<TradeOrderBean, TradeOrderBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<TradeOrderBean> values, Collector<TradeOrderBean> out) throws Exception {
                        TradeOrderBean tradeOrderBean = values.iterator().next();
                        tradeOrderBean.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                        tradeOrderBean.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                        tradeOrderBean.setTs(System.currentTimeMillis());
                        out.collect(tradeOrderBean);
                    }
                });


        resultDs.addSink(MyClickHouseUtil.getClickHouseSink("INSERT INTO dws_trade_order_window  values(?,?,?,?,?,?,?,?)"));
        env.execute("DwsTradeOrderWindow");
    }
}
