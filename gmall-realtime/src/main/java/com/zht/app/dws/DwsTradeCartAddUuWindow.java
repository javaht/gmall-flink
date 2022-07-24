package com.zht.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.zht.app.func.MyClickHouseUtil;
import com.zht.bean.CartAddUuBean;
import com.zht.utils.DateFormatUtil;
import com.zht.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.time.Time;
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
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.fusesource.leveldbjni.All;

import java.time.Duration;

public class DwsTradeCartAddUuWindow {
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

        String topic = "dwd_trade_cart_add";
        String groupId = "dws_trade_cart_add_uu_window";
        DataStreamSource<String> kafkaDs = env.addSource(MyKafkaUtil.getKafkaConsumer(topic, groupId));

        SingleOutputStreamOperator<JSONObject> jsonObj = kafkaDs.map(JSON::parseObject);

        SingleOutputStreamOperator<JSONObject> waterMarkDs = jsonObj.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject element, long recordTimestamp) {

                        String dateTime = element.getString("operate_time");

                        if(dateTime ==null){
                            dateTime = element.getString("create_time");
                        }
                        return DateFormatUtil.toTs(dateTime, true);
                    }
                }));
        KeyedStream<JSONObject, String> JsonObj = waterMarkDs.keyBy(json -> json.getString("user_id"));

        SingleOutputStreamOperator<CartAddUuBean> flatDs = JsonObj.flatMap(new RichFlatMapFunction<JSONObject, CartAddUuBean>() {
            private ValueState<String> lastCartAddDt;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("", String.class);
                StateTtlConfig ttl = new StateTtlConfig.Builder(Time.days(1)).setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite).build();
                valueStateDescriptor.enableTimeToLive(ttl);
                lastCartAddDt = getRuntimeContext().getState(valueStateDescriptor);
            }

            @Override
            public void flatMap(JSONObject value, Collector<CartAddUuBean> out) throws Exception {
                String lastDt = lastCartAddDt.value();
                String createTime = value.getString("create_time");
                String curDt = createTime.split(" ")[0];
                //如果状态数据为null或者和当前日期不是同一天 则保留数据  更新状态
                if (lastDt == null || !lastDt.equals(curDt)) {
                    lastCartAddDt.update(curDt);
                    out.collect(new CartAddUuBean("", "", 1L, 0L));
                }
            }
        });


        SingleOutputStreamOperator<CartAddUuBean> reduceDs = flatDs.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)))
                .reduce(new ReduceFunction<CartAddUuBean>() {
                    @Override
                    public CartAddUuBean reduce(CartAddUuBean value1, CartAddUuBean value2) throws Exception {
                        value1.setCartAddUuCt(value1.getCartAddUuCt() + value2.getCartAddUuCt());
                        return value1;
                    }
                }, new AllWindowFunction<CartAddUuBean, CartAddUuBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<CartAddUuBean> values, Collector<CartAddUuBean> out) throws Exception {
                        CartAddUuBean cartAddUuBean = values.iterator().next();
                        cartAddUuBean.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                        cartAddUuBean.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));

                        cartAddUuBean.setTs(System.currentTimeMillis());

                        out.collect(cartAddUuBean);
                    }
                });

        reduceDs.print(">>>>>>>>>>>>");
        reduceDs.addSink(MyClickHouseUtil.getClickHouseSink(" insert into dws_trade_cart_add_uu_window values(?,?,?,?)"));
        env.execute("DwsTradeCartAddUuWindow");

    }
}
