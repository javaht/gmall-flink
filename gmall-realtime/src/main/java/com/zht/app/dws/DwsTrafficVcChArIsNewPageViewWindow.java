package com.zht.app.dws;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.zht.bean.TrafficPageViewBean;
import com.zht.utils.DateFormatUtil;
import com.zht.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import scala.Tuple4;


import java.time.Duration;

public class DwsTrafficVcChArIsNewPageViewWindow {

    public static void main(String[] args) throws Exception {

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1.1 状态后端设置
//        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
//        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(1), Time.minutes(1)));
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/ck");
//        System.setProperty("HADOOP_USER_NAME", "root");

        //读取3个主题的数据创建三个流
        String page_topic = "dwd_traffic_page_log";
        String uv_topic = "dwd_traffic_unique_visitor_detail";
        String uj_topic = "dwd_traffic_user_jump_detail";
        String groupId = "dws_traffic_vc_ch_ar_isnew_page_view_window";

        DataStreamSource<String> pageStringDS = env.addSource(MyKafkaUtil.getKafkaConsumer(page_topic, groupId));
        DataStreamSource<String> uvStringDS = env.addSource(MyKafkaUtil.getKafkaConsumer(uv_topic, groupId));
        DataStreamSource<String> ujStringDS = env.addSource(MyKafkaUtil.getKafkaConsumer(uj_topic, groupId));


         //处理UV数据
        SingleOutputStreamOperator<TrafficPageViewBean> trafficPageViewWithUvDs = uvStringDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            JSONObject common = jsonObject.getJSONObject("common");

            return new TrafficPageViewBean("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"), 1L, 0L, 0L, 0L, 0L, jsonObject.getLong("ts"));
        });
        //处理Uj数据
        SingleOutputStreamOperator<TrafficPageViewBean> trafficPageViewWithUjDs = ujStringDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            JSONObject common = jsonObject.getJSONObject("common");

            return new TrafficPageViewBean("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L, 0L, 0L, 0L, 1L,
                    jsonObject.getLong("ts"));
        });

        SingleOutputStreamOperator<TrafficPageViewBean> trafficPageViewWithPvDs = pageStringDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            JSONObject common = jsonObject.getJSONObject("common");
            JSONObject page = jsonObject.getJSONObject("page");
            String lastPageId = page.getString("last_page_id");
            long sv = 0L;
            if (lastPageId == null) {
                sv = 1;
            }
            return new TrafficPageViewBean("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L, sv, 1L, page.getLong("during_time"), 0L,
                    jsonObject.getLong("ts"));
        });
        SingleOutputStreamOperator<TrafficPageViewBean> unionDs = trafficPageViewWithPvDs.union(trafficPageViewWithUjDs, trafficPageViewWithUjDs).
                assignTimestampsAndWatermarks(WatermarkStrategy.<TrafficPageViewBean>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                        .withTimestampAssigner(new SerializableTimestampAssigner<TrafficPageViewBean>() {
                            @Override
                            public long extractTimestamp(TrafficPageViewBean element, long recordTimestamp) {
                                return element.getTs();
                            }
                        }));

        KeyedStream<TrafficPageViewBean, Tuple4<String, String, String, String>> keyedStream = unionDs.keyBy(new KeySelector<TrafficPageViewBean, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> getKey(TrafficPageViewBean value) throws Exception {
                return new Tuple4<>(value.getAr(), value.getCh(), value.getIsNew(), value.getAr());
            }
        });


        WindowedStream<TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow> windowedStream = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10)));

        SingleOutputStreamOperator<TrafficPageViewBean> reduceDS = windowedStream.reduce(new ReduceFunction<TrafficPageViewBean>() {
            @Override
            public TrafficPageViewBean reduce(TrafficPageViewBean value1, TrafficPageViewBean value2) throws Exception {

                value1.setUvCt(value1.getUvCt() + value2.getUvCt());
                value1.setUjCt(value1.getUjCt() + value2.getUjCt());
                value1.setDurSum(value1.getDurSum() + value2.getDurSum());
                value1.setPvCt(value1.getPvCt() + value2.getPvCt());
                value1.setSvCt(value1.getSvCt() + value2.getSvCt());
                return value1;
            }
        }, new WindowFunction<TrafficPageViewBean, TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow>() {
            @Override
            public void apply(Tuple4<String, String, String, String> key, TimeWindow window, Iterable<TrafficPageViewBean> input, Collector<TrafficPageViewBean> out) throws Exception {
                //获取数据
                TrafficPageViewBean trafficPageViewBean = input.iterator().next();
                //获取窗口信息
                long start = window.getStart();
                long end = window.getEnd();
                //补充窗口信息
                trafficPageViewBean.setStt(DateFormatUtil.toYmdHms(start));
                trafficPageViewBean.setEdt(DateFormatUtil.toYmdHms(end));

                //输出信息
                out.collect(trafficPageViewBean);
            }
        });


        //TODO 7.启动任务
        env.execute("DwsTrafficVcChArIsNewPageViewWindow");

    }

}
