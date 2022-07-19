package com.zht.app.dws;


import com.zht.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


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




        //TODO 7.启动任务
        env.execute("DwsTrafficVcChArIsNewPageViewWindow");

    }

}
