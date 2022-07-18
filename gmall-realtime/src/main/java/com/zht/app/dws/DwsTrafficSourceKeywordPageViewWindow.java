package com.zht.app.dws;
/*
 * @Author root
 * @Data  2022/7/18 15:13
 * @Description
 * */

import com.zht.app.func.SplitFunction;
import com.zht.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwsTrafficSourceKeywordPageViewWindow {
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

       //创建table  数据来源是dwd_traffic_page_log
        tableEnv.executeSql("create table  page_log( " +
                "`page` map<string,string>, " +
                "`ts` bigint, " +
                "`rt` AS TO_TIMESTAMP(FROM_UNIXTIME(ts/1000, 'yyyy-MM-dd HH:mm:ss')), " +
                "WATERMARK FOR rt AS rt - INTERVAL '2' SECOND )"+
                MyKafkaUtil.getKafkaDDL("dwd_traffic_page_log","DwsTrafficSourceKeywordPageViewWindow"));

        //获取目标数据 并保存为表
        Table keyWordtable = tableEnv.sqlQuery("select    " +
                "page['item']  key_word,  " +
                "rt  " +
                "from  page_log   " +
                "where page['item'] is not null    " +
                "and   page['last_page_id'] ='search'  " +
                "and   page['item_type'] ='keyword'");
        tableEnv.createTemporaryView("key_word_table",keyWordtable);

        //注册udtf函数
        tableEnv.createFunction("SplitFunction", SplitFunction.class);

        //使用自定义函数分词处理
        Table splitTable = tableEnv.sqlQuery("env.sqlQuery(SELECT    " +
                //              " key_word,   " +  原字段
                " rt,   " +
                " word,   " +
                " length   " +
                " FROM MyTable, LATERAL TABLE(SplitFunction(key_word)))");
        tableEnv.createTemporaryView("split_table",splitTable);



        env.execute("");
    }
}
