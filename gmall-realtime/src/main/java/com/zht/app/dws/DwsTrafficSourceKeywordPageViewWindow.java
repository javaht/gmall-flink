package com.zht.app.dws;
/*
 * @Author root
 * @Data  2022/7/18 15:13
 * @Description
 * */

import com.zht.app.func.MyClickHouseUtil;
import com.zht.app.func.SplitFunction;
import com.zht.bean.KeywordBean;
import com.zht.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
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
                "rt,ts  " +
                "from  page_log   " +
                "where page['item'] is not null    " +
                "and   page['last_page_id'] ='search'  " +
                "and   page['item_type'] ='keyword'");
        tableEnv.createTemporaryView("key_word_table",keyWordtable);

        //注册udtf函数
        tableEnv.createFunction("SplitFunction", SplitFunction.class);

        //使用自定义函数分词处理
        Table splitTable = tableEnv.sqlQuery("SELECT    " +
                " word,   " +
                " rt   " +
                " FROM key_word_table, LATERAL TABLE(SplitFunction(key_word))");
        tableEnv.createTemporaryView("split_table",splitTable);
        //实现分组开窗聚合
        Table resultTable = tableEnv.sqlQuery("select 'search' source, " +
                "DATE_FORMAT(TUMBLE_START(rt,INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt," +   //窗口开始时间
                "DATE_FORMAT(TUMBLE_END(rt,INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss')  edt,"  +       //窗口结束时间
                "word  keyword," +
                "count(1) keyword_count," +
                "UNIX_TIMESTAMP() ts " +
                " from split_table group by TUMBLE(rt,INTERVAL '10' SECOND),word");
        DataStream<KeywordBean> keywordBeanDS = tableEnv.toAppendStream(resultTable, KeywordBean.class);
        keywordBeanDS.print(">>>>>>>>>>>>>>>>>>>>");


         //将数据写入clickhouse
        keywordBeanDS.addSink(MyClickHouseUtil.getClickHouseSink("insert into dws_traffic_source_keyword_page_view_window values(?,?,?,?,?,?)"));


        env.execute("DwsTrafficSourceKeywordPageViewWindow");
    }
}
