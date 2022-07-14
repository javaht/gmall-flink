package com.zht.app.dwd.db;
import com.zht.utils.KafkaUtils;
import com.zht.utils.MysqlUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;


/*
*
1.读取mysql购物车表创建购物车的临时表为card_add
2. base_dic为lookup table作为维表 关联  card_add 得到关联后的表resultTable
3. 创建kafka中关联后的表  将resultTable插入数据
4. 插入数据
* */
public class DwdTradeCartAdd {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
//        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
//        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        env.setRestartStrategy(RestartStrategies.failureRateRestart(10, Time.of(1L, TimeUnit.DAYS), Time.of(3L, TimeUnit.MINUTES)));
//        env.setStateBackend(new HashMapStateBackend());
//        System.setProperty("HADOOP_USER_NAME", "root");
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5));//状态存储时间

        String topic = "topic_db";
        String groupid ="dwd_trade_cart_add";
        tableEnv.executeSql(KafkaUtils.getTopicDbDDL(groupid));

           //这里只要新增购物车的数据 所以类型是插入或者更新并且old[sku_num] 不可以为空
        Table cartAddTable = tableEnv.sqlQuery("" +
                "select " +
                "    data['id'] id, " +
                "    data['user_id'] user_id, " +
                "    data['sku_id'] sku_id, " +
                "    data['cart_price'] cart_price, " +
                "    if(`type`='insert',cast(`data`['sku_num'] as int),cast(`data`['sku_num'] as int) - cast(`old`['sku_num'] as int)) sku_num, " +
                "    data['sku_name'] sku_name, " +
                "    data['is_checked'] is_checked, " +
                "    data['create_time'] create_time, " +
                "    data['operate_time'] operate_time, " +
                "    data['is_ordered'] is_ordered, " +
                "    data['order_time'] order_time, " +
                "    data['source_type'] source_type, " +
                "    data['source_id'] source_id, " +
                "    pt " +
                "from topic_db where `database`='gmall' " +
                "and `table`='cart_info' and (`type`='insert'  or (`type`='update'   and `old`['sku_num'] is not null    and cast(`data`['sku_num'] as int) > cast(`old`['sku_num'] as int)))");
        tableEnv.createTemporaryView("card_add", cartAddTable);

        //构建base_dic的lookup
        tableEnv.executeSql(MysqlUtils.getBaseDicLookUpDDL());
//        Table baseTable = tableEnv.sqlQuery("select * from base_dic");
//        DataStream<Row> baseTableStream = tableEnv.toAppendStream(baseTable, Row.class);
//        baseTableStream.print(">>>>>>>>>>>>>>>");

        Table resultTable = tableEnv.sqlQuery("select  " +
                "c.id,  " +
                "c.user_id,  " +
                "c.sku_id,  " +
                "c.cart_price,  " +
                "c.sku_num,  " +
                "c.sku_name,  " +
                "c.is_checked,  " +
                "c.create_time,  " +
                "c.operate_time,  " +
                "c.is_ordered,  " +
                "c.order_time,  " +
                "c.source_type,  " +
                "c.source_id,  " +
                "b.dic_name  " +
                "from card_add c join base_dic for SYSTEM_TIME as OF c.pt as b on c.source_type = b.dic_code");
        tableEnv.createTemporaryView("resultTable",resultTable);
//        Table table = tableEnv.sqlQuery("select * from resultTable");
//        DataStream<Row> rowDataStream = tableEnv.toAppendStream(table, Row.class);
//        rowDataStream.print(">>>>>>>>>>>>>>>>>");

        String sinkTopic = "dwd_trade_card_add";
        tableEnv.executeSql("CREATE TABLE dwd_trade_card_add(   " +
                "id   string,   " +
                "user_id   string,   " +
                "sku_id   string,   " +
                "cart_price   string,   " +
                "sku_num    int,   " +
                "sku_name   string,   " +
                "is_checked   string,   " +
                "create_time   string,   " +
                "operate_time   string,   " +
                "is_ordered   string,   " +
                "order_time   string,   " +
                "source_type   string,   " +
                "source_id   string,   " +
                "dic_name     string   " +
                ") "+KafkaUtils.getKafkaDDL(sinkTopic,""));

        tableEnv.executeSql("insert into dwd_trade_card_add select * from resultTable").print();



        env.execute("DwdTradeCartAdd");

    }
}
