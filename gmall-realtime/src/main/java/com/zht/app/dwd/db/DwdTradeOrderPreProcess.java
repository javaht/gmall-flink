package com.zht.app.dwd.db;

import com.zht.utils.KafkaUtils;
import com.zht.utils.MysqlUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;

public class DwdTradeOrderPreProcess {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //设置状态存储时间
        tableEnv.getConfig().setIdleStateRetention(Duration.ofDays(10));

         //读取kafka topic_db主题的数据(这一步是创建以kafka为源的)
          tableEnv.executeSql(KafkaUtils.getTopicDbDDL("dwd_trade_order_detail"));
        //这一步从topic_db中过滤出order_detail这个表的数据
          Table orderDetailTable = tableEnv.sqlQuery("select " +
                "data['id']           id, " +
                "data['order_id']     order_id, " +
                "data['sku_id']       sku_id,   " +
                "data['sku_name']     sku_name,  " +
                "data['img_url']      img_url,  " +
                "data['order_price']  order_price, " +
                "data['sku_num']      sku_num, " +
                "data['create_time']  create_time, " +
                "data['source_type']  source_type, " +
                "data['source_id']    source_id, " +
                "cast(cast(data['sku_num'] as decimal(16,2)) * cast(data['order_price'] as decimal(16,2)) as String) split_original_amount, " +
                "data['split_total_amount']    split_total_amount, " +
                "data['split_activity_amount'] split_activity_amount, " +
                "data['split_coupon_amount']   split_coupon_amount, " +
                 "ts od_ts,"+
                "pt  from topic_db where `database` ='gmall' and `table` = 'order_detail' and `type`='insert'");   //sqlQuery 查询出来的结果是个变量 不能直接在sql中使用
        tableEnv.createTemporaryView("order_detail",orderDetailTable);

            //过滤出订单表
        Table orderinfoTable = tableEnv.sqlQuery("select  " +
                "data['id']                     id, " +
                "data['consignee']              consignee, " +
                "data['consignee_tel']          consignee_tel, " +
                "data['total_amount']           total_amount, " +
                "data['order_status']           order_status, " +
                "data['user_id']                user_id, " +
                "data['payment_way']            payment_way, " +
                "data['out_trade_no']           out_trade_no, " +
                "data['trade_body']             trade_body, " +
                "data['operate_time']           operate_time, " +
                "data['expire_time']            expire_time, " +
                "data['process_status']         process_status, " +
                "data['tracking_no']            tracking_no, " +
                "data['parent_order_id']        parent_order_id, " +
                "data['province_id']            province_id, " +
                "data['activity_reduce_amount'] activity_reduce_amount, " +
                "data['coupon_reduce_amount']   coupon_reduce_amount, " +
                "data['original_total_amount']  original_total_amount, " +
                "data['feight_fee']             feight_fee, " +
                "data['feight_fee_reduce']      feight_fee_reduce, " +
                "ts oi_ts,"+
                "`type`,`old` " +
                "from topic_db where `database` = 'gmall' and `table` = 'order_info'  and(`type` = 'insert' or `type` = 'update')");
        tableEnv.createTemporaryView("order_info",orderinfoTable);

//        Table table = tableEnv.sqlQuery("select * from order_info");
//        tableEnv.toAppendStream(table,Row.class).print(">>>>>>>>>>>>>>>>");

        //过滤出订单活动表
        Table orderDetailActivityTable = tableEnv.sqlQuery("select   " +
                "data['id']                  id,  " +
                "data['order_id']            order_id,  " +
                "data['order_detail_id']     order_detail_id,  " +
                "data['activity_id']         activity_id,  " +
                "data['activity_rule_id']    activity_rule_id,  " +
                "data['sku_id']              sku_id,  " +
                "data['create_time']         create_time  " +
                "from topic_db where `database` = 'gmall' and `table` = 'order_detail_activity'  and(`type` = 'insert')");
         tableEnv.createTemporaryView("order_detail_activity",orderDetailActivityTable);
//        Table table = tableEnv.sqlQuery("select * from orderDetailActivity");
//        tableEnv.toAppendStream(table,Row.class).print(">>>>>>>>>>>>>>>>");
         //过滤出订单明细购物券数据
        Table order_detail_couponTable = tableEnv.sqlQuery("select    " +
                "data['id']                  id,   " +
                "data['order_id']            order_id,   " +
                "data['order_detail_id']     order_detail_id,   " +
                "data['coupon_id']           coupon_id,   " +
                "data['coupon_use_id']       coupon_use_id,   " +
                "data['sku_id']              sku_id,   " +
                "data['create_time']         create_time   " +
                "from topic_db where `database` = 'gmall' and `table` = 'order_detail_coupon'  and(`type` = 'insert')");
           tableEnv.createTemporaryView("order_detail_coupon",order_detail_couponTable);

//        Table table = tableEnv.sqlQuery("select * from  orderDetailCoupon");
//        tableEnv.toAppendStream(table,Row.class).print(">>>>>>>>>>>>>>>>");
        // 建立 MySQL-LookUp 字典表
        tableEnv.executeSql(MysqlUtils.getBaseDicLookUpDDL());

        //关联五张表
        Table resultTable = tableEnv.sqlQuery("select  " +
                "od.id, " +
                "od.order_id, " +
                "oi.user_id, " +
                "oi.order_status, " +
                "od.sku_id, " +
                "od.sku_name, " +
                "oi.province_id, " +
                "act.activity_id, " +
                "act.activity_rule_id, " +
                "cou.coupon_id, " +
                "date_format(od.create_time, 'yyyy-MM-dd') date_id, " +
                "od.create_time, " +
                "date_format(oi.operate_time, 'yyyy-MM-dd') operate_date_id, " +
                "oi.operate_time, " +
                "od.source_id, " +
                "od.source_type, " +
                "dic.dic_name source_type_name, " +
                "od.sku_num, " +
                "od.split_original_amount, " +
                "od.split_activity_amount, " +
                "od.split_coupon_amount, " +
                "od.split_total_amount, " +
                "oi.`type`, " +
                "oi.`old`, " +
                "od.od_ts, " +
                "oi.oi_ts, " +
                "current_row_timestamp() row_op_ts " +
                "from order_detail od  " +
                "join order_info oi on od.order_id = oi.id " +
                "left join order_detail_activity act on od.id = act.order_detail_id " +
                "left join order_detail_coupon cou on od.id = cou.order_detail_id " +
                "left join `base_dic` for system_time as of od.pt as dic on od.source_type = dic.dic_code");
        tableEnv.createTemporaryView("result_table", resultTable);
//        Table table = tableEnv.sqlQuery("select * from  result_table");
//        tableEnv.toChangelogStream(table).print(">>>>>>>>>>>>>>>>");


        //创建upsert-kafka的表
        tableEnv.executeSql("" +
                "create table dwd_trade_order_pre_process( " +
                "id string, " +
                "order_id string, " +
                "user_id string, " +
                "order_status string, " +
                "sku_id string, " +
                "sku_name string, " +
                "province_id string, " +
                "activity_id string, " +
                "activity_rule_id string, " +
                "coupon_id string, " +
                "date_id string, " +
                "create_time string, " +
                "operate_date_id string, " +
                "operate_time string, " +
                "source_id string, " +
                "source_type string, " +
                "source_type_name string, " +
                "sku_num string, " +
                "split_original_amount string, " +
                "split_activity_amount string, " +
                "split_coupon_amount string, " +
                "split_total_amount string, " +
                "`type` string, " +
                "`old` map<string,string>, " +
                "od_ts string, " +
                "oi_ts string, " +
                "row_op_ts timestamp_ltz(3), " +
                "primary key(id) not enforced " +
                ")" + KafkaUtils.getUpsertKafkaDDL("dwd_trade_order_pre_process"));


        //向upsert-kafka的表插入数据
        tableEnv.executeSql("insert into dwd_trade_order_pre_process select  * from result_table").print();;



        env.execute("DwdTradeOrderPreProcess");
    }
}
