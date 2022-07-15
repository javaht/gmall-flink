package com.zht.app.dwd.db;

import com.zht.utils.KafkaUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
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
         tableEnv.createTemporaryView("orderActivity",orderDetailActivityTable);
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
           tableEnv.createTemporaryView("orderCoupon",order_detail_couponTable);

//        Table table = tableEnv.sqlQuery("select * from  orderDetailCoupon");
//        tableEnv.toAppendStream(table,Row.class).print(">>>>>>>>>>>>>>>>");
        //关联五张表


        tableEnv.sqlQuery("select  from   ");


        env.execute("DwdTradeOrderPreProcess");
    }
}
