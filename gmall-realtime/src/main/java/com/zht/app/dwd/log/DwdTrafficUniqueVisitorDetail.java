package com.zht.app.dwd.log;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DwdTrafficUniqueVisitorDetail {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =  StreamExecutionEnvironment.getExecutionEnvironment();

            //读取kafka  dwd_traffic_page_log 主题数据创建流

          //将每行数据转换为json对象

         //过滤掉上一跳页面id不等于null的数据

        //按照Mid分组

        //使用状态编程进行每日登陆数据去重

        //将数据写出到kafka


        env.execute("");
    }
}
