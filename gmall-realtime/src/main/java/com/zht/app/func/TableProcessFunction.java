package com.zht.app.func;



import com.alibaba.fastjson.JSON;
import com.zht.bean.TableProcess;
import com.zht.common.GmallConfig;
import net.minidev.json.JSONObject;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import javax.security.auth.login.Configuration;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class TableProcessFunction extends BroadcastProcessFunction<JSONObject,String,JSONObject> {

    private Connection connection;


    private void open(Configuration parameters) throws SQLException {
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    @Override
    public void processBroadcastElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {

         //获取并解析数据为javabean对象
        String after = JSON.parseObject(value).getString("after");
        TableProcess tableProcess = JSON.parseObject(after, TableProcess.class);
        //校验表是否存在
        checkTable(tableProcess.getSinkTable(),tableProcess.getSinkColumns(),tableProcess.getSinkPk(),tableProcess.getSinkExtend());
        //将数据写入状态


    }
        /*
        * 在Phoniex中校验并创建表
        * */
    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) throws SQLException {
        PreparedStatement preparedStatement =null;
        try {
            if(sinkPk ==null ||sinkPk.equals("")){
                sinkPk="id";
            }
            if(sinkExtend==null){
                sinkExtend="";
            }
            StringBuilder sql = new StringBuilder("create table if not exists").append(GmallConfig.HBASE_SCHEMA).append(".").append(sinkTable).append("(");
            String[] columns = sinkColumns.split(",");
            for (int i = 0; i < columns.length; i++) {
                String column = columns[i];
                //判断是否为主键
                if (!sinkPk.equals(column)) {
                    sql.append(column).append("varchar");
                } else {
                    sql.append(column).append(" varchar primary key");
                }

                //判断不是最后一个字段
                if(i<columns.length -1){
                    sql.append(",");
                }
            }
            sql.append(")").append(sinkExtend);
            System.out.println(sql);
              preparedStatement = connection.prepareStatement(sql.toString());
              preparedStatement.execute();

        } catch (SQLException e) {
          throw new RuntimeException("建表"+sinkTable+"失败");
        }finally {
            if(preparedStatement!=null){
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }

        }


    }

    @Override
    public void processElement(JSONObject value, BroadcastProcessFunction<JSONObject, String, JSONObject>.ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {

        //获取广播的配置数据

        //根据sinkcolums配置信息过滤字段

        //补充sinktable输出
    }


}
