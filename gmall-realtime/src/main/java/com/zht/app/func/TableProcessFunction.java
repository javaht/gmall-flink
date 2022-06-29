package com.zht.app.func;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.zht.bean.TableProcess;
import com.zht.common.GmallConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import javax.security.auth.login.Configuration;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

public class TableProcessFunction extends BroadcastProcessFunction<JSONObject,String,JSONObject> {

    private MapStateDescriptor<String,TableProcess> stateDescriptor;
    private Connection connection;

    public TableProcessFunction(MapStateDescriptor<String, TableProcess> stateDescriptor) {
        this.stateDescriptor = stateDescriptor;
    }


        private void open(Configuration parameters) throws SQLException {
            connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        }

        @Override
        public void processBroadcastElement(String value, BroadcastProcessFunction<JSONObject, String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
            //获取并解析数据为javabean对象
            String after = JSON.parseObject(value).getString("after");
            TableProcess tableProcess = JSON.parseObject(after, TableProcess.class);
            //校验表是否存在
            checkTable(tableProcess.getSinkTable(),tableProcess.getSinkColumns(),tableProcess.getSinkPk(),tableProcess.getSinkExtend());
            //将数据写入状态
            String key = tableProcess.getSourceTable();
            BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(stateDescriptor);
            broadcastState.put(key,tableProcess);

        }

        @Override
        public void processElement(JSONObject value, BroadcastProcessFunction<JSONObject, String, JSONObject>.ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
            //获取广播的配置数
            ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(stateDescriptor);
            TableProcess tableProcess = broadcastState.get(value.getString("table"));
            String type = value.getString("type");
            if(tableProcess!=null && (("bootstrap-insert").equals(type)||("insert").equals(type)||("update").equals(type))){
                //根据sinkcolums配置信息过滤字段
                filter(value.getJSONObject("data"),tableProcess.getSinkColumns());
                //补充sinktable输出
                value.put("sinkTable",tableProcess.getSinkTable());
                out.collect(value);
            }else{
                System.out.println("过滤掉"+value);
            }
        }

        /*
         * 过滤
         * */
        private  void filter(JSONObject data, String sinkColumns) {
            String[] split = sinkColumns.split(",");
            List<String> columnsList = Arrays.asList(split);
            data.entrySet().removeIf(next -> !columnsList.contains(next.getKey()));
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
    }


