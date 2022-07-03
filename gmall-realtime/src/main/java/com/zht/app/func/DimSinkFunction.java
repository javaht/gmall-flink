package com.zht.app.func;

import com.alibaba.fastjson.JSONObject;
import com.zht.common.GmallConfig;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

public class DimSinkFunction extends RichSinkFunction<JSONObject> implements SinkFunction<com.alibaba.fastjson.JSONObject> {

    private Connection  connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }


    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        PreparedStatement preparedStatement = null;
        try {
            //拼接sql
            String upsertSql = genUpsertSql(value.getString("sinkTable"), value.getJSONObject("data"));
            System.out.println(upsertSql);
            //预编译sql
            preparedStatement = connection.prepareStatement(upsertSql);
            //执行写入操作
            preparedStatement.execute();
            connection.setAutoCommit(true);
            connection.commit();
        } catch (SQLException e) {
            System.out.println("插入数据失败！");
        } finally {
            //释放资源
            if(preparedStatement!=null){
                preparedStatement.close();
            }
        }
    }

    /*
    *
    * */
    private String genUpsertSql(String sinkTable, JSONObject data) {
        Set<String> columns = data.keySet();
        Collection<Object> values = data.values();


        return "upsert into  " +GmallConfig.HBASE_SCHEMA+"."+sinkTable+"("+
                StringUtils.join(columns,",")+") values('"+
                StringUtils.join(values,"','")+"')";
    }

}
