package com.zht.app.func;

import com.zht.bean.TransientSink;
import com.zht.common.GmallConfig;
import lombok.SneakyThrows;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MyClickHouseUtil {
 //这里的T其实就是一个javabean
    public static <T> SinkFunction<T> getClickHouseSink(String sql) {

        return JdbcSink.<T>sink(sql,
                new JdbcStatementBuilder<T>() {
                    @SneakyThrows
                    @Override
                    public void accept(PreparedStatement preparedStatement, T t) throws SQLException {

                        //使用反射的方式提取字段
                        Class<?> clz = t.getClass();

                        //Method[] methods = clz.getMethods();
                        //for (Method method : methods) {
                        //    method.invoke(t);
                        //}

                        Field[] fields = clz.getDeclaredFields();
                        //getFields() 获得某个类的所有的公共（public）的字段，包括父类。
                        //getDeclaredFields() 获得某个类的所有申明的字段，即包括 public、private 和 proteced,但是不包括父类的申明字段。

                        int offset = 0;
                        for (int i = 0; i < fields.length; i++) {

                            Field field = fields[i];
                            field.setAccessible(true);//取消java语言访问检查

                            TransientSink transientSink = field.getAnnotation(TransientSink.class);//获取field的注解类型
                            if (transientSink != null) {
                                offset++;
                                continue;
                            }

                            //获取数据并给占位符赋值
                            Object value = field.get(t);
                            preparedStatement.setObject(i + 1 - offset, value);
                        }
                    }
                }, new JdbcExecutionOptions.Builder()
                        .withBatchSize(5)
                        .withBatchIntervalMs(1000L)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName(GmallConfig.CLICKHOUSE_DRIVER)
                        .withUrl(GmallConfig.CLICKHOUSE_URL)
                        .build());
    }

}
