
/*
 * @Author root
 * @Data  2022/7/11 15:16
 * @Description
 * */

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;


public class FlinkSqlJoinTest {
    public static void main(String[] args) throws Exception {

        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        System.out.println(tableEnv.getConfig().getIdleStateRetention());//PT 0  S
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10)); //状态保存时间

        SingleOutputStreamOperator<bean1> bean1Ds = env.socketTextStream("192.168.20.62", 8888).map(
                line -> {
                    String[] split = line.split(",");
                    return new bean1(split[0], split[1], Long.parseLong(split[2]));
                });


        SingleOutputStreamOperator<bean2> bean2Ds = env.socketTextStream("192.168.20.62", 9999).map(
                line -> {
                    String[] split = line.split(",");
                    return new bean2(split[0], split[1], Long.parseLong(split[2]));
                });

              tableEnv.createTemporaryView("t1",bean1Ds);
              tableEnv.createTemporaryView("t2",bean2Ds);

            //内连接  左表： OnCreateAndWrite  右表：OnCreateAndWrite
        tableEnv.sqlQuery("select t1.id,t1.name,t2.sex from t1 join t2 on t1.id = t2.id").execute().print();

    }
}
