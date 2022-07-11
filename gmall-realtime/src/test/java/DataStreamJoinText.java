
/*
 * @Author root
 * @Data  2022/7/11 10:42
 * @Description
 * */


import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class DataStreamJoinText {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<bean1> bean1Ds = env.socketTextStream("hadoop102", 8888).map(
                line -> {
                    String[] split = line.split(",");
                    return new bean1(split[0], split[1], Long.parseLong(split[2]));
                }).assignTimestampsAndWatermarks(WatermarkStrategy.<bean1>forMonotonousTimestamps()
                .withTimestampAssigner((SerializableTimestampAssigner<bean1>) (element, recordTimestamp) -> element.getTs() * 1000L));


        SingleOutputStreamOperator<bean2> bean2Ds = env.socketTextStream("hadoop102", 9999).map(
                line -> {
                    String[] split = line.split(",");
                    return new bean2(split[0], split[1], Long.parseLong(split[2]));
                }).assignTimestampsAndWatermarks(WatermarkStrategy.<bean2>forMonotonousTimestamps()
                .withTimestampAssigner((SerializableTimestampAssigner<bean2>) (element, recordTimestamp) ->  element.getTs() * 1000L));

        //双流join

        SingleOutputStreamOperator<Tuple2<bean1, bean2>> result = bean1Ds.keyBy(line -> line.getId())
                .intervalJoin(bean2Ds.keyBy(line -> line.getId()))
                .between(Time.seconds(-5), Time.seconds(5))
                //.lowerBoundExclusive()   左开右闭    .upperBoundExclusive() 左闭右开  都不加就是左闭右闭
                .process(new ProcessJoinFunction<bean1, bean2, Tuple2<bean1, bean2>>() {
                    @Override
                    public void processElement(bean1 left, bean2 right, ProcessJoinFunction<bean1, bean2, Tuple2<bean1, bean2>>.Context ctx, Collector<Tuple2<bean1, bean2>> out) throws Exception {

                        out.collect(new Tuple2<>(left, right));
                    }
                });

         //打印结果并启动
        result.print(">>>>>>>>>>>>>>>>>>");
        env.execute("DataStreamJoinText");
    }
}
