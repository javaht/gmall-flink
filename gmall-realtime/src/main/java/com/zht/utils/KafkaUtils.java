package com.zht.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;


import java.util.Properties;

public class KafkaUtils {
    static String BOOTSTRAP_SERVERS = "hadoop102:9092";
    static String DEFAULT_TOPIC = "default_topic";

    private static  Properties properties =new Properties();
    static{
        properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
    }

    public static FlinkKafkaConsumer<String> getKafkaConsumer(String topic, String groupId) {
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(topic, new KafkaDeserializationSchema<String>() {
            @Override
            public boolean isEndOfStream(String s) {
                return false;
            }

            @Override
            public String deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
                if(record==null && record.value() == null){
                    return "";
                }else{
                    return new String(record.value());
                }
            }
            @Override
            public TypeInformation<String> getProducedType() {
                return BasicTypeInfo.of(String.class);
            }
        },properties);
        return  consumer;
    }
    public static FlinkKafkaProducer<String> getKafkaProducer(String topic){

        return new FlinkKafkaProducer<String>(topic,new SimpleStringSchema(),properties);
    }
    /**
     * Kafka-Source DDL 语句
     * @param topic   数据源主题
     * @param groupId 消费者组
     * @return 拼接好的 Kafka 数据源 DDL 语句
     */
    public static String getKafkaDDL(String topic, String groupId) {

        return " with ('connector' = 'kafka', " +
                " 'topic' = '" + topic + "'," +
                " 'properties.bootstrap.servers' = '"+BOOTSTRAP_SERVERS+"', " +
                " 'properties.group.id' = '" + groupId + "', " +
                " 'format' = 'json', " +
                " 'scan.startup.mode' = 'group-offsets')";
    }

    /**
     * Kafka-Sink DDL 语句
     *
     * @param topic 输出到 Kafka 的目标主题
     * @return 拼接好的 Kafka-Sink DDL 语句
     */
    public static String getUpsertKafkaDDL(String topic) {

        return "WITH ( " +
                "  'connector' = 'upsert-kafka', " +
                "  'topic' = '" + topic + "', " +
                "  'properties.bootstrap.servers' = '" + BOOTSTRAP_SERVERS + "', " +
                "  'key.format' = 'json', " +
                "  'value.format' = 'json' " +
                ")";
    }
    /*
    * 获取消费者组的数据
    * */

    public  static String getTopicDbDDL(String groupid){
       return "create  table topic_db( " +
                "`database` String, " +
                "`table` String, " +
                "`type`  String, " +
                " `ts`  String,  "+
                "`data` Map<String,String>, " +
                "`old`  Map<String,String>, " +
                "`pt` AS PROCTIME() " +
                ")"+KafkaUtils.getKafkaDDL("topic_db",groupid);
    }


    }
