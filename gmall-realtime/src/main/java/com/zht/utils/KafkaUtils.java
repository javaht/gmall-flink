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

}
