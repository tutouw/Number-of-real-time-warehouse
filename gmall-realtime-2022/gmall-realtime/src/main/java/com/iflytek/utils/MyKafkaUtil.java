package com.iflytek.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Properties;

/**
 * @author Aaron
 * @date 2022/6/18 23:00
 */

public class MyKafkaUtil {
    public static FlinkKafkaConsumer<String> getKafkaConsumer(String topic, String groupId) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        return new FlinkKafkaConsumer<String>(
                topic,
                new KafkaDeserializationSchema<String>() {

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return BasicTypeInfo.STRING_TYPE_INFO;
                    }

                    @Override
                    public boolean isEndOfStream(String s) {
                        return false;
                    }

                    @Override
                    public String deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
                        byte[] value = consumerRecord.value();
                        if (consumerRecord == null || consumerRecord.value() == null){
                            return "";
                        }else {
                            return new String(consumerRecord.value());
                        }
                    }
                },
                properties);
    }
}
