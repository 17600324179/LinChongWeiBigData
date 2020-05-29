package com.producer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * @Description: Kafka消费者 自动提交offset
 * @Author: chongweiLin
 * @CreateDate: 2019-12-10 11:23
 **/
public class MyConsumer {
    public static void main(String[] args) {
        Properties consumerProperties = new Properties();
        // 1. bootstrap.servers
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.101:9092,192.168.1.102:9092,192.168.1.103:9092");
        // 2. group.id
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "test0099");
        // 3. enable.auto.commit
        consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        // 4. auto.commit.interval.ms
        consumerProperties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        // 5. key.deserializer
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        // 6.value.deserializer
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        // 创建KafkaConsumer对象
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(consumerProperties);
        kafkaConsumer.subscribe(Arrays.asList("test"));
        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
            }
        }
    }
}
