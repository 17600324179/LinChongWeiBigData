package com.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class MyProducer {
    private static ArrayList<String> arrayList = new ArrayList<String>(){{
        add("hive");
        add("spark");
        add("hdfs");
        add("yarn");
        add("zookeeper");
        add("hadoop");
        add("flink");
        add("flume");
        add("kafka");
        add("hbase");
        add("phoenix");
        add("java");
        add("scala");
    }};

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties kafkaProperties = new Properties();

        //kafka集群，broker-list
        kafkaProperties.put("bootstrap.servers", "192.168.1.3:9092,192.168.1.4:9092,192.168.1.5:9092");
        kafkaProperties.put("acks", "all");
        //重试次数
        kafkaProperties.put("retries", 1);
        //发送数据批次大小
        kafkaProperties.put("batch.size", 16384);
        //发送数据等待时间
        kafkaProperties.put("linger.ms", 1);
        //RecordAccumulator缓冲区大小
        kafkaProperties.put("buffer.memory", 33554432);
        kafkaProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // 创建生产者对象
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(kafkaProperties);

        String TOPIC = "test";
        /**
         * 异步发送Api: kafkaProducer.send(new ProducerRecord<String, String>(TOPIC, "timeStamp:\t" + System.currentTimeMillis()));
         * 同步发送Api: kafkaProducer.send(new ProducerRecord<String, String>(TOPIC, "timeStamp:\t" + System.currentTimeMillis())).get();
         */
        for (int i = 0; i < 5; i++) {
//            kafkaProducer.send(new ProducerRecord<String, String>(TOPIC, "timeStamp:\t" + System.currentTimeMillis())).get();
            String value = getKafkaValue();
            kafkaProducer.send(new ProducerRecord<String, String>(TOPIC, value)).get();
            System.out.println(value);

        }

        System.out.println("开始关闭kafka producer");
        kafkaProducer.close();
        System.out.println("已关闭kafka producer");
    }

    private static String getKafkaValue(){
        return arrayList.get((int) (Math.random()*arrayList.size())) + "||"+(int) (Math.random()*10);
    }
}
