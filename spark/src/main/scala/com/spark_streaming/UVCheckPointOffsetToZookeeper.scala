package com.spark_streaming

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @Description: 实时从kafka的topic中读取数据，并统计数据的次数，将数据checkpoint到hdfs上,手动维护offset到zookeeper中
  * @Author: chongweiLin  
  * @CreateDate: 2020-05-29 16:57
  **/
object UVCheckPointOffsetToZookeeper {
  def main(args: Array[String]): Unit = {
    // checkpointPath保存路径
    val checkpointPath = "hdfs://192.168.1.101:9000/spark/checkpoint/test/uv"

    def functionToCreateContext(): StreamingContext = {
      val ssc = new StreamingContext(new SparkConf().setAppName("ScalaKafkaStreamUvCheckpointOffsetToZookeeper").setMaster("local[*]").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer"), Seconds(3))
      ssc.checkpoint(checkpointPath)

      val bootstrapServers = "192.168.1.101:9092,192.168.1.102:9092,192.168.1.103:9092"
      val groupId = "kafka-test-group"
      val topicName = "test"
      val maxPoll = 20000

      val kafkaParams = Map(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers,
        ConsumerConfig.GROUP_ID_CONFIG -> groupId,
        ConsumerConfig.MAX_POLL_RECORDS_CONFIG -> maxPoll.toString,
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
      )

      //2.使用KafkaUtil连接Kafak获取数据
      val offsetMap: mutable.Map[TopicPartition, Long] = OffsetUtil.getOffsetMap("SparkKafkaOffset", "spark_kafka")


      ssc
    }
  }
}
