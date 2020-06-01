package com.spark_streaming.checkpoint

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}


/**
  * @Description: spark streaming 消费kafka的数据，统计uv，数据checkpoint到hdfs上
  * @Author: chongweiLin  
  * @CreateDate: 2020-05-27 14:59
  **/
object UvCheckpoint {
  def main(args: Array[String]): Unit = {
    // checkpointPath保存路径
    val checkpointPath = "hdfs://192.168.1.101:9000/spark/checkpoint/test/uv"

    def functionToCreateContext(): StreamingContext = {
      val ssc = new StreamingContext(new SparkConf().setAppName("ScalaKafkaStreamUvCheckpoint").setMaster("local[*]").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer"), Seconds(3))
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

      val kafkaTopicDS = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](Set(topicName), kafkaParams))
      // 定义更新状态方法，参数values为当前批次单词频度，state为以往批次单词频度
      val updateFunc = (values: Seq[Int], state: Option[Int]) => {
        val currentCount = values.foldLeft(0)(_ + _)
        val previousCount = state.getOrElse(0)
        Some(currentCount + previousCount)
      }
      val reduceByKeyRdd = kafkaTopicDS.map {
        (value) =>
          val key_num = value.value().split("\\|\\|").toArray
          (key_num.apply(0), Integer.parseInt(key_num.apply(1)))
      }

      val reduceByKey = reduceByKeyRdd.reduceByKey {
        (value1, value2) =>
          value1 + value2
      }


      val updateStateByKeyRdd = reduceByKey.updateStateByKey(updateFunc)


      updateStateByKeyRdd.cache()
      updateStateByKeyRdd.checkpoint(Duration.apply(9000))

      updateStateByKeyRdd.print()
      //    }.reduceByKey{
      //      (value1,value2)=>
      //        (value1.toInt+value2.toInt).toString
      //    }
      //    reduceByKeyRdd.mapPartitions{
      //      (key,value) =>
      //        Integer.parseInt(key)
      //    }
      //    reduceByKeyRdd.updateStateByKey(updateFunc)
      ssc
    }

    val context = StreamingContext.getOrCreate(checkpointPath, functionToCreateContext _)
    context.start()
    context.awaitTermination()
    //    ssc.start()
    //    ssc.awaitTermination()


  }


}
