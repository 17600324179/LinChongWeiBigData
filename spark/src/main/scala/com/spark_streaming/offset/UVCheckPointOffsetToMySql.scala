package com.spark_streaming.offset

import OffsetUtil
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

import scala.collection.mutable

/**
  *  1.通过mysql中维护的偏移量创建DStream流(如果mysql中有值则通过存储的offset创建数据流，同时通过checkpoint的数据恢复历史统计结果)
  *  2.DStream流创建后通过updatStateByKey计算 (key,count)
  *  3.将数据进行checkpoint
  *  4.使用foreachRDD将消费的groupId、offset保存到mysql中
  */
/**
  * @Description: 实时从kafka的topic中读取数据，并统计数据的次数，将数据checkpoint到hdfs上,手动维护offset到mysql中
  * @Author: chongweiLin  
  * @CreateDate: 2020-05-29 16:57
  **/
object UVCheckPointOffsetToMySql {
  def main(args: Array[String]): Unit = {
    // checkpointPath保存路径
    val checkpointPath = "hdfs://192.168.1.101:9000/spark/checkpoint/test/uv-offset"

    def functionToCreateContext(): StreamingContext = {
      val ssc = new StreamingContext(new SparkConf().setAppName("UVCheckPointOffsetToMySql").setMaster("local[*]").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer"), Seconds(3))
      ssc.checkpoint(checkpointPath)

      val bootstrapServers = "192.168.1.101:9092,192.168.1.102:9092,192.168.1.103:9092"
      val groupId = "ScalaKafkaStreamUVCheckPointOffsetToMySql"
      val topics = Array("test")
      val maxPoll = 20000

      val kafkaParams = Map(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers,
        ConsumerConfig.GROUP_ID_CONFIG -> groupId,
        ConsumerConfig.MAX_POLL_RECORDS_CONFIG -> maxPoll.toString,
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
      )

      //2.使用KafkaUtil连接Kafak获取数据 , getOffsetMap(groupId,Topic)
      val offsetMap: mutable.Map[TopicPartition, Long] = OffsetUtil.getOffsetMap(groupId, topics.apply(0))
      val recordDStream: InputDStream[ConsumerRecord[String, String]] = if (offsetMap.size > 0) {
        //有记录offset，从该offset处开始消费
        KafkaUtils.createDirectStream[String, String](ssc,
          LocationStrategies.PreferConsistent, //位置策略：该策略,会让Spark的Executor和Kafka的Broker均匀对应
          ConsumerStrategies.Subscribe[String, String](topics, kafkaParams, offsetMap)) //消费策略
      } else {
        //MySQL中没有记录offset,则直接连接,从latest开始消费
        //直接创建一个sparkStreaming对象
        KafkaUtils.createDirectStream[String, String](ssc,
          LocationStrategies.PreferConsistent,
          ConsumerStrategies.Subscribe[String, String](topics, kafkaParams))
      }

      //3.操作数据
      // 定义更新状态方法，参数values为当前批次单词频度，state为以往批次单词频度
      val updateFunc = (values: Seq[Int], state: Option[Int]) => {
        val currentCount = values.foldLeft(0)(_ + _)
        val previousCount = state.getOrElse(0)
        Some(currentCount + previousCount)
      }
      val reduceByKeyRdd = recordDStream.map {
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

      // 4.存储offset
      //注意:我们要自己手动维护偏移量,也就意味着,消费了一小批数据就应该提交一次offset
      //而这一小批数据在DStream的表现形式就是RDD,所以我们需要对DStream中的RDD进行操作
      //而对DStream中的RDD进行操作的API有transform(转换)和foreachRDD(动作)
      updateStateByKeyRdd.foreachRDD(rdd => {
        //当前这一时间批次有数据
        if (rdd.count() > 0) {

          //接收到的Kafk发送过来的数据为:ConsumerRecord(topic = spark_kafka, partition = 1, offset = 6, CreateTime = 1565400670211, checksum = 1551891492, serialized key size = -1, serialized value size = 43, key = null, value = hadoop spark ...)
          //注意:通过打印接收到的消息可以看到,里面有我们需要维护的offset,和要处理的数据
          //接下来可以对数据进行处理....或者使用transform返回和之前一样处理
          //维护offset：为了方便我们对offset的维护/管理,spark提供了一个类,帮我们封装offset的数据
          val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

          // 打印offset
          //          for (o <- offsetRanges) {
          //            println(s"topic=${o.topic},partition=${o.partition},fromOffset=${o.fromOffset},untilOffset=${o.untilOffset}")
          //          }


          //手动提交offset,默认提交到Checkpoint中
          //recordDStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
          OffsetUtil.saveOffsetRanges(groupId, offsetRanges)
        }
      })
      ssc
    }

    val context = StreamingContext.getOrCreate(checkpointPath, functionToCreateContext _)
    context.start()
    context.awaitTermination()
  }
}
