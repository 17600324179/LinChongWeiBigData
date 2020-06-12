package com.helloworld

import org.apache.flink.api.scala.{AggregateDataSet, DataSet, ExecutionEnvironment}


/**
  * @Description:
  * @Author: chongweiLin  
  * @CreateDate: 2020-06-08 11:15
  **/
object FlinkWordCount {
  def main(args: Array[String]): Unit = {
    // 创建执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    // 从文件中读取数据
    val inputPath = "hdfs://192.168.1.101:9000/flink/data/pt_play_water_20200608_1591607894795.txt"
    val inputDS: DataSet[String] = env.readTextFile(inputPath)
    import org.apache.flink.api.scala._
    // 分词之后，对单词进行groupby分组，然后用sum进行聚合
    val wordCountDS: AggregateDataSet[(String, Int)] = inputDS.flatMap(_.split(" ")).map((_, 1)).groupBy(0).sum(1)

    val count = wordCountDS.count()
    System.out.println(count)
    // 打印输出
  }
}
