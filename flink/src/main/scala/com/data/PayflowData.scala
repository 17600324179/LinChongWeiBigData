package com.data

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}

/**
  * @Description:
  * @Author: chongweiLin  
  * @CreateDate: 2020-06-12 11:16
  **/
object PayflowData {
  def main(args: Array[String]): Unit = {
    // 创建执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    // 从文件中读取数据
    val inputPath = "hdfs://192.168.1.101:9000/flink/data/t_kt_payflow_20200610-20200611.txt"
//    val inputPath = "hdfs://192.168.1.101:9000/flink/data/test.txt"
    val inputDS: DataSet[String] = env.readTextFile(inputPath)
    import org.apache.flink.api.scala._
    }
}
