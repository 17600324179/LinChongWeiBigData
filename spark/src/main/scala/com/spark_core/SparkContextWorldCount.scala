package com.spark_core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Description:
  * @Author: chongweiLin  
  * @CreateDate: 2020-06-02 15:11
  **/
object SparkContextWorldCount {
  def main(args: Array[String]) {
    val inputFile =  "file:///usr/local/spark/mycode/wordcount/word.txt"
    val conf = new SparkConf().setAppName("WordCount")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile(inputFile)
    val wordCount = textFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((a, b) => a + b)
    wordCount.foreach(println)
  }
}
