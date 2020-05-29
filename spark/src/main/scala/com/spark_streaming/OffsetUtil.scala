package com.spark_streaming

import java.sql.{DriverManager, ResultSet}

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange

import scala.collection.mutable

/**
  * @https: https://blog.csdn.net/weixin_45399233/article/details/100538439
  *  @Description:
  * @Author: chongweiLin  
  * @CreateDate: 2020-05-29 17:20
  **/
/*
 手动维护offset的工具类,首先在MySQL创建如下表
   CREATE TABLE `t_offset` (
     `topic` varchar(255) NOT NULL,
     `partition` int(11) NOT NULL,
     `groupid` varchar(255) NOT NULL,
     `offset` bigint(20) DEFAULT NULL,
     PRIMARY KEY (`topic`,`partition`,`groupid`)
   ) DEFAULT CHARSET=utf8;
  */
object OffsetUtil {

  val ip_port = "192.168.1.101:3306"
  val user_name = "root"
  val password = "abc123"

  /**
    * 从数据库读取偏移量
    */
  def getOffsetMap(groupid: String, topic: String) = {
    val connection = DriverManager.getConnection("jdbc:mysql://" + ip_port + "/spark?characterEncoding=UTF-8", user_name, password)
    val pstmt = connection.prepareStatement("select * from t_offset where groupid=? and topic=?")
    pstmt.setString(1, groupid)
    pstmt.setString(2, topic)
    val rs: ResultSet = pstmt.executeQuery()
    val offsetMap = mutable.Map[TopicPartition, Long]()
    while (rs.next()) {
      offsetMap += new TopicPartition(rs.getString("topic"), rs.getInt("partition")) -> rs.getLong("offset")
    }
    rs.close()
    pstmt.close()
    connection.close()
    offsetMap
  }

  /**
    * 将偏移量保存到数据库
    */
  def saveOffsetRanges(groupid: String, offsetRange: Array[OffsetRange]) = {
    val connection = DriverManager.getConnection("jdbc:mysql://" + ip_port + "/spark?characterEncoding=UTF-8", user_name, password)
    //replace into表示之前有就替换,没有就插入
    val pstmt = connection.prepareStatement("replace into t_offset (`topic`, `partition`, `groupid`, `offset`) values(?,?,?,?)")
    for (o <- offsetRange) {
      pstmt.setString(1, o.topic)
      pstmt.setInt(2, o.partition)
      pstmt.setString(3, groupid)
      pstmt.setLong(4, o.untilOffset)
      pstmt.executeUpdate()
    }
    pstmt.close()
    connection.close()
  }
}
