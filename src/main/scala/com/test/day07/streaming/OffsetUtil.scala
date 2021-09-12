package com.test.day07.streaming

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import scala.collection.mutable.Map

import java.sql.{DriverManager, ResultSet}


/**
 * @Author: Jface
 * @Date: 2021/8/22 12:32
 * @Desc: 定义一个单例对象, 定义 2 个方法
 *        方法1: 从 MySQL 读取行偏移量
 *        方法2: 将行偏移量保存的 MySQL
 */
object OffsetUtil {

  /**
   * 定义一个单例方法, 将偏移量保存到MySQL数据库
   *
   * @param groupid     消费者组id
   * @param offsetRange 行偏移量对象
   */
  def saveOffsetRanges(groupid: String, offsetRange: Array[OffsetRange]) = {
    val connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/d_spark",
      "root",
      "root")
    //replace into表示之前有就替换,没有就插入
    val ps = connection.prepareStatement("replace into t_offset (`topic`, `partition`, `groupid`, `offset`) values(?,?,?,?)")
    for (o <- offsetRange) {
      ps.setString(1, o.topic)
      ps.setInt(2, o.partition)
      ps.setString(3, groupid)
      ps.setLong(4, o.untilOffset)
      ps.executeUpdate()
    }
    ps.close()
    connection.close()
  }

  /**
   * 定义一个方法, 用于从 MySQL 中读取行偏移位置
   *
   * @param groupid 消费者组id
   * @param topic   想要消费的数据主题
   */
  def getOffsetMap(groupid: String, topic: String) = {

    //1.从数据库查询对应数据
    val connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/d_spark",
      "root",
      "root")

    val ps = connection.prepareStatement("select * from t_offset where groupid=?  and topic=?")
    ps.setString(1, groupid)
    ps.setString(2, topic)
    val rs: ResultSet = ps.executeQuery()
    //解析数据, 返回
    var offsetMap = Map[TopicPartition, Long]()
    while (rs.next()) {
      val topicPartition = new TopicPartition(rs.getString("topic"), rs.getInt("partition"))

      offsetMap.put(topicPartition, (rs.getLong("offset")))
    }
    rs.close()
    rs.close()
    connection.close()
    offsetMap
  }


}
