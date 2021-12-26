package com.test.day10.streaming_task

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Second

import java.sql.DriverManager

/**
 * @author jface
 * @create 2021/12/23 23:15
 * @desc 模拟使用 SparkStreaming
 *
 */
object ScoreStreaming {
  def main(args: Array[String]): Unit = {
    //1.spark Streaming 环境准备
    val conf = new SparkConf()
      .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
      .setMaster("local[*]")
    val ssc = SparkUtil.getSpark(conf)
    //2.数据源
    //TODO：rocketMQ 如何接收消息？？
    val inputDStream = ssc.socketTextStream("192.168.1.102", 9999)
    //2.获取字符串，格式化并提取查询条件，msg 里面就是多个 product_id，是主键
    // {"type": "stock_change", "msg": "22,333,4444"}
    // {"type": "stock_change", "msg": "11,1111,55555"}
    inputDStream.filter(x=>x!=null && x.length>0)
      .map(record=>HandleMessage.handleMessage2CaseClass(record)).foreachRDD(rdd=>{
      val spark = SparkSession.builder().config(rdd.sparkContext.getConf).getOrCreate()
      val df = spark.createDataFrame(rdd)
      df.show()
      //TODO：如何将 多行 msg 拼接成查询条件字符串？就是把上面的"22,333,4444，11,1111,55555"
      val condition =null
      //TODO:根据查询条件 JDBC 查询 SQLserver 数据库，获取 stock_number 和 send_number
      val conn = DriverManager.getConnection("192.168.1.102", "root", "123456")
      val sql = "select product_id,stock_number,send_number from sz_stock where  product_id in (" + condition + ")"
      val statement = conn.prepareStatement(sql)
      val resultSet = statement.executeQuery()
      while(resultSet.next()){
        val product_id = resultSet.getInt(1)
        val stock_number = resultSet.getInt(2)
        val send_number = resultSet.getInt(3)
      }
      //TODO: 根据查询条件 JDBC 查询 Impala 获取 real_number
      //TODO: 根据 product_id,计算 stock_number - send_number + real_number ，获取 total_number
      //TODO: 将 product_id 和 total_number 写入 MySQL 数据库
      //TODO：发送原先的字符串给到 rocketMQ
    })


    //启动与优雅停止
    ssc.start()
    ssc.awaitTermination()
    ssc.stop(true,true)
  }
}
