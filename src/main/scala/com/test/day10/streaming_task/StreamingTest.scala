package com.test.day10.streaming_task

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import java.sql.DriverManager

/**
 * @author jface
 * @create 2021/12/26 20:15
 * @desc
 *
 */
object StreamingTest {
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
    var condition:String = null
    rdd.foreachPartition(par=>{
      val list=par.toList
      //TODO：如何将 多行 msg 拼接成查询条件字符串？就是把上面的"22,333,4444，11,1111,55555"
      condition = list.map(_.msg).mkString(",")
    })
    println(condition)

  })

  //启动与优雅停止
  ssc.start()
  ssc.awaitTermination()
  ssc.stop(true,true)
}
  }
