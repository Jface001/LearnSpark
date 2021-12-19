package com.test.day10.streaming_task
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

/**
 * @author jface
 * @create 2021/12/19 17:49
 * @desc
 * 复习 sparkStraming 的 wordcount，掌握基础的算子
 */

object WordCountTest {
  def main(args: Array[String]): Unit = {
    //1.流式环境
    val conf = new SparkConf()
      .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
      .setMaster("local[*]")
    val ssc = SparkUtil.getSpark(conf)
    //2.数据源
    val inputDStream = ssc.socketTextStream("127.0.0.1", 9999)
    //3.算子
    val wordDStream = inputDStream
      .filter(x => x.isEmpty && null.equals(x))
      .flatMap(x => x.split(" "))
      .map(x => (x, 1))
      .reduceByKey(_ + _)
    //4.输出
    wordDStream.print()
    //5.启动与停止
    ssc.start()
    ssc.stop(true,true)
  }
}
