package com.test.day06.streaming

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}


/**
 * @Author: Jface
 * @Date: 2021/8/21 12:03
 * @Desc: 学习SparkSteaming的窗口函数, 并在结果做排序
 */
object S6SocketWordcountWindowSort {
  def main(args: Array[String]): Unit = {
    //1.创建上下文对象, 指定批处理时间间隔为5秒
    val conf: SparkConf = new SparkConf()
      .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    //2. 创建一个接收文本数据流的流对象
    val ssc = new StreamingContext(sc, Seconds(5))
    val inputDStream: ReceiverInputDStream[String] = ssc.socketTextStream("node1", 9999)
    //3.打印结果
    val mapDStream: DStream[(String, Int)] = inputDStream.flatMap(_.split(" "))
      .map((_, 1))

    val wordDStream: DStream[(String, Int)] = mapDStream.reduceByKeyAndWindow((x: Int, y: Int) => x + y, Seconds(10), Seconds(5))
    val sortDStream: DStream[(String, Int)] = wordDStream.transform((rdd) => {
      rdd.sortBy(x => x._2, false)
    })
    sortDStream.print()
    //4.启动流式应用
    ssc.start()
    //5.让应用一直处于监听状态
    ssc.awaitTermination()
    //6.合理关闭流式应用
    ssc.stop(true, true)
  }

}
