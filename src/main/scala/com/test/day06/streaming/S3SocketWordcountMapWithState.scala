package com.test.day06.streaming

import org.apache.spark.streaming.dstream.{DStream, MapWithStateDStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext, Time}
import org.apache.spark.{SparkConf, SparkContext}


/**
 * @Author: Jface
 * @Desc: 通过 WordcountMapWithState 实现只输出有变动的内容
 */
object S3SocketWordcountMapWithState {
  def main(args: Array[String]): Unit = {
    //1.创建上下文对象, 指定批处理时间间隔为5秒
    val conf: SparkConf = new SparkConf()
      .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    //2. 创建一个接收文本数据流的流对象
    val ssc = new StreamingContext(sc, Seconds(5))
    //3.设置checkpoint位置
    ssc.checkpoint(this.getClass.getSimpleName.stripSuffix("$"))
    //4.接收socket数据
    val inputDStream: ReceiverInputDStream[String] = ssc.socketTextStream("node1", 9999)
    //TODO: 5.wordcount, 并做累计统计
    //自定义一个函数, 实现状态更新
    //是一个元组, 一个元组地处理.
    val mappingFunction = (key: String, value: Option[Int], state: State[Int]) => {
      val this_value: Int = value.getOrElse(0)
      val last_value: Int = state.getOption().getOrElse(0)
      val new_value: Int = this_value + last_value
      state.update(new_value)
      (key, new_value) // 返回一个元组, 最后结果也是元组
    }
    val spec: StateSpec[String, Int, Int, (String, Int)] = StateSpec.function(mappingFunction)


    //开始做wordcount,并打印输出
    val mapDStream: DStream[(String, Int)] = inputDStream.flatMap(_.split(" "))
      .map((_, 1))
    val wordDStream: MapWithStateDStream[String, Int, Int, (String, Int)] = mapDStream.mapWithState(spec)
    wordDStream.print()

    //启动流式应用
    ssc.start()
    //让应用一直处于监听状态
    ssc.awaitTermination()
    //合理关闭流式应用
    ssc.stop(true, true)
  }

}
