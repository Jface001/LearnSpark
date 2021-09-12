package com.test.day06.streaming

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}


/**
 * @Author: Jface
 * @Date: 2021/8/21 12:03
 * @Desc: 通过UpdateStateByKey 实现宕机后状态恢复
 */


object S4ocketWordcountUpdateStateByKeyRecovery {
    //设置路径
    val CKP ="src/main/data/ckp/"+this.getClass.getSimpleName.stripSuffix("$")
    //1.创建上下文对象, 指定批处理时间间隔为5秒
    val creatingFunc =()=>
    {
      val conf: SparkConf = new SparkConf()
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
        .setMaster("local[*]")
      val sc = new SparkContext(conf)
      //2. 创建一个接收文本数据流的流对象
      val ssc = new StreamingContext(sc, Seconds(5))


      //3.设置checkpoint位置
      ssc.checkpoint(CKP)
      //4.接收socket数据
      val inputDStream: ReceiverInputDStream[String] = ssc.socketTextStream("node1", 9999)
      //TODO: 5.wordcount, 并做累计统计
      //自定义一个函数, 实现保存State状态和数据聚合
      //seq里面是value的数组,[1,1,], state是上次的状态, 累计值
      val updateFunc = (seq: Seq[Int], state: Option[Int]) => {
        if (!seq.isEmpty) {
          val this_value: Int = seq.sum
          val last_value: Int = state.getOrElse(0)
          val new_state: Int = this_value + last_value
          Some(new_state)
        }
        else {
          state
        }
      }
      //开始做wordcount,并打印输出
      val wordDStream: DStream[(String, Int)] = inputDStream.flatMap(_.split(" "))
        .map((_, 1))
        .updateStateByKey(updateFunc)
      wordDStream.print()
    ssc
    }

  def main(args: Array[String]): Unit = {
    val ssc: StreamingContext = StreamingContext.getOrCreate(CKP, creatingFunc)

    //启动流式应用
    ssc.start()
    //让应用一直处于监听状态
    ssc.awaitTermination()
    //合理关闭流式应用
    ssc.stop(true, true)

  }



}
