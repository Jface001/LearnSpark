package com.test.day06.streaming

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import java.sql.{Connection, DriverManager, PreparedStatement, Timestamp}


/**
 * @Author: Jface
 * @Desc: 学习SparkSteaming的窗口函数, 并在结果做排序, 并分别输出到 控制台 磁盘 MySQL数据库
 */
object S7SocketWordcountWindowSortForeachRDD {
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
    sortDStream.foreachRDD((rdd, time) => {

      val dateFormat: FastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
      val date_str: String = dateFormat.format(time.milliseconds)
      println("当前时间批次是:" + date_str)
      //1.打印到控制台
      val arr: Array[(String, Int)] = rdd.collect().sortBy(x => x._2).reverse
      arr.foreach(println(_))
      //2.保存到磁盘
      rdd.coalesce(1).saveAsTextFile(this.getClass.getSimpleName.stripSuffix("$") + "/" + time.milliseconds)
      //3.输出到MySQL
      rdd.foreachPartition(par => {
        var conn: Connection = null
        var stat: PreparedStatement = null
        try {
          conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/d_spark", "root", "root")
          stat = conn.prepareStatement("insert into t_hotwords values (?,?,?)")
          par.foreach(x => {
            stat.setTimestamp(1, new Timestamp(time.milliseconds))
            stat.setString(2, x._1)
            stat.setInt(3, x._2)
            stat.addBatch()
          })
          stat.executeBatch()
        } catch {
          case exception: Exception => exception.printStackTrace()
        } finally {
          if (stat != null) stat.close()
          if (conn != null) conn.close()

        }

      })

    })
    //4.启动流式应用
    ssc.start()
    //5.让应用一直处于监听状态
    ssc.awaitTermination()
    //6.合理关闭流式应用
    ssc.stop(true, true)
  }

}
