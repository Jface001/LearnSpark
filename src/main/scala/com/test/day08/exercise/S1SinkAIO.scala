package com.test.day08.exercise

import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @Author: Jface
 * @Desc: 测试学习 StruturedStreaming 的 sink 特性
 */
object S1SinkAIO {
  def main(args: Array[String]): Unit = {
    //1.创建上下文对象
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", 4)
      .getOrCreate()

    //2.读取 socket 数据源, 获取流式DataFrame
    val inputDF: DataFrame = spark.readStream
      .format("socket")
      .option("host", "node1")
      .option("port", 9999)
      .load()
    //3.wordcount , 用DSL风格
    import spark.implicits._
    val resulttable: DataFrame = inputDF.as[String]
      .flatMap(_.split(" "))
      .groupBy("value") //聚合, 默认列叫value
      .count() //计数求和, 默认会生产叫count的列

    //4.结果输入到控制台, 设置输出模式, 查询名称, 触发间隔, 检查点位置, memory sink
    val query: StreamingQuery = resulttable.writeStream
      .format("console") //输出目标
      .outputMode(OutputMode.Complete()) //输出模式
      .queryName("t_wordcount") //查询名称
      //.trigger(Trigger.ProcessingTime("1 second")) //触发间隔
      .option("checkpointLocation", this.getClass.getSimpleName.stripSuffix("$")) //检查点位置
      .start() //启动流式查询

    while (true) {
      println("打印: ")
      spark.sql(
        """
          |select *
          |from t_wordcount
          |""".stripMargin).show()
      Thread.sleep(1000)
    }

    //5.启动停留 关闭
    query.awaitTermination()
    query.stop()

  }


}
