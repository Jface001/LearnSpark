package com.test.day08.exercise

import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}

/**
 * @Author: Jface
 * @Date: 2021/8/25 12:25
 * @Desc: 测试学习 StruturedStreaming 的 sink 特性, 使用 foreachbatch
 */
object S2SinkForeachBatch {
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
    val resultDS: Dataset[Row] = inputDF.as[String]
      .flatMap(_.split(" "))
      .groupBy("value")
      .count()
      .orderBy($"count".desc)

    //4.结果输入到控制台, 设置输出模式, 查询名称, 触发间隔, 检查点位置, memory sink
    val query: StreamingQuery = resultDS.writeStream
      .outputMode("complete")
      .foreachBatch((batchDF: DataFrame, batchId: Long) => {
        println("批次是:" + batchId)
        batchDF.show()
        //每个batchDF都保存到MySQL
        batchDF.coalesce(0).write
          .mode(SaveMode.Overwrite)
          .option("url", "jdbc:mysql://localhost:3306/bigdata?characterEncoding=UTF-8")
          .option("user", "root")
          .option("password", "123456")
          .option("dbtable", "t_struct_words")
          .save()
      }).start()

    //5.启动停留 关闭
    query.awaitTermination()
    query.stop()
  }


}
