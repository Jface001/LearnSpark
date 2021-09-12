package com.test.day07.structuredStreaming

import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * @Author: Jface
 * @Date: 2021/8/22 17:01
 * @Desc: Wordvount案例-读取 socket 源
 */
object S1StructuredStreamingSocket {
  def main(args: Array[String]): Unit = {
    //1.创建上下文对象
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", 4)
      .getOrCreate()
    //2.读取socket数据源, 得到流式DataFrame, 每行就是每批次的行数据
    val inputDF: DataFrame = spark.readStream
      .format("socket")
      .option("host", "node1")
      .option("port", 9999)
      .load()
    //3.进行wordcount, DSL风格
    inputDF.printSchema()
    //inputDF.show() //这句话会报错, 动态 DataFrame 需要通过 writeStream
    import spark.implicits._
    val DF: Dataset[Row] = inputDF.as[String]
      .flatMap(_.split(" "))
      .groupBy("value")
      .count()
      .orderBy($"count".desc)

    val query: StreamingQuery = DF.writeStream
      .outputMode("complete")
      .format("console")
      .option("rowNumber", 10)
      .option("truncate", false)
      //4.启动流式查询
      .start()
    //5.驻留监听
    query.awaitTermination()
    //6.关闭流式查询
    query.stop()
  }

}
