package com.test.day07.structuredStreaming

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
 * @Author: Jface
 * @Date: 2021/8/22 20:35
 * @Desc: wordcount 案例 之 读取文件
 */
object S2StructuredStreamingTextFile {
  def main(args: Array[String]): Unit = {
    //1.创建上下文对象
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", 4)
      .getOrCreate()
    //2.读取csv, 得到流式DataFrame, 每行就是每批次的行数据
    //自定义 Schema 信息
    val schema = new StructType(Array(
      StructField("name", StringType),
      StructField("age", IntegerType),
      StructField("hobby", StringType))
    )
    val inputDF: DataFrame = spark.readStream
      .format("csv")
      .option("sep", ";")
      .schema(schema)
      .load("src/main/data/input/persons")
    //3.进行wordcount, DSL风格
    inputDF.printSchema()
    //用 DSL 风格实现
    import spark.implicits._
    val DF: Dataset[Row] = inputDF.where("age<25")
      .groupBy("hobby")
      .count()
      .orderBy($"count".desc)

    // 用 SQL 风格实现
    inputDF.createOrReplaceTempView("t_spark")
    val DF2: DataFrame = spark.sql(
      """
        |select
        |hobby,
        |count(1) as cnt
        |from t_spark
        |where age<25
        |group by hobby
        |order by cnt desc
        |""".stripMargin)

    val query: StreamingQuery = DF.writeStream
      //append 默认追加 输出新的数据, 只支持简单查询, 有聚合就不能使用
      //complete:完整模式, 输出完整数据, 支持集合和排序
      //update: 更新模式, 输出有更新的数据,  支持聚合但是不支持排序
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
