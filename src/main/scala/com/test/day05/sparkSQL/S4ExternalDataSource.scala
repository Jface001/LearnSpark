package com.test.day05.sparkSQL

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * @Author: Jface
 * @Desc:
 */
object S4ExternalDataSource {
  def main(args: Array[String]): Unit = {
    //1.创建上下文对象
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .config("spark.sql.shuffle.partitions", "8") //设置Shuffle的并行度为8, 否则默认为200
      .master("local[*]")
      .getOrCreate()
    //2.加载文件生产DataFrame
    val df: DataFrame = spark.read.text("src/main/data/input/resources/people.json")

    //3.将DataFrame数据保存为各种格式的数据 ，json
    //TODO:json
    df.write
      .mode(SaveMode.Overwrite)
      .format("json")
      .save("src/main/data/output/data/json")

    //简写方式
    df.write
      .mode(SaveMode.Overwrite)
      .json("src/main/data/output/data/json")
    //TODO:csv text parquet
    df.write.mode(SaveMode.Overwrite).csv("src/main/data/output/data/csv")
    df.write.mode(SaveMode.Overwrite).text("src/main/data/output/data/text")
    df.write.mode(SaveMode.Overwrite).parquet("src/main/data/output/data/parquet")
    df.write.mode(SaveMode.Overwrite).save("src/main/data/output/data/csv")

    //4.读取各种格式的文件，json csv text parquet
    val df1: DataFrame = spark.read.format("json")
      .load("src/main/data/input/resources/people.json")
    df.printSchema()
    df.show()

    val df2: DataFrame = spark.read
      .option("inferSchema", true)
      .option("header", true)
      .csv("src/main/data/input/resources/people.csv")
    println()
    df2.printSchema()
    df2.show()

    //关闭上下文对象
    spark.stop()
  }

}
