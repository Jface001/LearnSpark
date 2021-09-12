package com.test.day04.sparkSQL

import org.apache.spark.sql.{DataFrame, SparkSession}


/**
 * @Author: Jface
 * @Date: 2021/8/18 11:42
 * @Desc:
 */
object S1FirstSQL {
  def main(args: Array[String]): Unit = {
    //创建上下文对象,取名spark
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[*]")
      .getOrCreate()

    //读取各种数据文件
    //TODO: 纯文本
    //默认打印20行, truncate表示是否截取省略,默认截取20字符
    val df1: DataFrame = spark.read.text("src/main/data/input/resources/people.txt")
    //TODO: JSON
    val df2: DataFrame = spark.read.json("src/main/data/input/resources/people.json")
    //TODO: CSV
    val df3: DataFrame = spark.read.csv("src/main/data/input/resources/people.csv")
    //TODO: parquet
    val df4: DataFrame = spark.read.parquet("src/main/data/input/resources/users.parquet")
    //打印展示
    df1.show(20, false)
    df2.show()
    df3.show()
    df4.show()
    //写入不同格式文件中
    df1.write.text("src/main/data/output/text")
    df2.write.json("src/main/data/output/json")
    df3.write.csv("src/main/data/output/csv")
    df4.write.parquet("src/main/data/output/parquet")
    //关闭上下文对象
    spark.close()
  }

}
