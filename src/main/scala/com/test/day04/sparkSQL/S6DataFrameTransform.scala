package com.test.day04.sparkSQL

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * @Author: Jface
 * @Date: 2021/8/18 16:02
 * @Desc:
 */
object S6DataFrameTransform {
  /**
   * 定义样例类
   *
   * @param id
   * @param name
   * @param age
   */
  case class Person(id: Int, name: String, age: Int)

  def main(args: Array[String]): Unit = {
    //创建上下文对象
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[*]")
      .getOrCreate()
    //读取文件形成RDD
    val sc: SparkContext = spark.sparkContext
    val inputRDD: RDD[String] = sc.textFile("src/main/data/input/person.txt")

    // RDD--> Person对象
    val personRDD: RDD[Person] = inputRDD.map(x => {
      val arr: Array[String] = x.split("\\s+")
      Person(arr(0).toInt, arr(1), arr(2).toInt)
    })
    // RDD-->DataFrame
    println("RDD-->DataFrame")
    //TODO:需要隐式转换
    import spark.implicits._
    val df1: DataFrame = personRDD.toDF()
    df1.printSchema()
    df1.show()
    //DataFrame --> RDD
    println("DataFrame --> RDD, 会出现域丢失")
    val rdd1: RDD[Row] = df1.rdd
    rdd1.foreach(println(_))
    //RDD -->DataSet
    println("RDD -->DataSet")
    val ds1: Dataset[Person] = personRDD.toDS()
    ds1.printSchema()
    ds1.show()
    //DataSet-->RDD
    println("DataSet-->RDD, 不会发生域丢失")
    val rdd2: RDD[Person] = ds1.rdd
    rdd2.foreach(println(_))
    //DataFrame -->DataSet
    println("DataFrame -->DataSet")
    val ds2: Dataset[Person] = df1.as[Person]
    ds2.printSchema()
    ds2.show()
    //DataSet-->DataFrame
    println("DataSet-->DataFrame")
    val df2: DataFrame = ds1.toDF()
    df2.printSchema()
    df2.show()

    //关闭上下文对象
    sc.stop()
    spark.stop()
  }

}
